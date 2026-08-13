# LB Health Transition Test Cases

Tests validating AWS Load Balancer health-check behaviour during a KAS
(Kubernetes API Server) graceful rollout. The core question: **does the NLB
route traffic to a pod before `/readyz` returns 200?**

Each scenario uses a `healthserver` binary that mimics KAS lifecycle signals
on port `19443`. An in-cluster HTTP client fires ~320 req/s through the load
balancer and an aggregator collects every request record.

---

## Shared Concepts

### KAS Graceful Shutdown Model

```
Pod lifecycle                 /readyz state    NLB target state
─────────────────────────────────────────────────────────────────
                              200 OK           HEALTHY  ← traffic flows
SIGTERM received
  └─ sets readyz → 503        503              HEALTHY  ← traffic still flows
  └─ keeps serving ~135s                        (NLB HC not propagated yet)
                              503              UNHEALTHY  ← NLB stops routing
  └─ process exits
                                               (port closed, TCP RST)
New pod starts
  └─ startup delay (boot)     —                UNHEALTHY  (port not up yet)
  └─ port bound               —                UNHEALTHY  (HC not passed yet)
  └─ /readyz → 200            200 OK           UNHEALTHY  (HC polling: ~20s)
                              200 OK           HEALTHY  ← traffic flows again
```

### Timing Milestones (all scenarios)

```
t0   Deployment/DaemonSet created
t1   All pods Running (healthserver up, startup delay pending)
t2   NLB / LB provisioned (DNS assigned)
t3   All TG targets HEALTHY (first HC cycle passed)
t4   First client request successfully routed

t5   readyz → 503  (SIGTERM sent / admin signal)
t6   TG target transitions to UNHEALTHY  (~20 s after t5)
t7   Last request routed to target after t5  (NLB drains connection)

t7.1 Pod delete sent (SIGTERM delivered by kubelet)
t7.3 New pod TCP port bound (from X-Server-Start-Time)
t7.4 First pre-readyz request  (BUG if present)

t8   readyz → 200  (new pod ready)
t9   TG target transitions to HEALTHY  (~20 s after t8)
t10  First request routed to new pod
```

---

## Scenario 5.5 — NLB Pre-Readyz Routing (OCPBUGS-86789)

**Bug being tested:** Does the AWS NLB route traffic to a restarted instance
before its `/readyz` health check passes?  If yes → OCPBUGS-86789 is
reproduced.

**Workload:** Kubernetes-managed NLB (`type: LoadBalancer` with
`aws-load-balancer-type: nlb`). Pods scheduled on control-plane nodes via
Deployment.

```
                  CLIENT (in-cluster, worker node)
                  │  ~320 req/s via NLB DNS
                  ▼
         ┌────────────────┐
         │      NLB       │  HC: HTTP /readyz, interval=10s, threshold=2
         │  (k8s-managed) │  Target type: instance
         └───┬────┬───┬───┘
             │    │   │
        ┌────┘ ┌──┘ └──┐
        ▼      ▼        ▼
   [node-A]  [node-B]  [node-C]     ← control-plane nodes (hostNetwork)
  pod-TARGET pod-2     pod-3        ← healthserver on port 19443


Phase 1 — STEADY STATE  (t3 → t5)
───────────────────────────────────
  All 3 targets HEALTHY, traffic distributed across all 3 pods.
  Expect: 0 pre-readyz requests.

Phase 2 — GRACEFUL SHUTDOWN  (t5 → t7)
────────────────────────────────────────
  t5:  SIGTERM → pod-TARGET sets /readyz → 503, keeps serving
  t6:  ~20s later, NLB HC detects UNHEALTHY
  t7:  NLB stops routing to node-A

  Timeline on node-A:
  ┌────────────────────────────────────────────────────────┐
  │ t5          t6 (~+20s)    t7 (~+31s)                  │
  │ ├───────────┤─────────────┤                            │
  │  readyz=503  HC=UNHEALTHY  last routed req             │
  │  ↑ still receives traffic ↑                            │
  └────────────────────────────────────────────────────────┘
  Expected: traffic continues for ~20-31s (HC propagation delay) — NOT a bug.

Phase 3 — RESTART  (t7 → t9)
──────────────────────────────
  t7.1: kubelet deletes pod-TARGET on node-A
  ·····  node-A: port 19443 CLOSED (after terminationGracePeriodSeconds)
  ·····  DaemonSet/Deployment creates replacement pod on node-A
  t7.3: new pod binds port 19443 (TCP up, /readyz still returning draining/503)
  t8:   new pod /readyz → 200

        ┌─────────────────────────────────────────────────────────────┐
        │ t7.1    port closed   t7.3  port up   t8  readyz=200        │
        │  ├──────────────────────┤────────────────┤                  │
        │                         ↑                ↑                  │
        │                    pre-readyz         HC polling (~20s)     │
        │                    window                                    │
        │                    (BUG ZONE: should NLB route here?)        │
        └─────────────────────────────────────────────────────────────┘

  PASS: NLB does NOT route to node-A during pre-readyz window
  BUG:  NLB DOES route to node-A before t8  → OCPBUGS-86789

Phase 4 — RECOVERY  (t9 → end)
────────────────────────────────
  t9:  NLB HC detects HEALTHY on node-A
  t10: First client request routed to new pod
  Expect: traffic resumes on all 3 nodes, 0 errors.


VERDICT logic
─────────────
  [OK]       PreReadyzReqCount == 0 AND no unhealthy reqs during Restart
  [BUG]      X-Server-State: pre-readyz received  → reproduces OCPBUGS-86789
  [SHUTDOWN] Requests after readyz→503 (expected, NLB propagation delay)
  [RESTART]  Unhealthy/pre-readyz reqs during Restart phase (NLB re-routed too early)
```

---

## Scenario 5.5-SDK — SDK-Managed NLB Baseline (OCPBUGS-86789)

**Report label:** `5.5-SDK (Pre-Readyz Routing KAS-Equivalent / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing (KAS-equivalent) (OCPBUGS-86789)`

**Why:** The KAS NLB is provisioned directly via AWS SDK (not via `type: LoadBalancer`).
This scenario creates an identical NLB manually to test the same pre-readyz routing
question on the exact same stack KAS uses.

| Parameter | Value |
|-----------|-------|
| NLB | AWS SDK, internal, `instance:port` targets |
| Healthserver | DaemonSet on control-plane, hostNetwork, port 19443 |
| Client | 1 pod on worker, **32 workers** × 50ms (~640 req/s) |
| preserve_client_ip | **true** (default, matches real KAS NLB) |

```
[client pod] (1 IP, 32 workers)
      │
      ▼
 SDK NLB (preserve_client_ip=true)  →  hash by client IP → mostly 1 target
      │
      ▼
 [node-A] [node-B] [node-C]   ← DaemonSet healthserver, 1 pod/node
```

**Known limitation:** With a single client IP, NLB stickiness sends ~96% of traffic
to one target. Post-rollout timing metrics (T_pod_restart, T_route_start) may be N/A
for the restarted pod until multi-client variants are used.

**Run:**
```bash
$BIN run-test "[cloud-provider-aws-e2e-openshift] loadbalancer health-transition SDK-managed NLB pre-readyz routing (KAS-equivalent) (OCPBUGS-86789)"
```

**Code:** `lb_health_transition.go` ~line 630

---

## Scenario 5.5-SDK-no-cip — Single Client, No Source-IP Stickiness

**Report label:** `5.5-SDK-no-cip (preserve_client_ip=false / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, preserve_client_ip=false (OCPBUGS-86789)`

Identical to **5.5-SDK** except `preserve_client_ip.enabled=false` is set on the TG
after creation via `setTGPreserveClientIP()`.

| Parameter | Value |
|-----------|-------|
| Diff vs 5.5-SDK | TG attribute `preserve_client_ip.enabled=false` only |
| Client | 1 pod, 32 workers |
| Expected distribution | ~even across 3 targets (no IP hash stickiness) |

**Purpose:** Isolate whether source-IP stickiness affects pre-readyz routing when
using a single client pod.

**Run:**
```bash
$BIN run-test "...preserve_client_ip=false..."
```

**Code:** `lb_health_transition.go` ~line 893

---

## Scenario 5.5-SDK-multi — Multi-Client, Source-IP Stickiness

**Report label:** `5.5-SDK-multi (Multi-Client DaemonSet / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client (OCPBUGS-86789)`

Identical to **5.5-SDK** except the client is a **DaemonSet** (one pod per worker node).
Each pod has a distinct source IP → NLB distributes traffic across all targets even
with `preserve_client_ip=true` (same as real KAS clients from many node IPs).

```
[client-ds on worker-1] (IP-1) ──┐
[client-ds on worker-2] (IP-2) ──┼──→ SDK NLB (preserve_client_ip=true)
[client-ds on worker-3] (IP-3) ──┘         ↓
                                    [node-A] [node-B] [node-C]
```

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, **16 workers** × 50ms per pod |
| preserve_client_ip | **true** |
| Records | `fetchMergedClientRecords()` from all client pods |
| Expected distribution | ~33% per target |

**Purpose:** Fair KAS-equivalent test — even traffic + observable post-restart metrics
on the rolled target.

**Run:**
```bash
$BIN run-test "...multi-client (OCPBUGS-86789) should not route..."
# Avoid matching the multi-no-cip It name
```

**Code:** `lb_health_transition.go` ~line 1115

---

## Scenario 5.5-SDK-multi-no-cip — Multi-Client, No Source-IP Stickiness

**Report label:** `5.5-SDK-multi-no-cip (Multi-Client + preserve_client_ip=false / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client preserve_client_ip=false (OCPBUGS-86789)`

Combines **5.5-SDK-multi** (client DaemonSet) with **5.5-SDK-no-cip**
(`preserve_client_ip=false`).

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| preserve_client_ip | **false** |
| Expected distribution | ~even (multi-client + no stickiness) |

**Purpose:** Control for both variables — tests pre-readyz behaviour with maximum
traffic spread and no source-IP affinity.

**Run:**
```bash
$BIN run-test "...multi-client preserve_client_ip=false..."
```

**Code:** `lb_health_transition.go` ~line 1335

---

## SDK Variants — Comparison Matrix

All four share: healthserver DaemonSet on masters, SDK-managed NLB, same HC/TG config
(HTTP `/readyz`, interval=10s, threshold=2), same rollout simulation (delete one pod,
wait for same-node replacement). **Only client topology and `preserve_client_ip` differ.**

| Scenario | Client | preserve_client_ip | Traffic spread | KAS-faithful |
|----------|--------|-------------------|----------------|--------------|
| 5.5-SDK | 1 pod, 32w | true | Skewed (~1 target) | NLB yes, clients no |
| 5.5-SDK-no-cip | 1 pod, 32w | false | Even | NLB no |
| 5.5-SDK-multi | DS/worker, 16w | true | Even | **Yes (recommended)** |
| 5.5-SDK-multi-no-cip | DS/worker, 16w | false | Even | Clients no |

**Shared AWS lifecycle** (all SDK variants): see v19 plan (`sdk_nlb.go`).
Cleanup includes SG retry on `DependencyViolation` and idempotent SG create on reruns.

**Plan:** `ai-plans/lb-health-transition-e2e-plan-v21-multi-client.md`

---

## Scenario 5.5-CAPA — NLB with CAPA TG Attributes (OCPBUGS-86789)

Same as 5.5 but applies the CAPA-specific Target Group attributes after TG
creation (`connection_termination.enabled=false`, `draining_interval=300s`).
Tests whether CAPA's fix attributes change the pre-readyz routing behaviour.

```
  TG attributes applied post-creation:
    connection_termination.enabled = false
    target_health_state.unhealthy.draining_interval_seconds = 300

  Verdict adds:
    [DRAINING] requests with X-Server-State: draining (300s window)
```

---

## Scenario 5.2 — NLB Shutdown Propagation (SPLAT-307)

**Question:** How long does it take the NLB to stop routing to a target after
it signals `/readyz → 503`? (No pod restart — measures propagation delay only.)

```
  t5   readyz → 503  (admin signal, no pod delete)
  t6   NLB HC detects UNHEALTHY
  t7   Last routed request to target

  ┌────────────────────────────────┐
  │ t5    t6 (~+20s)   t7         │
  │ ├─────┤────────────┤          │
  │        T_tg_unhealthy         │
  │                  T_route_stop │
  └────────────────────────────────┘

  Expected: T_route_stop ≈ T_tg_unhealthy (no extra routing after HC flips)
  Bug:      T_route_stop >> T_tg_unhealthy (extra requests after HC detects it)
```

---

## Scenario 5.5-CLB — Classic Load Balancer Baseline (OCPBUGS-86789)

Same test as 5.5 but using a Classic Load Balancer (`type: LoadBalancer` with
no NLB annotation). Provides a CLB vs NLB comparison to determine if
pre-readyz routing is NLB-specific or general to all AWS LBs.

```
  CLB differences:
    - TCP proxy (no HTTP routing)
    - Connection-level health checks (not HTTP /readyz)
    - No target group abstraction
    - Different HC propagation timing

  Expected: CLB may show different pre-readyz window than NLB
```

---

## Summary Table

| Scenario | LB Type | Managed by | Workload | Client | preserve_client_ip | Tests |
|----------|---------|------------|----------|--------|-------------------|-------|
| 5.5 | NLB | Kubernetes | Deployment | 1 pod | true (svc default) | Pre-readyz (OCPBUGS) |
| 5.5-CAPA | NLB | Kubernetes | Deployment | 1 pod | true | Pre-readyz + CAPA TG |
| 5.5-SDK | NLB | AWS SDK | DaemonSet | 1 pod, 32w | true | KAS-equivalent baseline |
| 5.5-SDK-no-cip | NLB | AWS SDK | DaemonSet | 1 pod, 32w | **false** | Stickiness isolation |
| 5.5-SDK-multi | NLB | AWS SDK | DaemonSet | DS/worker, 16w | true | **Recommended KAS-faithful** |
| 5.5-SDK-multi-no-cip | NLB | AWS SDK | DaemonSet | DS/worker, 16w | **false** | Multi + no stickiness |
| 5.2 | NLB | Kubernetes | Deployment | 1 pod | true | Shutdown propagation (SPLAT-307) |
| 5.5-CLB | CLB | Kubernetes | Deployment | 1 pod | N/A | Pre-readyz CLB baseline |

**Pass criteria (all 5.5* scenarios):**
- `PreReadyzReqCount == 0` — no requests before `/readyz → 200`
- Unhealthy requests during Restart phase == 0 (informational in verdict, not hard fail)

**Informational (always reported, not a failure):**
- Shutdown propagation delay (t5→t7, ~20–35s) — expected NLB HC lag

## Related Plans

| Plan | Topic |
|------|-------|
| `ai-plans/lb-health-transition-e2e-plan-v19-sdk-managed-nlb.html` | SDK NLB creation, infra discovery |
| `ai-plans/lb-health-transition-e2e-plan-v20-daemonset-rollout.md` | Healthserver DaemonSet, same-node rollout |
| `ai-plans/lb-health-transition-e2e-plan-v21-multi-client.md` | Four SDK variants, client scaling, preserve_client_ip matrix |
