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

t7.1 Restart trigger — **depends on restart engine** (see below)
t7.3 Target TCP port bound again (from `X-Server-Start-Time`)
t7.4 First pre-readyz request  (BUG if present)

t8   readyz → 200  (new pod ready)
t9   TG target transitions to HEALTHY  (~20 s after t8)
t10  First request routed to new pod
```

### Restart engines (SDK scenarios)

Two ways to simulate KAS restart after graceful shutdown. **Only one scenario uses ctl
today**; all other SDK tests still use pod delete.

| Engine | t5 signal | t7.1 action | t7.1 → t7.3 gap | NLB target identity | Used by |
|--------|-----------|-------------|-----------------|---------------------|---------|
| **pod delete** | admin proxy or implicit via delete | `kubectl delete pod` | **~2–3 min** (DaemonSet replacement) | New pod name | 5.5-SDK … 5.5-SDK-multi-kas-tls |
| **ctl in-place** | `kubectl exec` → `ctl readyz-false` (SIGUSR1) | `ctl restart` (SIGUSR2) | **~1–2 s** (kubelet container restart) | Same pod / same instance:port | 5.5-SDK-multi-kas-ctl, **5.5-SDK-multi-kas-tls-ctl** |

**Why ctl matters:** Pod delete closes the TCP port for minutes while the NLB target
re-registers. Real KAS restarts on the same node in seconds — the pre-readyz routing
window is visible only with ctl in-place restart. Case study: `5.5-SDK-multi-kas` (pod
delete) reported `Pre_readyz_reqs=0`; `5.5-SDK-multi-kas-ctl` reported `Pre_readyz_reqs=204`
on the same cluster (`nlb-case5.txt` vs `nlb-case9.1.txt`).

**Plan:** `ai-plans/lb-health-transition-e2e-plan-v23-ctl-inplace-restart.md`

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

## Scenario 5.5-SDK-multi-kas — Multi-Client, Real KAS TG Config

**Report label:** `5.5-SDK-multi-kas (Multi-Client + KAS TG Config / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client KAS-config (OCPBUGS-86789)`

Multi-client DaemonSet with TG attributes **exactly matching the real KAS NLB**.
This is the most faithful reproduction of real OCPBUGS-86789 conditions.

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| preserve_client_ip | **false** (matches real KAS) |
| connection_termination | **false** (matches real KAS; default is true) |
| draining_interval | **300s** (matches real KAS; default is 0) |
| deregistration_delay | 300s |
| deregistration_delay.connection_termination | false |
| stickiness | false |

**Key difference from 5.5-SDK-multi-no-cip:** The `connection_termination=false` +
`draining_interval=300s` combination means the NLB does NOT immediately terminate
connections to unhealthy targets. Instead, it drains them for up to 300s — the same
window as the real KAS NLB. This produces higher `Unhealthy_reqs` counts in the
GracefulShutdown phase, matching production behaviour.

**Run:**
```bash
$BIN run-test "...multi-client KAS-config..."
```

**Code:** `lb_health_transition.go`, `setTGKASAttributes()` in `sdk_nlb.go`

---

## Scenario 5.5-SDK-multi-kas-cip — Multi-Client, KAS TG Config + CIP

**Report label:** `5.5-SDK-multi-kas-cip (Multi-Client + KAS TG Config + CIP / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client KAS-config preserve_client_ip=true (OCPBUGS-86789)`

Same as **5.5-SDK-multi-kas** but with `preserve_client_ip=true`. Allows isolating
the source-IP stickiness effect under real KAS draining / connection-termination
settings.

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| preserve_client_ip | **true** (override from KAS default) |
| connection_termination | **false** (matches real KAS) |
| draining_interval | **300s** (matches real KAS) |
| deregistration_delay | 300s |

**Purpose:** Compare with 5.5-SDK-multi-kas to isolate `preserve_client_ip` effect
under production-identical draining settings.

**Run:**
```bash
$BIN run-test "...multi-client KAS-config preserve_client_ip=true..."
```

**Code:** `lb_health_transition.go`, `setTGKASAttributes()` in `sdk_nlb.go`

---

## Scenario 5.5-SDK-multi-kas-tls — Multi-Client, KAS TG Config + TLS

**Report label:** `5.5-SDK-multi-kas-tls (Multi-Client + KAS TG Config + TLS / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client KAS-config TLS (OCPBUGS-86789)`

Clone of **5.5-SDK-multi-kas** with TLS end-to-end on the same port (`19443`):
- healthserver serves traffic and `/readyz` via `ListenAndServeTLS`
- NLB health check uses **HTTPS** `/readyz` on port `19443`
- client DaemonSet uses `https://NLB:19443/` with `--tls-insecure`
- aggregator and client metrics remain plain HTTP (unchanged)

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| Traffic | **TLS** (self-signed cert, wildcard SAN `*`) |
| HC protocol | **HTTPS** `/readyz` on same port |
| preserve_client_ip | false |
| connection_termination | false |
| draining_interval | 300s |

**Purpose:** Determine whether OCPBUGS-86789 pre-readyz routing is specific to the
TLS handshake path (NLB may route after TCP+TLS up but before `/readyz` returns 200).

**Run:**
```bash
$BIN run-test "...multi-client KAS-config TLS..."
```

**Code:** `lb_health_transition.go`, `tls_certs.go`, `buildHealthserverDaemonSetTLS()`, `serve.go` `--tls`

---

## Scenario 5.5-SDK-multi-kas-ctl — Multi-Client, KAS TG Config + ctl In-Place Restart

**Report label:** `5.5-SDK-multi-kas-ctl (Multi-Client + KAS TG Config + ctl in-place restart / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client KAS-config ctl restart (OCPBUGS-86789)`

Same TG attributes as **5.5-SDK-multi-kas**, but simulates KAS restart more faithfully:

| Step | Action |
|------|--------|
| t5 | `kubectl exec` → `/e2e-nlb-health-test ctl readyz-false` (SIGUSR1 → `/readyz` 503, keep serving) |
| t6 | TG detects unhealthy |
| t7 | Observe shutdown drain (~90s) while NLB may still route to draining target |
| t7.1 | `kubectl exec` → `/e2e-nlb-health-test ctl restart` (SIGUSR2 → exit; kubelet restarts container in seconds) |
| t7.3 | TCP up from `X-Server-Start-Time` (same pod name / NLB target) |
| t8–t10 | `/readyz` → 200, TG healthy, client traffic |

**Why not pod delete:** Deleting the DaemonSet pod creates a ~3 minute TCP gap (new pod on same node still needs NLB target propagation). Real KAS restarts on the same node in seconds — this variant closes that gap.

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| Restart mode | **ctl in-place** (no pod delete) |
| preserve_client_ip | false |
| connection_termination | false |
| draining_interval | 300s |
| shutdown drain observe | **90s** before ctl restart (Hyperplane-boundary timing; see case 9.x) |

**Run:**
```bash
$BIN run-test "...multi-client KAS-config ctl restart..."
```

**Code:** `lb_health_transition.go`, `ctl_exec.go`, `cmd/e2e-nlb-health-test/ctl.go`

---

## Scenario 5.5-SDK-multi-kas-tls-ctl — Multi-Client, KAS TG + TLS + ctl In-Place Restart

**Report label:** `5.5-SDK-multi-kas-tls-ctl (Multi-Client + KAS TG + TLS + ctl in-place restart / OCPBUGS-86789)`

**Ginkgo:** `SDK-managed NLB pre-readyz routing, multi-client KAS-config TLS ctl restart (OCPBUGS-86789)`

**Most faithful KAS reproduction** — combines everything from the production path:

| Layer | Config |
|-------|--------|
| NLB stack | AWS SDK, `instance:19443` (same as KAS) |
| TG attributes | KAS-equivalent (`conn_term=false`, `draining=300s`, `preserve_client_ip=false`) |
| Traffic | **TLS** on port 19443 (self-signed cert, `--tls-insecure` client) |
| Health check | **HTTPS** `/readyz` on same port |
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| Restart | **ctl in-place** (SIGUSR1 shutdown → **30s** drain → SIGUSR2 restart) |

Same ctl flow as **5.5-SDK-multi-kas-ctl**, with TLS/HTTPS HC from **5.5-SDK-multi-kas-tls**.

| Parameter | Value |
|-----------|-------|
| Client | DaemonSet on workers, 16 workers × 50ms per pod |
| Restart mode | **ctl in-place** |
| Traffic / HC | **TLS / HTTPS** |
| preserve_client_ip | false |
| connection_termination | false |
| draining_interval | 300s |
| shutdown drain observe | **30s** before ctl restart (v25) |

**Timing (v25):** After TG detects unhealthy (~25s post t5), wait **30s** drain, then
ctl restart → TCP up ~**55s** after readyz→503. Places recovery inside the NLB
Hyperplane propagation window (50–120s), improving repro consistency vs 90s drain
(~117s total) which sat at the propagation boundary (cases 9.1/9.2 reproduced;
9.3/9.4 did not — cloud variability, not code).

**Also confirms:** `unhealthy.draining` (conn_term=false, draining=300s) does **not**
prevent pre-readyz routing — case 9.1/9.2 routed 204/37 pre-readyz reqs while target
was in draining state.

**Observed results (v25, 30s drain):**

| Case | Pre_readyz | T_route_stop | Result |
|------|------------|--------------|--------|
| 10.2 | **19,676** | **54.78s** | ✅ REPRODUCED (consistent) |
| 10.3 | **10,784** | **52.67s** | ✅ REPRODUCED (consistent) |

TCP reopens ~55s after readyz→503 while `T_route_stop` is still ~53s — NLB is actively
routing during container restart. Contrast case 9.3/9.4 (90s drain, `T_route_stop` ~1m22s,
TCP ~1m55s, 0 pre-readyz).

**Run:**
```bash
$BIN run-test "...multi-client KAS-config TLS ctl restart..." \
  | tee ai-plans/nlb-test-cases/nlb-case10.2.txt
```

**Compare with:**
```bash
# HTTP ctl (v23)
$BIN run-test "...KAS-config ctl restart..." | tee ai-plans/nlb-test-cases/nlb-case9.1.txt
# TLS pod-delete (v22, conservative)
$BIN run-test "...KAS-config TLS..." | tee ai-plans/nlb-test-cases/nlb-case7.1.txt
```

**Code:** `lb_health_transition.go`, `tls_certs.go`, `ctl_exec.go`, `cmd/e2e-nlb-health-test/ctl.go`

**Plan:** `ai-plans/lb-health-transition-e2e-plan-v24-kas-tls-ctl.md`, `ai-plans/lb-health-transition-e2e-plan-v25-kas-tls-ctl-short-drain.md`

---

## SDK Variants — Comparison Matrix

All SDK variants share: healthserver DaemonSet on masters, SDK-managed NLB, same
HC config (HTTP `/readyz`, interval=10s, threshold=2). They differ in **client
topology**, **TG attributes**, **TLS**, and **restart engine** (see Restart engines
above).

### Default TG attributes (5.5-SDK through 5.5-SDK-multi-no-cip)

| Scenario | Client | preserve_client_ip | conn_term | draining | Restart |
|----------|--------|-------------------|-----------|----------|---------|
| 5.5-SDK | 1 pod, 32w | true | true (default) | 0 (default) | pod delete |
| 5.5-SDK-no-cip | 1 pod, 32w | false | true (default) | 0 (default) | pod delete |
| 5.5-SDK-multi | DS/worker, 16w | true | true (default) | 0 (default) | pod delete |
| 5.5-SDK-multi-no-cip | DS/worker, 16w | false | true (default) | 0 (default) | pod delete |

### Real KAS TG attributes (5.5-SDK-multi-kas family)

| Scenario | Client | preserve_client_ip | conn_term | draining | TLS/HC | Restart |
|----------|--------|-------------------|-----------|----------|--------|---------|
| 5.5-SDK-multi-kas | DS/worker, 16w | false | **false** | **300s** | HTTP/HTTP | pod delete |
| 5.5-SDK-multi-kas-cip | DS/worker, 16w | true | **false** | **300s** | HTTP/HTTP | pod delete |
| 5.5-SDK-multi-kas-tls | DS/worker, 16w | false | **false** | **300s** | **TLS/HTTPS** | pod delete |
| 5.5-SDK-multi-kas-ctl | DS/worker, 16w | false | **false** | **300s** | HTTP/HTTP | **ctl in-place** |
| **5.5-SDK-multi-kas-tls-ctl** | DS/worker, 16w | false | **false** | **300s** | **TLS/HTTPS** | **ctl in-place** |

**Real KAS TG attributes** (from `aws elbv2 describe-target-group-attributes`):
```
preserve_client_ip.enabled                                = false
target_health_state.unhealthy.connection_termination.enabled = false
target_health_state.unhealthy.draining_interval_seconds     = 300
deregistration_delay.timeout_seconds                        = 300
deregistration_delay.connection_termination.enabled          = false
stickiness.enabled                                          = false
```

**Shared AWS lifecycle** (all SDK variants): see v19 plan (`sdk_nlb.go`).
Cleanup includes SG retry on `DependencyViolation` and idempotent SG create on reruns.

**Plans:** v21 (SDK matrix), v22 (TLS), **v23 (ctl in-place restart)**

---

## SDK Data Collection — Final Report

Before filing or updating OCPBUGS-86789 evidence, collect all SDK variant runs on the
**same cluster** and **same `HEALTHSERVER_IMAGE` build**. Save output under
`ai-plans/nlb-test-cases/`.

| Output file | Scenario | Restart engine | Run filter (substring) |
|-------------|----------|----------------|------------------------|
| `nlb-case4.txt` | 5.5-SDK | pod delete | `KAS-equivalent` |
| `nlb-case1.txt` | 5.5-SDK-no-cip | pod delete | `preserve_client_ip=false` (not multi-client) |
| `nlb-case3.txt` | 5.5-SDK-multi | pod delete | `multi-client (OCPBUGS` |
| `nlb-case2.txt` | 5.5-SDK-multi-no-cip | pod delete | `multi-client preserve_client_ip=false` |
| `nlb-case5.txt` | 5.5-SDK-multi-kas | pod delete | `multi-client KAS-config (OCPBUGS` |
| `nlb-case6.txt` | 5.5-SDK-multi-kas-cip | pod delete | `KAS-config preserve_client_ip=true` |
| `nlb-case7.1.txt` | 5.5-SDK-multi-kas-tls | pod delete | `KAS-config TLS` |
| `nlb-case9.1.txt` | 5.5-SDK-multi-kas-ctl | **ctl in-place** | `KAS-config ctl restart` |
| `nlb-case10.2.txt` | **5.5-SDK-multi-kas-tls-ctl** | **ctl + TLS (30s drain)** | **`KAS-config TLS ctl restart`** | **19,676 pre-readyz** |
| `nlb-case10.3.txt` | **5.5-SDK-multi-kas-tls-ctl** | **ctl + TLS (30s drain)** | re-run | **10,784 pre-readyz** |

**Primary metric:** `Pre_readyz_reqs` in TIMING TABLE (0 = pass, >0 = OCPBUGS-86789 repro).

**Compare across runs:** `T_pod_restart`, `T_route_start`, `Unhealthy_reqs`, `Late_conn_reqs`,
VERDICT lines. Use the final report template in
`ai-plans/lb-health-transition-e2e-plan-v23-ctl-inplace-restart.md`.

**Key insight:** Pod-delete runs may show `Pre_readyz_reqs=0` while ctl runs on the same
KAS TG config reproduce the bug — label pod-delete results as *conservative* (slow restart
masks the pre-readyz window).

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

| Scenario | LB Type | Managed by | Workload | Client | preserve_client_ip | conn_term / draining | TLS/HC | Tests |
|----------|---------|------------|----------|--------|-------------------|---------------------|--------|-------|
| 5.5 | NLB | Kubernetes | Deployment | 1 pod | true (svc default) | default | HTTP | Pre-readyz (OCPBUGS) |
| 5.5-CAPA | NLB | Kubernetes | Deployment | 1 pod | true | default | HTTP | Pre-readyz + CAPA TG |
| 5.5-SDK | NLB | AWS SDK | DaemonSet | 1 pod, 32w | true | default | HTTP | KAS-equivalent baseline |
| 5.5-SDK-no-cip | NLB | AWS SDK | DaemonSet | 1 pod, 32w | **false** | default | HTTP | Stickiness isolation |
| 5.5-SDK-multi | NLB | AWS SDK | DaemonSet | DS/worker, 16w | true | default | HTTP | Multi-client baseline |
| 5.5-SDK-multi-no-cip | NLB | AWS SDK | DaemonSet | DS/worker, 16w | **false** | default | HTTP | Multi + no stickiness |
| 5.5-SDK-multi-kas | NLB | AWS SDK | DaemonSet | DS/worker, 16w | **false** | **false / 300s** | HTTP | **Most faithful KAS repro (HTTP)** |
| 5.5-SDK-multi-kas-cip | NLB | AWS SDK | DaemonSet | DS/worker, 16w | true | **false / 300s** | HTTP | KAS TG + CIP comparison |
| 5.5-SDK-multi-kas-tls | NLB | AWS SDK | DaemonSet | DS/worker, 16w | **false** | **false / 300s** | **TLS/HTTPS** | **KAS TLS + HC repro** |
| 5.5-SDK-multi-kas-ctl | NLB | AWS SDK | DaemonSet | DS/worker, 16w | **false** | **false / 300s** | HTTP | **KAS + in-place restart** |
| **5.5-SDK-multi-kas-tls-ctl** | NLB | AWS SDK | DaemonSet | DS/worker, 16w | **false** | **false / 300s** | **TLS/HTTPS** | **Most faithful KAS repro** |
| 5.2 | NLB | Kubernetes | Deployment | 1 pod | true | default | HTTP | Shutdown propagation (SPLAT-307) |
| 5.5-CLB | CLB | Kubernetes | Deployment | 1 pod | N/A | N/A | HTTP | Pre-readyz CLB baseline |

**Pass criteria (all 5.5* scenarios):**
- `PreReadyzReqCount == 0` — no requests before `/readyz → 200`
- Unhealthy requests during Restart phase == 0 (informational in verdict, not hard fail)

**Informational (always reported, not a failure):**
- Shutdown propagation delay (t5→t7, ~20–35s) — expected NLB HC lag
- `Late_conn_reqs` — requests to target after 80% of `kasShutdownDelay` (153.6s); high RST risk window

## Related Plans

| Plan | Topic |
|------|-------|
| `ai-plans/lb-health-transition-e2e-plan-v19-sdk-managed-nlb.html` | SDK NLB creation, infra discovery |
| `ai-plans/lb-health-transition-e2e-plan-v20-daemonset-rollout.md` | Healthserver DaemonSet, same-node rollout |
| `ai-plans/lb-health-transition-e2e-plan-v21-multi-client.md` | SDK variant matrix (client × CIP × TG) |
| `ai-plans/lb-health-transition-e2e-plan-v22-kas-tls-lateconn.md` | KAS TLS variant + LateConnections metric |
| `ai-plans/lb-health-transition-e2e-plan-v23-ctl-inplace-restart.md` | ctl in-place restart engine, data collection |
| `ai-plans/lb-health-transition-e2e-plan-v24-kas-tls-ctl.md` | KAS TLS + ctl — most faithful repro |
