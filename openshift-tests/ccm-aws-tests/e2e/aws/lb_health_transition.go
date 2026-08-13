package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/openshift/cluster-cloud-controller-manager-operator/openshift-tests/ccm-aws-tests/e2e/aws/health"
	"github.com/openshift/cluster-cloud-controller-manager-operator/openshift-tests/ccm-aws-tests/e2e/common"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	// envHealthserverImage is the container image for the unified binary
	// e2e-nlb-health-test. Used for all three roles (serve, client, aggregator).
	envHealthserverImage = "HEALTHSERVER_IMAGE"

	healthTransitionTestPrefix = e2eTestPrefixLoadBalancer + " health-transition"

	// healthserverPort is the port the healthserver binds on the node IP
	// via hostNetwork. Chosen to avoid conflicts with existing services on
	// control-plane nodes (verified via netstat). Echoes 6443 (KAS port).
	healthserverPort = 19443

	// aggregatorPort is the port the aggregator listens on (worker node).
	aggregatorPort = 8090

	// clientPort is the port the in-cluster client serves metrics/records on.
	clientPort = 8080

	// kasShutdownDelay matches the KAS shutdown-delay-duration (135s graceful +
	// margin), simulating how long KAS keeps serving after /readyz→503 before
	// the process exits.  CKAO sets 135s; we add buffer for HC propagation.
	kasShutdownDelay = 192 * time.Second

	// defaultClientInterval controls how often each worker sends requests
	// through the NLB. Each worker fires independently on its own ticker.
	// With DisableKeepAlives (new TCP per request), each worker creates
	// one outbound connection at a time. Too many workers with short
	// intervals can exhaust ephemeral ports and starve K8s API calls.
	// With the in-cluster client (~1-5ms RTT to NLB), higher concurrency
	// is safe. 16 workers at 50ms = ~320 req/s at 1ms RTT, ~160 req/s
	// at 5ms RTT. Port exhaustion is not a concern because the client
	// runs inside the cluster on a worker node, not from an external
	// machine competing with K8s API calls.
	defaultClientInterval = 50 * time.Millisecond
	defaultClientWorkers  = 16

	// postHealthyObserve is how long we continue observing after all targets
	// become healthy (both initial setup and post-restart). 90s gives enough
	// time to confirm stable routing while keeping test duration reasonable.
	postHealthyObserve = 90 * time.Second
)

// transitionTimeline captures all timing milestones from the SPLAT-307 state
// machine extended with restart-phase timers (t7.1–t7.4) for OCPBUGS-86789.
//
// A zero time.Time means the milestone was not observed.
type transitionTimeline struct {
	// Initial registration phase (t0-t4, captured during setup)
	T0 time.Time // deployment created (pods scheduling)
	T1 time.Time // deployment ready (all pods Running)
	T2 time.Time // NLB provisioned (LB DNS assigned)
	T3 time.Time // all TG targets healthy (HC passed + propagated)
	T4 time.Time // first client request received (NLB routing established)

	// Shutdown phase (SPLAT-307 path)
	T5  time.Time // readyz→503 signal sent
	T6  time.Time // first observer event: target unhealthy
	T7  time.Time // last client request routed to target after t5

	// Restart phase (Scenario 5.5 only; zero for 5.2)
	T71 time.Time // pod delete sent
	T73 time.Time // new pod TCP up (from X-Server-Start-Time header)
	T74 time.Time // first pre-readyz request from new pod (BUG if present)

	// Startup phase
	T8  time.Time // readyz→200 (from X-First-Readyz-Time header or admin signal)
	T9  time.Time // first observer event: target healthy after t8
	T10 time.Time // first client request to target after t9

	// Counters
	UnhealthyReqCount int // requests served by target between t5 and t7
	PreReadyzReqCount int // requests with X-Server-State: pre-readyz

	// Identity
	TargetPod  string
	TargetNode string
	NewPod     string

	// PodNodeMap maps pod names to the node they run on, used for
	// displaying node identity alongside pod names in the report.
	PodNodeMap map[string]string
}

// serviceConfig records Service, TG, and environment configuration for the report.
type serviceConfig struct {
	ServiceAnnotations map[string]string
	TGAttributes       []health.TGAttribute
	TGARN              string
	TGTargetType       string
	LBARN              string
	LBDNS              string

	// Environment summary
	Region   string
	Platform string // e.g., "AWS"
	Topology string // e.g., "HighlyAvailable"
}

var _ = Describe(healthTransitionTestPrefix, func() {
	f := framework.NewDefaultFramework("cloud-provider-aws")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var cs clientset.Interface
	var ns *v1.Namespace

	BeforeEach(func() {
		cs = f.ClientSet
		ns = f.Namespace
	})

	// ── Scenario 5.5 ───────────────────────────────────────────────────
	Context("NLB pre-readyz routing detection (OCPBUGS-86789)", func() {
		It("should not route to pre-readyz targets "+
			"when healthy targets are available", func(ctx context.Context) {

			image := os.Getenv(envHealthserverImage)
			if image == "" {
				Skip(fmt.Sprintf("%s not set", envHealthserverImage))
			}

			replicas := int32(3)
			startupDelay := 60 * time.Second
			shutdownDelay := kasShutdownDelay

			deployName := "healthserver"
			svcName := "healthserver-lb"

			// Setup creates NLB targeting master nodes, waits for ALL targets healthy
			_, observer, svcCfg, setupTimes, clientPodName := setupHealthTransition(
				ctx, cs, ns, deployName, svcName, image,
				replicas, startupDelay,
			)

			// The in-cluster client is already running (deployed in setup).
			// Start the TG observer for health state tracking, and push
			// TG snapshots to the aggregator every 2s so all state changes
			// from the AWS perspective appear in the aggregator timeline.
			observer.Start(ctx)
			stopTGPush := startTGSnapshotPusher(ctx, cs, ns.Name, observer)
			framework.Logf("[observer] started TG health polling (1s) + aggregator push (2s)")
			framework.Logf("[client-pod] in-cluster client %s already sending requests", clientPodName)
			defer func() { stopTGPush(); observer.Stop() }()

			// Steady state: 90s for the in-cluster client to establish
			// traffic to all replicas before triggering the test scenario.
			By(fmt.Sprintf("verifying steady state for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			// Fetch steady-state records from the in-cluster client
			steadyRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			steadyNonReady := 0
			for _, r := range steadyRecords {
				if r.IsNonReadyReq {
					steadyNonReady++
				}
			}
			framework.Logf("[steady] %d requests from in-cluster client, %d non-ready", len(steadyRecords), steadyNonReady)
			Expect(steadyNonReady).To(Equal(0), "pre-readyz responses during steady state")

			By("listing pods to identify target for rollout simulation")
			pods, err := cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", deployName),
			})
			framework.ExpectNoError(err, "list healthserver pods")
			Expect(len(pods.Items)).To(BeNumerically(">=", int(replicas)))

			// Build knownServers and podNodeMap from ALL existing pods.
			knownServers := make(map[string]bool)
			podNodeMap := make(map[string]string)
			for _, p := range pods.Items {
				knownServers[p.Name] = true
				podNodeMap[p.Name] = p.Spec.NodeName
			}

			targetPod := pods.Items[0].Name
			targetNode := pods.Items[0].Spec.NodeName

			// t5 = t7.1: Delete pod — kubelet sends SIGTERM, healthserver sets
			// readyz→503 and keeps serving for terminationGracePeriodSeconds (192s).
			// This exactly matches KAS rollout behavior: SIGTERM → readyz→503 →
			// keep serving for shutdown-delay-duration → process killed.
			// After terminationGracePeriodSeconds, kubelet kills the pod and
			// the Deployment creates a replacement.
			By("deleting target pod (t5/t7.1 — SIGTERM triggers readyz→503)")
			t5 := time.Now()
			t71 := t5
			err = cs.CoreV1().Pods(ns.Name).Delete(ctx, targetPod, metav1.DeleteOptions{})
			framework.ExpectNoError(err)

			By("waiting for replacement pod")
			newPod := waitForNewPod(ctx, cs, ns.Name, deployName, targetPod)

			// Capture the new pod's node for the report
			newPodObj, npErr := cs.CoreV1().Pods(ns.Name).Get(ctx, newPod, metav1.GetOptions{})
			if npErr == nil {
				podNodeMap[newPod] = newPodObj.Spec.NodeName
			}

			// First wait for the TG to detect the unhealthy target (HC
			// needs threshold×interval to detect). Without this, the next
			// waitForAllTGTargetsHealthy returns immediately because the TG
			// hasn't processed the failure yet.
			By("waiting for TG to detect unhealthy target")
			waitForTGUnhealthy(ctx, observer, 3*time.Minute)

			// Now wait for the restarted target to recover and become healthy.
			By("waiting for TG to detect unhealthy target")
			waitForTGUnhealthy(ctx, observer, 3*time.Minute)

			By("waiting for restarted target to become healthy")
			err = waitForAllTGTargetsHealthy(ctx, observer, 10*time.Minute)
			framework.ExpectNoError(err, "restarted target healthy")

			By(fmt.Sprintf("observing post-recovery traffic for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			// Fetch all request records from the in-cluster client pod
			allRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			allEvents := observer.Events()

			tl := computeTimeline(targetPod, knownServers, t5, t71, allRecords, allEvents)
			tl.T0 = setupTimes.T0
			tl.T1 = setupTimes.T1
			tl.T2 = setupTimes.T2
			tl.T3 = setupTimes.T3
			for _, r := range steadyRecords {
				if r.Error == "" && r.HTTPStatus > 0 {
					tl.T4 = r.Timestamp
					break
				}
			}
			tl.TargetPod = targetPod
			tl.TargetNode = targetNode
			tl.NewPod = newPod
			tl.PodNodeMap = podNodeMap

			report := buildReport("5.5 (Pre-Readyz Routing / OCPBUGS-86789)",
				tl, svcCfg, replicas, startupDelay, shutdownDelay,
				allRecords, allEvents, observer.Snapshots())

			report += buildVerdict55(tl, allRecords)

			framework.Logf("\n%s", report)
		})
	})

	// ── Scenario 5.5 variant with CAPA TG attributes ────────────────────
	// Same as 5.5 but applies the CAPA fix TG attributes after TG creation:
	//   target_health_state.unhealthy.connection_termination.enabled = false
	//   target_health_state.unhealthy.draining_interval_seconds = 300
	// This simulates the NLB configuration applied by CAPA (OCPBUGS-55626).
	Context("NLB pre-readyz routing with CAPA TG attributes (OCPBUGS-86789)", func() {
		It("should not route to pre-readyz targets "+
			"with connection-termination disabled and draining=300s", func(ctx context.Context) {

			image := os.Getenv(envHealthserverImage)
			if image == "" {
				Skip(fmt.Sprintf("%s not set", envHealthserverImage))
			}

			replicas := int32(3)
			startupDelay := 60 * time.Second
			shutdownDelay := kasShutdownDelay

			deployName := "healthserver"
			svcName := "healthserver-lb"

			_, observer, svcCfg, setupTimes, clientPodName := setupHealthTransition(
				ctx, cs, ns, deployName, svcName, image,
				replicas, startupDelay,
			)

			// Apply CAPA fix TG attributes BEFORE starting the observer.
			capaAttrs := map[string]string{
				"target_health_state.unhealthy.connection_termination.enabled": "false",
				"target_health_state.unhealthy.draining_interval_seconds":      "300",
			}
			By("applying CAPA TG attributes (conn_term=false, draining=300s)")
			err := observer.ModifyTGAttributes(ctx, capaAttrs)
			framework.ExpectNoError(err, "modify TG attributes for CAPA variant")

			// Re-fetch TG attributes so the report reflects the modified config
			tgAttrs, err := observer.DescribeTGAttributes(ctx)
			if err == nil {
				svcCfg.TGAttributes = tgAttrs
			}
			fetchTGHealthCheckConfig(ctx, &svcCfg)

			observer.Start(ctx)
			stopTGPush := startTGSnapshotPusher(ctx, cs, ns.Name, observer)
			framework.Logf("[observer] started TG health polling (1s) + aggregator push (2s)")
			framework.Logf("[client-pod] in-cluster client %s already sending requests", clientPodName)
			defer func() { stopTGPush(); observer.Stop() }()

			By(fmt.Sprintf("verifying steady state for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			steadyRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			steadyNonReady := 0
			for _, r := range steadyRecords {
				if r.IsNonReadyReq {
					steadyNonReady++
				}
			}
			Expect(steadyNonReady).To(Equal(0), "pre-readyz responses during steady state")

			By("listing pods to identify target for rollout simulation")
			pods, err := cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", deployName),
			})
			framework.ExpectNoError(err, "list healthserver pods (K8s API may be overloaded by client workers)")
			Expect(len(pods.Items)).To(BeNumerically(">=", int(replicas)))

			// Build knownServers and podNodeMap from ALL existing pods.
			knownServers := make(map[string]bool)
			podNodeMap := make(map[string]string)
			for _, p := range pods.Items {
				knownServers[p.Name] = true
				podNodeMap[p.Name] = p.Spec.NodeName
			}

			targetPod := pods.Items[0].Name
			targetNode := pods.Items[0].Spec.NodeName

			By("deleting target pod (t5/t7.1 — SIGTERM triggers readyz→503)")
			t5 := time.Now()
			t71 := t5
			err = cs.CoreV1().Pods(ns.Name).Delete(ctx, targetPod, metav1.DeleteOptions{})
			framework.ExpectNoError(err)

			By("waiting for replacement pod")
			newPod := waitForNewPod(ctx, cs, ns.Name, deployName, targetPod)

			// Capture the new pod's node for the report
			newPodObj, npErr := cs.CoreV1().Pods(ns.Name).Get(ctx, newPod, metav1.GetOptions{})
			if npErr == nil {
				podNodeMap[newPod] = newPodObj.Spec.NodeName
			}

			By("waiting for TG to detect unhealthy target")
			waitForTGUnhealthy(ctx, observer, 3*time.Minute)

			By("waiting for restarted target to become healthy")
			err = waitForAllTGTargetsHealthy(ctx, observer, 10*time.Minute)
			framework.ExpectNoError(err, "restarted target healthy")

			By(fmt.Sprintf("observing post-recovery traffic for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			// Fetch all request records from the in-cluster client pod
			allRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			allEvents := observer.Events()

			tl := computeTimeline(targetPod, knownServers, t5, t71, allRecords, allEvents)
			tl.T0 = setupTimes.T0
			tl.T1 = setupTimes.T1
			tl.T2 = setupTimes.T2
			tl.T3 = setupTimes.T3
			for _, r := range steadyRecords {
				if r.Error == "" && r.HTTPStatus > 0 {
					tl.T4 = r.Timestamp
					break
				}
			}
			tl.TargetPod = targetPod
			tl.TargetNode = targetNode
			tl.NewPod = newPod
			tl.PodNodeMap = podNodeMap

			report := buildReport("5.5-CAPA (Pre-Readyz + conn_term=false draining=300s)",
				tl, svcCfg, replicas, startupDelay, shutdownDelay,
				allRecords, allEvents, observer.Snapshots())

			report += buildVerdict55(tl, allRecords)

			framework.Logf("\n%s", report)
		})
	})

	// ── Scenario 5.2 ───────────────────────────────────────────────────
	Context("NLB shutdown propagation measurement (SPLAT-307)", func() {
		It("should stop routing within shutdown-delay after "+
			"readyz starts failing", func(ctx context.Context) {

			// Scenario 5.2 requires the admin signal (readyz→503 without pod
			// deletion) which doesn't work with hostNetwork: the K8s API server
			// pod proxy can't reach nodeIP:19443 due to security group rules.
			// TODO: implement alternative signaling (e.g., ConfigMap watch, or
			// a non-hostNetwork admin sidecar).
			Skip("Scenario 5.2 not yet supported with hostNetwork (admin signal unreachable)")

			image := os.Getenv(envHealthserverImage)
			if image == "" {
				Skip(fmt.Sprintf("%s not set", envHealthserverImage))
			}

			replicas := int32(3)
			startupDelay := 60 * time.Second
			shutdownObserveDuration := 3 * time.Minute
			recoveryObserveDuration := 3 * time.Minute

			deployName := "healthserver"
			svcName := "healthserver-lb"

			_, observer, svcCfg, setupTimes, clientPodName := setupHealthTransition(
				ctx, cs, ns, deployName, svcName, image,
				replicas, startupDelay,
			)

			observer.Start(ctx)
			stopTGPush := startTGSnapshotPusher(ctx, cs, ns.Name, observer)
			framework.Logf("[observer] started TG health polling (1s) + aggregator push (2s)")
			framework.Logf("[client-pod] in-cluster client %s already sending requests", clientPodName)
			defer func() { stopTGPush(); observer.Stop() }()

			By(fmt.Sprintf("verifying steady state for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			By("listing pods to identify target for shutdown simulation")
			pods, err := cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", deployName),
			})
			framework.ExpectNoError(err, "list healthserver pods")
			podNodeMap := make(map[string]string)
			for _, p := range pods.Items {
				podNodeMap[p.Name] = p.Spec.NodeName
			}
			targetPod := pods.Items[0].Name
			targetNode := pods.Items[0].Spec.NodeName

			By("signaling target pod readyz→503 (t5)")
			t5 := time.Now()
			err = sendAdminSignal(ctx, cs, ns.Name, targetPod, false)
			framework.ExpectNoError(err)

			By(fmt.Sprintf("observing shutdown propagation for %s", shutdownObserveDuration))
			time.Sleep(shutdownObserveDuration)

			By("signaling target pod readyz→200 (t8)")
			t8 := time.Now()
			err = sendAdminSignal(ctx, cs, ns.Name, targetPod, true)
			framework.ExpectNoError(err)

			By(fmt.Sprintf("observing recovery for %s", recoveryObserveDuration))
			time.Sleep(recoveryObserveDuration)

			// Fetch all records from the in-cluster client
			allRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			allEvents := observer.Events()

			tl := computeTimeline52(targetPod, t5, t8, allRecords, allEvents)
			tl.T0 = setupTimes.T0
			tl.T1 = setupTimes.T1
			tl.T2 = setupTimes.T2
			tl.T3 = setupTimes.T3
			// t4: first successful client request
			for _, r := range allRecords {
				if r.Error == "" && r.HTTPStatus > 0 {
					tl.T4 = r.Timestamp
					break
				}
			}
			tl.TargetPod = targetPod
			tl.TargetNode = targetNode
			tl.PodNodeMap = podNodeMap

			report := buildReport("5.2 (Shutdown Propagation / SPLAT-307)",
				tl, svcCfg, replicas, startupDelay, 0,
				allRecords, allEvents, observer.Snapshots())

			report += buildVerdict52(tl)

			framework.Logf("\n%s", report)
		})
	})

	// ── Scenario 5.5 CLB baseline ───────────────────────────────────────
	// Same as Scenario 5.5 but using Classic Load Balancer instead of NLB.
	// Compares CLB and NLB health transition behavior to determine if the
	// pre-readyz routing issue is NLB-specific (Hyperplane) or broader.
	Context("CLB pre-readyz routing detection baseline (OCPBUGS-86789)", func() {
		It("should not route to pre-readyz targets "+
			"when healthy targets are available", func(ctx context.Context) {

			image := os.Getenv(envHealthserverImage)
			if image == "" {
				Skip(fmt.Sprintf("%s not set", envHealthserverImage))
			}

			replicas := int32(3)
			startupDelay := 60 * time.Second
			shutdownDelay := kasShutdownDelay

			deployName := "healthserver"
			svcName := "healthserver-lb"

			// Setup uses CLB (no nlb annotation) with same HC config
			lbDNS, clbObserver, svcCfg, setupTimes, clientPodName := setupHealthTransitionCLB(
				ctx, cs, ns, deployName, svcName, image,
				replicas, startupDelay,
			)
			_ = lbDNS

			clbObserver.Start(ctx)
			// Push CLB health snapshots to aggregator every 2s
			stopCLBPush := startCLBSnapshotPusher(ctx, cs, ns.Name, clbObserver)
			framework.Logf("[observer] started CLB health polling (1s) + aggregator push (2s)")
			framework.Logf("[client-pod] in-cluster client %s already sending requests", clientPodName)
			defer func() { stopCLBPush(); clbObserver.Stop() }()

			By(fmt.Sprintf("verifying steady state for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			steadyRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			steadyNonReady := 0
			for _, r := range steadyRecords {
				if r.IsNonReadyReq {
					steadyNonReady++
				}
			}
			framework.Logf("[steady] %d requests from in-cluster client, %d non-ready", len(steadyRecords), steadyNonReady)
			Expect(steadyNonReady).To(Equal(0), "pre-readyz responses during steady state")

			By("listing pods to identify target for rollout simulation")
			pods, err := cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", deployName),
			})
			framework.ExpectNoError(err, "list healthserver pods")
			Expect(len(pods.Items)).To(BeNumerically(">=", int(replicas)))

			knownServers := make(map[string]bool)
			podNodeMap := make(map[string]string)
			for _, p := range pods.Items {
				knownServers[p.Name] = true
				podNodeMap[p.Name] = p.Spec.NodeName
			}

			targetPod := pods.Items[0].Name
			targetNode := pods.Items[0].Spec.NodeName

			By("deleting target pod (t5/t7.1 — SIGTERM triggers readyz→503)")
			t5 := time.Now()
			t71 := t5
			err = cs.CoreV1().Pods(ns.Name).Delete(ctx, targetPod, metav1.DeleteOptions{})
			framework.ExpectNoError(err)

			By("waiting for replacement pod")
			newPod := waitForNewPod(ctx, cs, ns.Name, deployName, targetPod)

			newPodObj, npErr := cs.CoreV1().Pods(ns.Name).Get(ctx, newPod, metav1.GetOptions{})
			if npErr == nil {
				podNodeMap[newPod] = newPodObj.Spec.NodeName
			}

			// Wait for CLB to detect unhealthy, then recover
			By("waiting for CLB to detect unhealthy instance")
			waitForCLBUnhealthy(ctx, clbObserver, 3*time.Minute)

			By("waiting for all CLB instances to become healthy")
			err = clbObserver.WaitForAllHealthy(ctx, int(replicas), 10*time.Minute)
			framework.ExpectNoError(err, "CLB instances healthy")

			By(fmt.Sprintf("observing post-recovery traffic for %s", postHealthyObserve))
			time.Sleep(postHealthyObserve)

			allRecords := fetchClientRecords(ctx, cs, ns.Name, clientPodName)
			allEvents := clbObserver.Events()

			tl := computeTimeline(targetPod, knownServers, t5, t71, allRecords, allEvents)
			tl.T0 = setupTimes.T0
			tl.T1 = setupTimes.T1
			tl.T2 = setupTimes.T2
			tl.T3 = setupTimes.T3
			for _, r := range steadyRecords {
				if r.Error == "" && r.HTTPStatus > 0 {
					tl.T4 = r.Timestamp
					break
				}
			}
			tl.TargetPod = targetPod
			tl.TargetNode = targetNode
			tl.NewPod = newPod
			tl.PodNodeMap = podNodeMap

			report := buildReport("5.5-CLB (Pre-Readyz Routing CLB Baseline / OCPBUGS-86789)",
				tl, svcCfg, replicas, startupDelay, shutdownDelay,
				allRecords, allEvents, clbObserver.Snapshots())

			report += buildVerdict55(tl, allRecords)

			framework.Logf("\n%s", report)
		})
	})
})

// ─── Setup helper ───────────────────────────────────────────────────────────

// setupHealthTransition creates the healthserver Deployment and NLB Service,
// discovers the TG, fetches TG config, and waits for ALL TG targets to be
// healthy before returning. Pods are scheduled on master/control-plane nodes
// to match KAS topology. The NLB targets only master nodes via the
// target-node-labels annotation. Cross-zone load balancing is enabled.
func setupHealthTransition(
	ctx context.Context,
	cs clientset.Interface,
	ns *v1.Namespace,
	deployName, svcName, image string,
	replicas int32,
	startupDelay time.Duration,
) (lbDNS string, observer *health.Observer, cfg serviceConfig, setupTimes transitionTimeline, clientPodName string) {

	// Deploy the aggregator first — servers and client will connect to it.
	// The aggregator runs on a worker node with normal networking.
	By("deploying aggregator pod + service on worker node")
	aggregatorURL := deployAggregator(ctx, cs, ns.Name, image)
	framework.Logf("[aggregator] ready at %s", aggregatorURL)

	// Grant the default SA in this namespace permission to use hostNetwork
	// via the OpenShift hostnetwork-v2 SCC. Required because the healthserver
	// pod uses hostNetwork: true to match KAS static pod behavior.
	By("granting privileged SCC to default service account")
	grantHostNetworkSCC(ctx, cs, ns.Name)

	// t0: deployment created — pods begin scheduling on master nodes
	By("creating healthserver Deployment (scheduled on master nodes, hostNetwork)")
	deploy := buildHealthserverDeployment(ns.Name, deployName, replicas, startupDelay, image, aggregatorURL)
	setupTimes.T0 = time.Now()
	_, err := cs.AppsV1().Deployments(ns.Name).Create(ctx, deploy, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create deployment")

	By("creating NLB Service (master-only targets, cross-zone, /readyz HC)")
	svc := buildHealthTransitionService(ns.Name, svcName, deployName)
	_, err = cs.CoreV1().Services(ns.Name).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create service")
	cfg.ServiceAnnotations = svc.Annotations

	// Populate environment summary from the cluster's Infrastructure resource
	cfg.Platform = "AWS"
	if region, rErr := common.GetRegionFromInfrastructure(ctx); rErr == nil {
		cfg.Region = region
	}
	if isExternal, tErr := common.IsExternalTopology(ctx); tErr == nil {
		if isExternal {
			cfg.Topology = "External (HyperShift)"
		} else {
			cfg.Topology = "HighlyAvailable"
		}
	}

	DeferCleanup(func(cleanupCtx context.Context) {
		framework.Logf("cleaning up health transition resources")
		// Clean up all pods/deployments/services created by the test.
		// Order: delete NLB service first (triggers LB deletion), then
		// pods, then wait for LB to be fully removed from AWS.
		_ = cs.CoreV1().Services(ns.Name).Delete(cleanupCtx, svcName, metav1.DeleteOptions{})
		_ = cs.AppsV1().Deployments(ns.Name).Delete(cleanupCtx, deployName, metav1.DeleteOptions{})
		_ = cs.CoreV1().Pods(ns.Name).Delete(cleanupCtx, "healthtest-aggregator", metav1.DeleteOptions{})
		_ = cs.CoreV1().Services(ns.Name).Delete(cleanupCtx, "healthtest-aggregator", metav1.DeleteOptions{})
		_ = cs.CoreV1().Pods(ns.Name).Delete(cleanupCtx, "healthtest-client", metav1.DeleteOptions{})
		if lbDNS != "" {
			waitForLBDeletion(cleanupCtx, lbDNS)
		}
	})

	By("waiting for Deployment rollout")
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(ns.Name).Get(ctx, deployName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		framework.Logf("deployment ready replicas: %d/%d", d.Status.ReadyReplicas, replicas)
		return d.Status.ReadyReplicas >= replicas, nil
	})
	framework.ExpectNoError(err, "deployment rollout")
	// t1: all pods running (startup-delay may still be in progress)
	setupTimes.T1 = time.Now()

	By("waiting for NLB provisioning")
	err = wait.PollUntilContextTimeout(ctx, 10*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		s, err := cs.CoreV1().Services(ns.Name).Get(ctx, svcName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if len(s.Status.LoadBalancer.Ingress) > 0 {
			lbDNS = s.Status.LoadBalancer.Ingress[0].Hostname
			return lbDNS != "", nil
		}
		return false, nil
	})
	framework.ExpectNoError(err, "NLB provisioning")
	// t2: NLB provisioned, DNS assigned
	setupTimes.T2 = time.Now()
	cfg.LBDNS = lbDNS

	By("discovering NLB and target group in AWS")
	elbClient, err := createAWSClientLoadBalancer(ctx)
	framework.ExpectNoError(err, "create ELB client")

	foundLB, err := getAWSLoadBalancerFromDNSName(ctx, elbClient, lbDNS)
	framework.ExpectNoError(err, "find NLB")
	cfg.LBARN = aws.ToString(foundLB.LoadBalancerArn)

	observer = health.NewObserver(elbClient, 1*time.Second)
	err = observer.DiscoverTargetGroup(ctx, cfg.LBARN)
	framework.ExpectNoError(err, "discover target group")
	cfg.TGARN = observer.TargetGroupARN()
	cfg.TGTargetType = observer.TargetType()

	// Fetch TG attributes and HC config for the report
	tgAttrs, err := observer.DescribeTGAttributes(ctx)
	if err == nil {
		cfg.TGAttributes = tgAttrs
	}
	fetchTGHealthCheckConfig(ctx, &cfg)

	// Wait for ALL registered TG targets to be healthy (not just N replicas).
	// With master-only node targeting, this should be exactly 3 targets.
	// Previously we waited for minHealthy=replicas which could pass with
	// worker-node targets while master-node targets were still "initial".
	By("waiting for ALL TG targets to become healthy")
	err = waitForAllTGTargetsHealthy(ctx, observer, 10*time.Minute)
	framework.ExpectNoError(err, "all TG targets healthy")
	// t3: all TG targets healthy — HC passed and propagated through Hyperplane
	setupTimes.T3 = time.Now()

	// Deploy in-cluster client on a worker node. The client sends requests
	// to the NLB with ~1ms RTT (vs ~430ms from external), achieving much
	// higher throughput for better detection coverage.
	By("deploying in-cluster client on worker node")
	clientPodName = deployInClusterClient(ctx, cs, ns.Name, image, lbDNS, aggregatorURL)

	return lbDNS, observer, cfg, setupTimes, clientPodName
}

// waitForAllTGTargetsHealthy polls DescribeTargetHealth directly (via
// observer.PollOnce) until every registered target reports healthy.
// Logs per-target state every 10s so the operator can see convergence.
// This works both during setup (observer not started) and during the test
// (observer running — PollOnce is independent of the background loop).
func waitForAllTGTargetsHealthy(ctx context.Context, observer *health.Observer, timeout time.Duration) error {
	lastLog := time.Time{}
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		snap, err := observer.PollOnce(ctx)
		if err != nil {
			framework.Logf("[tg-wait] poll error: %v", err)
			return false, nil
		}

		total := snap.HealthyCount + snap.UnhealthyCount + snap.InitialCount + snap.DrainingCount
		allHealthy := total > 0 && snap.UnhealthyCount == 0 && snap.InitialCount == 0 && snap.DrainingCount == 0

		// Log every 10s or on state change, showing per-target detail
		if time.Since(lastLog) >= 10*time.Second || allHealthy {
			var details []string
			for id, state := range snap.Targets {
				details = append(details, fmt.Sprintf("%s=%s", id, state))
			}
			framework.Logf("[tg-wait] healthy=%d unhealthy=%d initial=%d total=%d | %s",
				snap.HealthyCount, snap.UnhealthyCount, snap.InitialCount, total,
				strings.Join(details, ", "))
			lastLog = time.Now()
		}

		if allHealthy {
			framework.Logf("[tg-wait] all %d targets healthy", snap.HealthyCount)
		}
		return allHealthy, nil
	})
}

// waitForTGUnhealthy blocks until at least one TG target reports unhealthy.
// This ensures the NLB HC has detected the failure before we start waiting
// for recovery. Without this, waitForAllTGTargetsHealthy may return
// immediately if called before the HC threshold is met.
func waitForTGUnhealthy(ctx context.Context, observer *health.Observer, timeout time.Duration) {
	_ = wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		snap, err := observer.PollOnce(ctx)
		if err != nil {
			return false, nil
		}
		if snap.UnhealthyCount > 0 {
			framework.Logf("[tg-wait] detected %d unhealthy target(s)", snap.UnhealthyCount)
			return true, nil
		}
		return false, nil
	})
}

// startTGSnapshotPusher starts a goroutine that pushes TG health snapshots
// to the aggregator every 2 seconds. This runs the observer's PollOnce and
// sends the result to the aggregator so all TG state changes are captured
// in the aggregator's timeline. Returns a cancel function to stop the goroutine.
func startTGSnapshotPusher(ctx context.Context, cs clientset.Interface, namespace string, observer *health.Observer) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap, err := observer.PollOnce(ctx)
				if err != nil {
					continue
				}
				pushTGSnapshotToAggregator(ctx, cs, namespace, snap)
			}
		}
	}()
	return cancel
}

// fetchTGHealthCheckConfig reads the TG's health check settings from the AWS API
// and appends them to the serviceConfig for report output.
func fetchTGHealthCheckConfig(ctx context.Context, cfg *serviceConfig) {
	elbClient, err := createAWSClientLoadBalancer(ctx)
	if err != nil {
		return
	}
	out, err := elbClient.DescribeTargetGroups(ctx, &elbv2.DescribeTargetGroupsInput{
		TargetGroupArns: []string{cfg.TGARN},
	})
	if err != nil || len(out.TargetGroups) == 0 {
		return
	}
	tg := out.TargetGroups[0]
	cfg.TGAttributes = append(cfg.TGAttributes,
		health.TGAttribute{Key: "_hc_protocol", Value: string(tg.HealthCheckProtocol)},
		health.TGAttribute{Key: "_hc_port", Value: aws.ToString(tg.HealthCheckPort)},
		health.TGAttribute{Key: "_hc_path", Value: aws.ToString(tg.HealthCheckPath)},
		health.TGAttribute{Key: "_hc_interval_seconds", Value: fmt.Sprintf("%d", aws.ToInt32(tg.HealthCheckIntervalSeconds))},
		health.TGAttribute{Key: "_hc_healthy_threshold", Value: fmt.Sprintf("%d", aws.ToInt32(tg.HealthyThresholdCount))},
		health.TGAttribute{Key: "_hc_unhealthy_threshold", Value: fmt.Sprintf("%d", aws.ToInt32(tg.UnhealthyThresholdCount))},
	)
}

// ─── Admin API via K8s API server proxy ─────────────────────────────────────

// sendAdminSignal sends a readyz control signal to a healthserver pod via
// the K8s API server pod proxy. With hostNetwork: true, the pod listens on
// the node's IP on healthserverPort. The API server proxy connects to
// podIP:port which equals nodeIP:port — this requires the API server to be
// able to reach the node on that port (same-node for control-plane pods).
func sendAdminSignal(ctx context.Context, cs clientset.Interface, namespace, podName string, ready bool) error {
	readyStr := "false"
	if ready {
		readyStr = "true"
	}
	result := cs.CoreV1().RESTClient().Post().
		AbsPath(fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy/admin/readyz", namespace, podName, healthserverPort)).
		Param("ready", readyStr).
		Timeout(30 * time.Second).
		Do(ctx)
	if err := result.Error(); err != nil {
		return fmt.Errorf("admin signal ready=%s to %s: %w", readyStr, podName, err)
	}
	framework.Logf("[admin] sent readyz=%s to pod %s", readyStr, podName)
	return nil
}

// ─── Pod lifecycle helpers ──────────────────────────────────────────────────

func waitForNewPod(ctx context.Context, cs clientset.Interface, namespace, deployName, oldPodName string) string {
	var newPod string
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", deployName),
		})
		if err != nil {
			return false, nil
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Name == oldPodName || p.DeletionTimestamp != nil {
				continue
			}
			if p.Status.Phase == v1.PodRunning {
				newPod = p.Name
				return true, nil
			}
		}
		return false, nil
	})
	framework.ExpectNoError(err, "wait for replacement pod")
	return newPod
}

// ─── Timeline computation ───────────────────────────────────────────────────

// isUnhealthyState returns true for any unhealthy TG state, including
// "unhealthy.draining" which occurs when connection_termination.enabled=false
// (CAPA fix / OCPBUGS-55626).
func isUnhealthyState(state string) bool {
	return strings.HasPrefix(state, "unhealthy")
}

// computeTimeline builds the full timing model for Scenario 5.5 from raw
// client records and observer events. See transitionTimeline for the t-value
// definitions aligned with the SPLAT-307 state machine.
func computeTimeline(
	oldPod string,
	knownServers map[string]bool,
	t5, t71 time.Time,
	records []health.RequestRecord,
	events []health.HealthEvent,
) transitionTimeline {
	tl := transitionTimeline{T5: t5, T71: t71}

	// t6: first observer event showing a target transitioning healthy→unhealthy
	// AFTER t5 (when we signaled readyz→503). Excludes initial→unhealthy which
	// are nodes that never had local pods and failed HC from the start.
	for _, e := range events {
		if e.Timestamp.Before(t5) {
			continue
		}
		if isUnhealthyState(e.State) && e.PrevState == "healthy" {
			tl.T6 = e.Timestamp
			break
		}
	}

	// t7: last request served by the OLD target pod after t5.
	// Each request after readyz→503 counts as an "unhealthy" request.
	for _, r := range records {
		if r.Timestamp.Before(t5) {
			continue
		}
		if r.ServerID == oldPod {
			tl.T7 = r.Timestamp
			tl.UnhealthyReqCount++
		}
	}

	// Identify the new pod: first ServerID not in knownServers, after t7.1
	for _, r := range records {
		if r.ServerID == "" || knownServers[r.ServerID] || r.Timestamp.Before(t71) {
			continue
		}
		tl.NewPod = r.ServerID
		break
	}

	// Now process only responses from the identified new pod
	for _, r := range records {
		if r.ServerID != tl.NewPod || r.Timestamp.Before(t71) {
			continue
		}

		// t7.3: first response from new pod (approximates TCP up)
		if tl.T73.IsZero() {
			tl.T73 = r.Timestamp
		}

		// t7.4: first pre-readyz request from new pod
		if r.IsNonReadyReq {
			tl.PreReadyzReqCount++
			if tl.T74.IsZero() {
				tl.T74 = r.Timestamp
			}
		}

		// t8: when the new pod's /readyz first returned 200 (from header, local time).
		// The healthserver runs in UTC inside the container; we convert to local
		// time to match the test's clock for consistent delta calculations.
		if tl.T8.IsZero() && r.ServerState == "ready" && r.FirstReadyzTime != "never" && r.FirstReadyzTime != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, r.FirstReadyzTime); err == nil {
				tl.T8 = parsed.UTC()
			}
		}

		// t10: first client request where new pod reports "ready"
		if tl.T10.IsZero() && r.ServerState == "ready" {
			tl.T10 = r.Timestamp
		}
	}

	// t9: first observer healthy event AFTER t71 (pod restart), not after t8
	// (t8 may be wrong or zero). Look for the healthy transition that corresponds
	// to the new pod coming online.
	for _, e := range events {
		if e.Timestamp.Before(t71) {
			continue
		}
		if e.State == "healthy" && (isUnhealthyState(e.PrevState) || e.PrevState == "initial") {
			tl.T9 = e.Timestamp
			break
		}
	}

	return tl
}

// computeTimeline52 builds the timing model for Scenario 5.2 (no restart).
// The target pod stays alive; we signal readyz→503, observe shutdown propagation,
// then signal readyz→200 and observe recovery.
func computeTimeline52(
	targetPod string,
	t5, t8 time.Time,
	records []health.RequestRecord,
	events []health.HealthEvent,
) transitionTimeline {
	tl := transitionTimeline{T5: t5, T8: t8}

	// t6: first observer event showing a target going unhealthy AFTER t5.
	// Only match healthy→unhealthy transitions (not initial→unhealthy which
	// are nodes that never passed HC, e.g. nodes without local pods).
	for _, e := range events {
		if e.Timestamp.Before(t5) {
			continue
		}
		if isUnhealthyState(e.State) && e.PrevState == "healthy" {
			tl.T6 = e.Timestamp
			break
		}
	}

	// t7: last request served by the target pod after t5 and before t8.
	// Each such request is "unhealthy" because readyz was 503.
	for _, r := range records {
		if r.Timestamp.Before(t5) || r.Timestamp.After(t8) {
			continue
		}
		if r.ServerID == targetPod {
			tl.T7 = r.Timestamp
			tl.UnhealthyReqCount++
		}
	}

	// t9: first observer event showing a target going healthy AFTER t8.
	// Match unhealthy→healthy (recovery after we signaled readyz→200).
	for _, e := range events {
		if e.Timestamp.Before(t8) {
			continue
		}
		if e.State == "healthy" && isUnhealthyState(e.PrevState) {
			tl.T9 = e.Timestamp
			break
		}
	}

	// t10: first request to the target pod after recovery (after t9 if known,
	// otherwise after t8).
	searchAfter := t8
	if !tl.T9.IsZero() {
		searchAfter = tl.T9
	}
	for _, r := range records {
		if r.Timestamp.Before(searchAfter) {
			continue
		}
		if r.ServerID == targetPod {
			tl.T10 = r.Timestamp
			break
		}
	}

	return tl
}

// ─── Report (single block, no per-line logger timestamps) ───────────────────

// fmtT formats a time in UTC to avoid timezone mismatches between the
// test binary (local TZ) and containers (UTC). All timestamps in the
// report use UTC for consistent comparison.
func fmtT(t time.Time) string {
	if t.IsZero() {
		return "N/A"
	}
	return t.UTC().Format(time.RFC3339)
}

func fmtDelta(base, t time.Time) string {
	if t.IsZero() || base.IsZero() {
		return ""
	}
	return fmt.Sprintf("[+%s]", t.Sub(base).Truncate(time.Millisecond))
}

func fmtDur(a, b time.Time) string {
	if a.IsZero() || b.IsZero() {
		return "N/A"
	}
	return b.Sub(a).Truncate(time.Millisecond).String()
}

// timelineEntry is a single row in the unified chronological timeline.
type timelineEntry struct {
	t     time.Time
	label string
	delta string
}

func buildReport(
	scenario string,
	tl transitionTimeline,
	cfg serviceConfig,
	replicas int32,
	startupDelay, shutdownDelay time.Duration,
	records []health.RequestRecord,
	events []health.HealthEvent,
	snapshots []health.TargetSnapshot,
) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	sep := "═══════════════════════════════════════════════════════════════════════════"

	w(sep)
	w("HEALTH TRANSITION REPORT — Scenario %s", scenario)
	w(sep)

	// ── Environment ──
	w("")
	w("ENVIRONMENT")
	w("  Platform:       %s", cfg.Platform)
	w("  Region:         %s", cfg.Region)
	w("  Topology:       %s", cfg.Topology)

	// ── Identity ──
	w("")
	w("TARGET")
	w("  Pod:            %s", tl.TargetPod)
	w("  Node:           %s", tl.TargetNode)
	if tl.NewPod != "" {
		w("  New Pod:        %s", tl.NewPod)
	}

	// ── Test params ──
	w("")
	w("TEST PARAMETERS")
	w("  Replicas:       %d", replicas)
	w("  Startup Delay:  %s", startupDelay)
	if shutdownDelay > 0 {
		w("  Shutdown Delay: %s", shutdownDelay)
	}
	w("  Client Interval: %s (%d parallel workers)", defaultClientInterval, defaultClientWorkers)

	// ── Service config ──
	w("")
	w("SERVICE CONFIGURATION")
	w("  LB DNS:         %s", cfg.LBDNS)
	w("  LB ARN:         %s", cfg.LBARN)
	for k, v := range cfg.ServiceAnnotations {
		short := strings.TrimPrefix(k, "service.beta.kubernetes.io/aws-load-balancer-")
		w("  svc/%s: %s", short, v)
	}

	// ── TG config ──
	w("")
	w("TARGET GROUP CONFIGURATION")
	w("  TG ARN:         %s", cfg.TGARN)
	w("  Target Type:    %s", cfg.TGTargetType)
	for _, a := range cfg.TGAttributes {
		if a.Key == "" {
			continue
		}
		w("  %s: %s", a.Key, a.Value)
	}

	// ── Timing table ──
	w("")
	w("TIMING TABLE")
	w("%-25s %-14s %-14s %s", "Metric", "Value", "Expected", "Description")
	w("%-25s %-14s %-14s %s", strings.Repeat("─", 25), strings.Repeat("─", 14), strings.Repeat("─", 14), strings.Repeat("─", 30))
	w("%-25s %-14s %-14s %s", "T_deploy_ready", fmtDur(tl.T0, tl.T1), "", "t1-t0: pods scheduled + running")
	w("%-25s %-14s %-14s %s", "T_nlb_provision", fmtDur(tl.T0, tl.T2), "", "t2-t0: NLB provisioned")
	w("%-25s %-14s %-14s %s", "T_tg_initial_healthy", fmtDur(tl.T0, tl.T3), "", "t3-t0: all TG targets healthy")
	w("%-25s %-14s %-14s %s", "T_first_request", fmtDur(tl.T3, tl.T4), "seconds", "t4-t3: first routed request")
	w("%-25s %-14s %-14s %s", "", "", "", "")
	w("%-25s %-14s %-14s %s", "T_tg_unhealthy", fmtDur(tl.T5, tl.T6), "~20s", "t6-t5: HC detect unhealthy")
	w("%-25s %-14s %-14s %s", "T_route_stop", fmtDur(tl.T5, tl.T7), "<shutdown-delay", "t7-t5: last req after readyz→503")
	w("%-25s %-14d %-14s %s", "Unhealthy_reqs", tl.UnhealthyReqCount, "0 ideal", "requests to target after readyz→503")
	if !tl.T71.IsZero() {
		w("%-25s %-14s %-14s %s", "T_pod_restart", fmtDur(tl.T71, tl.T73), "seconds", "t7.3-t7.1: pod kill→TCP up")
		w("%-25s %-14d %-14s %s", "Pre_readyz_reqs", tl.PreReadyzReqCount, "0", "requests before readyz→200 (BUG)")
	}
	w("%-25s %-14s %-14s %s", "T_tg_healthy", fmtDur(tl.T8, tl.T9), "~20s", "t9-t8: HC detect healthy")
	w("%-25s %-14s %-14s %s", "T_route_start", fmtDur(tl.T8, tl.T10), "20-120s", "t10-t8: first req after readyz→200")
	w("%-25s %-14s %-14s %s", "T_total_cycle", fmtDur(tl.T5, tl.T10), "", "t10-t5: full cycle")

	// ── Request statistics ──
	// Compute overall and per-phase request counts from client records.
	var totalReqs, reqs2xx, reqs4xx, reqs5xx, reqsErr int
	for _, r := range records {
		totalReqs++
		switch {
		case r.Error != "":
			reqsErr++
		case r.HTTPStatus >= 200 && r.HTTPStatus < 300:
			reqs2xx++
		case r.HTTPStatus >= 400 && r.HTTPStatus < 500:
			reqs4xx++
		case r.HTTPStatus >= 500:
			reqs5xx++
		}
	}

	// Compute average req/s across the full test duration (t3→last record)
	var avgReqsPerSec float64
	var testDuration time.Duration
	if len(records) > 1 {
		testDuration = records[len(records)-1].Timestamp.Sub(records[0].Timestamp)
		if testDuration > 0 {
			avgReqsPerSec = float64(totalReqs) / testDuration.Seconds()
		}
	}

	w("")
	w("REQUEST STATISTICS")
	w("  Total:    %d", totalReqs)
	w("  2xx:      %d", reqs2xx)
	w("  4xx:      %d", reqs4xx)
	w("  5xx:      %d", reqs5xx)
	w("  Errors:   %d (connection/timeout failures)", reqsErr)
	w("  Duration: %s", testDuration.Truncate(time.Second))
	w("  Avg rate: %.1f req/s", avgReqsPerSec)

	// ── Per-phase request breakdown ──
	// Phases are defined by the timeline milestones:
	//   Warmup:           t3→t5  (all targets healthy, steady-state traffic)
	//   GracefulShutdown: t5→t7  (SIGTERM received, readyz→503, NLB still routing)
	//   Restart:          t7→t9  (NLB stopped routing, pod terminated, new pod starting)
	//   Recovery:         t9→end (new target healthy, traffic flowing)
	// For Scenario 5.2 (no restart): GracefulShutdown=t5→t8, Recovery=t8→end
	type phaseStats struct {
		name                   string
		from, to               time.Time
		total, ok, err, preRdz int
	}
	var phases []phaseStats

	classifyPhase := func(name string, from, to time.Time) phaseStats {
		ps := phaseStats{name: name, from: from, to: to}
		for _, r := range records {
			if (!from.IsZero() && r.Timestamp.Before(from)) || (!to.IsZero() && r.Timestamp.After(to)) {
				continue
			}
			ps.total++
			if r.Error != "" {
				ps.err++
			} else if r.HTTPStatus >= 200 && r.HTTPStatus < 300 {
				ps.ok++
			}
			if r.IsNonReadyReq {
				ps.preRdz++
			}
		}
		return ps
	}

	// Warmup: t3 (all healthy) → t5 (readyz→503).  Includes steady state.
	phases = append(phases, classifyPhase("Warmup (t3→t5)", tl.T3, tl.T5))

	if !tl.T71.IsZero() {
		// Scenario 5.5: has restart phase
		phases = append(phases, classifyPhase("GracefulShutdown (t5→t7)", tl.T5, tl.T7))
		phases = append(phases, classifyPhase("Restart (t7→t9)", tl.T7, tl.T9))
		phases = append(phases, classifyPhase("Recovery (t9→end)", tl.T9, time.Time{}))
	} else {
		// Scenario 5.2: no restart
		phases = append(phases, classifyPhase("GracefulShutdown (t5→t8)", tl.T5, tl.T8))
		phases = append(phases, classifyPhase("Recovery (t8→end)", tl.T8, time.Time{}))
	}

	w("")
	w("REQUEST BREAKDOWN BY PHASE")
	w("%-25s %10s %8s %8s %8s %8s %10s", "Phase", "Duration", "Total", "2xx", "Errors", "PreRdz", "Avg req/s")
	w("%-25s %10s %8s %8s %8s %8s %10s", strings.Repeat("─", 25), strings.Repeat("─", 10), strings.Repeat("─", 8), strings.Repeat("─", 8), strings.Repeat("─", 8), strings.Repeat("─", 8), strings.Repeat("─", 10))
	for _, ps := range phases {
		dur := "N/A"
		rps := "N/A"
		var phaseDur time.Duration
		if !ps.from.IsZero() && !ps.to.IsZero() {
			phaseDur = ps.to.Sub(ps.from)
		} else if !ps.from.IsZero() && len(records) > 0 {
			// Open-ended phase (→end): use last record timestamp
			phaseDur = records[len(records)-1].Timestamp.Sub(ps.from)
		}
		if phaseDur > 0 {
			dur = phaseDur.Truncate(time.Second).String()
			rps = fmt.Sprintf("%.1f", float64(ps.total)/phaseDur.Seconds())
		}
		w("%-25s %10s %8d %8d %8d %8d %10s", ps.name, dur, ps.total, ps.ok, ps.err, ps.preRdz, rps)
	}

	// ── Per-server request distribution by phase ──
	// Shows how many requests each backend (ServerID/pod) received in each phase.
	// This is the key metric for detecting routing anomalies: if the target pod
	// receives requests during Restart (after deletion), that's the NLB bug.
	// Collect unique server IDs across all records
	serverSet := make(map[string]bool)
	for _, r := range records {
		if r.ServerID != "" {
			serverSet[r.ServerID] = true
		}
	}
	var serverIDs []string
	for id := range serverSet {
		serverIDs = append(serverIDs, id)
	}
	sort.Strings(serverIDs)

	if len(serverIDs) > 0 {
		// Build per-server per-phase counts
		type serverPhaseCount struct {
			total, preRdz int
		}
		// phaseServerCounts[phaseIdx][serverID] = counts
		phaseServerCounts := make([]map[string]serverPhaseCount, len(phases))
		for i, ps := range phases {
			phaseServerCounts[i] = make(map[string]serverPhaseCount)
			for _, r := range records {
				if r.ServerID == "" {
					continue
				}
				if (!ps.from.IsZero() && r.Timestamp.Before(ps.from)) || (!ps.to.IsZero() && r.Timestamp.After(ps.to)) {
					continue
				}
				sc := phaseServerCounts[i][r.ServerID]
				sc.total++
				if r.IsNonReadyReq {
					sc.preRdz++
				}
				phaseServerCounts[i][r.ServerID] = sc
			}
		}

		w("")
		w("PER-SERVER REQUEST DISTRIBUTION BY PHASE")

		// Annotate server IDs with their role and node name.
		// Format: "pod-name (node-name) ← TARGET"
		serverLabel := func(id string) string {
			node := ""
			if tl.PodNodeMap != nil {
				node = tl.PodNodeMap[id]
			}
			role := ""
			switch id {
			case tl.TargetPod:
				role = " ← TARGET"
			case tl.NewPod:
				role = " ← NEW"
			}
			if node != "" {
				return fmt.Sprintf("%s (%s)%s", id, node, role)
			}
			return id + role
		}

		// Print a sub-table per phase showing each server's request count
		for i, ps := range phases {
			dur := "N/A"
			if !ps.from.IsZero() && !ps.to.IsZero() {
				dur = ps.to.Sub(ps.from).Truncate(time.Second).String()
			} else if !ps.from.IsZero() && len(records) > 0 {
				dur = records[len(records)-1].Timestamp.Sub(ps.from).Truncate(time.Second).String()
			}
			w("  %s (%s):", ps.name, dur)
			for _, sid := range serverIDs {
				sc := phaseServerCounts[i][sid]
				if sc.total == 0 {
					continue
				}
				preRdzNote := ""
				if sc.preRdz > 0 {
					preRdzNote = fmt.Sprintf("  ← %d pre-readyz!", sc.preRdz)
				}
				w("    %-50s  reqs=%d%s", serverLabel(sid), sc.total, preRdzNote)
			}
		}
	}

	// ── Unified chronological timeline ──
	w("")
	w("TIMELINE")
	w("%-27s %-28s %s", "Time", "Event", "Delta")
	w("%-27s %-28s %s", strings.Repeat("─", 27), strings.Repeat("─", 28), strings.Repeat("─", 20))

	var entries []timelineEntry

	addEntry := func(t time.Time, label, delta string) {
		if !t.IsZero() {
			entries = append(entries, timelineEntry{t: t, label: label, delta: delta})
		}
	}

	// Initial registration milestones (t0-t4)
	addEntry(tl.T0, "t0  deploy created", "")
	addEntry(tl.T1, "t1  pods ready", fmtDelta(tl.T0, tl.T1))
	addEntry(tl.T2, "t2  NLB provisioned", fmtDelta(tl.T0, tl.T2))
	addEntry(tl.T3, "t3  TG all healthy", fmtDelta(tl.T0, tl.T3))
	addEntry(tl.T4, "t4  first request", fmtDelta(tl.T3, tl.T4))

	// Shutdown/restart milestones (t5-t10)
	addEntry(tl.T5, "t5  readyz→503", "")
	addEntry(tl.T6, "t6  TG unhealthy", fmtDelta(tl.T5, tl.T6))
	addEntry(tl.T7, "t7  last routed req", fmtDelta(tl.T5, tl.T7))
	addEntry(tl.T71, "t7.1 pod deleted (SIGTERM sent)", fmtDelta(tl.T5, tl.T71))
	addEntry(tl.T73, "t7.3 new TCP up", fmtDelta(tl.T71, tl.T73))
	if !tl.T74.IsZero() {
		addEntry(tl.T74, "t7.4 pre-readyz req ← BUG", fmtDelta(tl.T73, tl.T74))
	}
	addEntry(tl.T8, "t8  readyz→200", fmtDelta(tl.T5, tl.T8))
	addEntry(tl.T9, "t9  TG healthy", fmtDelta(tl.T8, tl.T9))
	addEntry(tl.T10, "t10 first routed req", fmtDelta(tl.T8, tl.T10))

	// TG health events
	for _, e := range events {
		addEntry(e.Timestamp,
			fmt.Sprintf("TG  %s→%s", e.PrevState, e.State),
			fmt.Sprintf("target=%s", e.TargetID))
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].t.Before(entries[j].t) })

	for _, e := range entries {
		w("%-27s %-28s %s", fmtT(e.t), e.label, e.delta)
	}

	// ── Snapshot summary ──
	if len(snapshots) > 0 {
		first := snapshots[0]
		last := snapshots[len(snapshots)-1]
		w("")
		w("TG SNAPSHOTS (%d polls, %s duration)", len(snapshots),
			last.Timestamp.Sub(first.Timestamp).Truncate(time.Second))
		w("  first: %s  healthy=%d unhealthy=%d initial=%d",
			fmtT(first.Timestamp), first.HealthyCount, first.UnhealthyCount, first.InitialCount)
		w("  last:  %s  healthy=%d unhealthy=%d initial=%d",
			fmtT(last.Timestamp), last.HealthyCount, last.UnhealthyCount, last.InitialCount)
	}

	w(sep)
	return b.String()
}

// ─── Resource builders ──────────────────────────────────────────────────────

// ─── Verdict builders ───────────────────────────────────────────────────────

// buildVerdict55 produces the verdict string for Scenario 5.5 (pre-readyz routing).
// It checks both client-side (X-Server-State: pre-readyz) and server-side
// (did the target pod receive requests during Shutdown/Restart phases).
func buildVerdict55(tl transitionTimeline, records []health.RequestRecord) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	// Count requests to the target pod AFTER readyz→503 (shutdown phase)
	var targetAfterShutdown int
	for _, r := range records {
		if r.Timestamp.Before(tl.T5) || r.ServerID != tl.TargetPod {
			continue
		}
		targetAfterShutdown++
	}

	// Count requests to the target pod's node during Restart phase (t7→t9).
	// Start from t7 (last routed request), NOT t7.1 (pod delete/SIGTERM),
	// because requests between t5→t7 are expected GracefulShutdown traffic
	// (NLB propagation delay) and are already reported by [SHUTDOWN].
	// Requests AFTER t7 mean the LB re-routed to the target unexpectedly.
	var targetDuringRestart int
	if !tl.T7.IsZero() {
		end := tl.T9
		if end.IsZero() {
			end = tl.T10
		}
		for _, r := range records {
			if r.Timestamp.Before(tl.T7) {
				continue
			}
			if !end.IsZero() && r.Timestamp.After(end) {
				continue
			}
			// Match the target pod OR the new pod
			if r.ServerID == tl.TargetPod || r.ServerID == tl.NewPod {
				if r.ServerState == "pre-readyz" || r.ServerState == "draining" || r.ServerState == "shutdown" {
					targetDuringRestart++
				}
			}
		}
	}

	w("")
	w("VERDICT")

	if tl.PreReadyzReqCount > 0 {
		w("  [BUG] NLB routed %d request(s) with X-Server-State: pre-readyz", tl.PreReadyzReqCount)
		w("        This reproduces OCPBUGS-86789 — NLB routes before /readyz passes")
	}

	if targetAfterShutdown > 0 {
		w("  [SHUTDOWN] Target pod received %d request(s) after readyz→503 (T_route_stop=%s)",
			targetAfterShutdown, fmtDur(tl.T5, tl.T7))
		w("             NLB continued routing to unhealthy target for %s", fmtDur(tl.T5, tl.T7))
	}

	if targetDuringRestart > 0 {
		w("  [RESTART] Target node received %d unhealthy/pre-readyz request(s) during Restart phase", targetDuringRestart)
	}

	if tl.PreReadyzReqCount == 0 && targetDuringRestart == 0 {
		w("  [OK] No pre-readyz routing detected")
		w("       NLB correctly waited for HC to pass before routing to restarted target")
	}

	if targetAfterShutdown > 0 {
		w("  [INFO] Shutdown propagation: %d requests routed to target after readyz→503 (expected: NLB propagation delay)",
			targetAfterShutdown)
	}

	return b.String()
}

// buildVerdict52 produces the verdict string for Scenario 5.2 (shutdown propagation).
func buildVerdict52(tl transitionTimeline) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	w("")
	w("VERDICT")
	w("  NLB routed %d request(s) to unhealthy target after readyz→503", tl.UnhealthyReqCount)
	if !tl.T7.IsZero() && !tl.T5.IsZero() {
		w("  T_route_stop = %s (NLB kept routing after readyz→503)",
			tl.T7.Sub(tl.T5).Truncate(time.Second))
	}
	if !tl.T10.IsZero() && !tl.T8.IsZero() {
		w("  T_route_start = %s (NLB started routing after readyz→200)",
			tl.T10.Sub(tl.T8).Truncate(time.Second))
	}

	return b.String()
}

// ─── Resource builders ──────────────────────────────────────────────────────

// buildHealthserverDeployment creates a Deployment spec that schedules pods on
// master/control-plane nodes to match KAS topology. Includes tolerations for
// both master and control-plane taints, and topologySpreadConstraints to
// distribute pods across nodes.
func buildHealthserverDeployment(namespace, name string, replicas int32, startupDelay time.Duration, image string, aggregatorURL ...string) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: v1.PodSpec{
					// hostNetwork: pod binds directly on the node's network
					// interface, exactly like KAS static pods. The NLB health
					// check hits nodeIP:19443/readyz directly — no kube-proxy
					// mediation. This is essential for reproducing OCPBUGS-86789.
					HostNetwork: true,
					DNSPolicy:   v1.DNSClusterFirstWithHostNet,
					// Schedule on control-plane nodes to match KAS topology.
					// OCP 5.x uses control-plane; OCP 4.x has both labels.
					NodeSelector: map[string]string{
						"node-role.kubernetes.io/control-plane": "",
					},
					// terminationGracePeriodSeconds matches KAS
					// shutdown-delay-duration. After SIGTERM, the healthserver
					// sets readyz→503 and keeps serving for this duration.
					TerminationGracePeriodSeconds: ptrInt64(int64(kasShutdownDelay.Seconds())),
					// Tolerate master and control-plane taints
					Tolerations: []v1.Toleration{
						{Key: "node-role.kubernetes.io/master", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule},
						{Key: "node-role.kubernetes.io/control-plane", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule},
					},
					TopologySpreadConstraints: []v1.TopologySpreadConstraint{{
						MaxSkew:           1,
						TopologyKey:       "kubernetes.io/hostname",
						WhenUnsatisfiable: v1.ScheduleAnyway,
						LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
					}},
					Containers: []v1.Container{{
						Name:  "healthserver",
						Image: image,
						Args: func() []string {
							// Use the unified binary with "serve" subcommand.
							// If aggregatorURL is provided, pass it so the server
							// pushes lifecycle events to the aggregator.
							args := []string{
								"serve",
								fmt.Sprintf("--port=%d", healthserverPort),
								fmt.Sprintf("--startup-delay=%s", startupDelay),
							}
							if len(aggregatorURL) > 0 && aggregatorURL[0] != "" {
								args = append(args, fmt.Sprintf("--aggregator=%s", aggregatorURL[0]))
							}
							return args
						}(),
						Ports: []v1.ContainerPort{{
							Name:          "http",
							ContainerPort: healthserverPort,
							HostPort:      healthserverPort,
						}},
						// SecurityContext: let OpenShift assign the UID from the
						// namespace range. The privileged SCC handles hostNetwork.
						SecurityContext: &v1.SecurityContext{
							AllowPrivilegeEscalation: ptrBool(false),
							Capabilities: &v1.Capabilities{
								Drop: []v1.Capability{"ALL"},
							},
							SeccompProfile: &v1.SeccompProfile{
								Type: v1.SeccompProfileTypeRuntimeDefault,
							},
						},
						Env: []v1.EnvVar{
							{
								Name: "POD_NAME",
								ValueFrom: &v1.EnvVarSource{
									FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"},
								},
							},
							{
								// POD_IP is used to register with the aggregator
								// using the real node IP (hostNetwork pod).
								Name: "POD_IP",
								ValueFrom: &v1.EnvVarSource{
									FieldRef: &v1.ObjectFieldSelector{FieldPath: "status.podIP"},
								},
							},
						},
					}},
				},
			},
		},
	}
}

// buildHealthTransitionService creates a Service spec for an NLB that:
// - Targets only master/control-plane nodes (target-node-labels annotation)
// - Enables cross-zone load balancing for HA
// - Uses HTTP /readyz health check with 10s interval and threshold=2
// - Uses externalTrafficPolicy: Local for per-node health tracking
func buildHealthTransitionService(namespace, name, deployName string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				"service.beta.kubernetes.io/aws-load-balancer-type":                            "nlb",
				"service.beta.kubernetes.io/aws-load-balancer-target-node-labels":              "node-role.kubernetes.io/control-plane=",
				"service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled": "true",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-protocol":            "HTTP",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-path":                "/readyz",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-port":                fmt.Sprintf("%d", healthserverPort),
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-interval":            "10",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-healthy-threshold":   "2",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-unhealthy-threshold": "2",
			},
		},
		Spec: v1.ServiceSpec{
			Type:                  v1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			Selector:              map[string]string{"app": deployName},
			Ports: []v1.ServicePort{{
				Name:       "http",
				Protocol:   v1.ProtocolTCP,
				Port:       int32(healthserverPort),
				TargetPort: intstr.FromInt(healthserverPort),
			}},
		},
	}
}

func waitForLBDeletion(ctx context.Context, lbDNS string) {
	elbClient, err := createAWSClientLoadBalancer(ctx)
	if err != nil {
		return
	}
	_ = wait.PollUntilContextTimeout(ctx, 10*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		lb, err := findAWSLoadBalancerByDNSName(ctx, elbClient, lbDNS)
		if err != nil {
			return false, nil
		}
		return lb == nil, nil
	})
}

// grantHostNetworkSCC creates a RoleBinding that grants the default service
// account in the given namespace access to the privileged SCC. This is
// required on OpenShift for pods with hostNetwork: true. The privileged SCC
// allows hostNetwork, hostPort, and any UID — matching what static pods
// (like KAS) use on control-plane nodes.
func grantHostNetworkSCC(ctx context.Context, cs clientset.Interface, namespace string) {
	rbName := "healthserver-privileged"
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rbName,
			Namespace: namespace,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      "default",
			Namespace: namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "system:openshift:scc:privileged",
		},
	}
	_, err := cs.RbacV1().RoleBindings(namespace).Create(ctx, rb, metav1.CreateOptions{})
	framework.ExpectNoError(err, "grant privileged SCC to default SA")
}

// ─── In-cluster aggregator + client deployment ─────────────────────────────

// deployAggregator creates a Pod and ClusterIP Service for the aggregator
// on a worker node. Returns the service DNS name for other pods to connect.
func deployAggregator(ctx context.Context, cs clientset.Interface, namespace, image string) string {
	svcName := "healthtest-aggregator"
	podName := "healthtest-aggregator"

	// Pod
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels:    map[string]string{"app": "healthtest-aggregator"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "aggregator",
				Image: image,
				Args:  []string{"aggregator", fmt.Sprintf("--port=%d", aggregatorPort), "--scrape-interval=1s"},
				Ports: []v1.ContainerPort{{
					Name:          "http",
					ContainerPort: int32(aggregatorPort),
				}},
				ReadinessProbe: &v1.Probe{
					ProbeHandler: v1.ProbeHandler{
						HTTPGet: &v1.HTTPGetAction{
							Path: "/healthz",
							Port: intstr.FromInt(aggregatorPort),
						},
					},
					PeriodSeconds: 2,
				},
			}},
		},
	}
	_, err := cs.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create aggregator pod")

	// ClusterIP Service so servers and client can reach the aggregator by DNS
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespace,
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"app": "healthtest-aggregator"},
			Ports: []v1.ServicePort{{
				Port:       int32(aggregatorPort),
				TargetPort: intstr.FromInt(aggregatorPort),
			}},
		},
	}
	_, err = cs.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create aggregator service")

	// Wait for aggregator pod ready
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		p, err := cs.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, c := range p.Status.Conditions {
			if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	framework.ExpectNoError(err, "aggregator pod ready")

	// Return the in-cluster DNS name for the aggregator service
	return fmt.Sprintf("http://%s.%s.svc:%d", svcName, namespace, aggregatorPort)
}

// deployInClusterClient creates a Pod on a worker node that sends HTTP
// requests to the NLB. Returns the pod name for result fetching.
func deployInClusterClient(ctx context.Context, cs clientset.Interface, namespace, image, nlbDNS, aggregatorURL string) string {
	podName := "healthtest-client"

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels:    map[string]string{"app": "healthtest-client"},
		},
		Spec: v1.PodSpec{
			// Schedule on worker nodes (NOT control-plane)
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{{
							MatchExpressions: []v1.NodeSelectorRequirement{{
								Key:      "node-role.kubernetes.io/worker",
								Operator: v1.NodeSelectorOpExists,
							}},
						}},
					},
				},
			},
			Containers: []v1.Container{{
				Name:  "client",
				Image: image,
				// POD_IP is used by the client to register with the
				// aggregator using its real pod IP (not localhost).
				Env: []v1.EnvVar{{
					Name: "POD_IP",
					ValueFrom: &v1.EnvVarSource{
						FieldRef: &v1.ObjectFieldSelector{FieldPath: "status.podIP"},
					},
				}},
				Args: []string{
					"client",
					fmt.Sprintf("--url=http://%s:%d/", nlbDNS, healthserverPort),
					fmt.Sprintf("--workers=%d", defaultClientWorkers),
					fmt.Sprintf("--interval=%s", defaultClientInterval),
					fmt.Sprintf("--port=%d", clientPort),
					fmt.Sprintf("--aggregator=%s", aggregatorURL),
				},
				Ports: []v1.ContainerPort{{
					Name:          "http",
					ContainerPort: int32(clientPort),
				}},
				ReadinessProbe: &v1.Probe{
					ProbeHandler: v1.ProbeHandler{
						HTTPGet: &v1.HTTPGetAction{
							Path: "/healthz",
							Port: intstr.FromInt(clientPort),
						},
					},
					PeriodSeconds: 2,
				},
			}},
		},
	}
	_, err := cs.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create client pod")

	// Wait for client pod ready (starts sending requests immediately)
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		p, err := cs.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, c := range p.Status.Conditions {
			if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	framework.ExpectNoError(err, "client pod ready")
	framework.Logf("[client-pod] started on worker node, sending requests to NLB")

	return podName
}

// fetchClientRecords retrieves all request records from the in-cluster
// client pod via the K8s API server proxy. The client pod runs on a worker
// node with normal networking, so the API proxy works.
func fetchClientRecords(ctx context.Context, cs clientset.Interface, namespace, clientPodName string) []health.RequestRecord {
	result := cs.CoreV1().RESTClient().Get().
		AbsPath(fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy/records", namespace, clientPodName, clientPort)).
		Timeout(30 * time.Second).
		Do(ctx)
	if err := result.Error(); err != nil {
		framework.Logf("warning: failed to fetch client records: %v", err)
		return nil
	}
	raw, err := result.Raw()
	if err != nil {
		framework.Logf("warning: failed to read client records: %v", err)
		return nil
	}

	// The client returns ClientRecord (types from the unified binary).
	// Map to health.RequestRecord for compatibility with existing analysis.
	type clientRecord struct {
		Timestamp       time.Time `json:"timestamp"`
		TargetIP        string    `json:"target_ip"`
		TCPDialDuration int64     `json:"tcp_dial_ms"`
		HTTPStatus      int       `json:"http_status"`
		ServerState     string    `json:"server_state"`
		ServerID        string    `json:"server_id"`
		FirstReadyzTime string    `json:"first_readyz_time"`
		IsNonReadyReq   bool      `json:"is_non_ready_req"`
		Error           string    `json:"error,omitempty"`
	}
	var crs []clientRecord
	if err := json.Unmarshal(raw, &crs); err != nil {
		framework.Logf("warning: failed to parse client records: %v", err)
		return nil
	}

	records := make([]health.RequestRecord, len(crs))
	for i, cr := range crs {
		records[i] = health.RequestRecord{
			Timestamp:       cr.Timestamp,
			TargetIP:        cr.TargetIP,
			TCPDialDuration: time.Duration(cr.TCPDialDuration) * time.Millisecond,
			HTTPStatus:      cr.HTTPStatus,
			ServerState:     cr.ServerState,
			ServerID:        cr.ServerID,
			FirstReadyzTime: cr.FirstReadyzTime,
			IsNonReadyReq:   cr.IsNonReadyReq,
			Error:           cr.Error,
		}
	}
	framework.Logf("[client-pod] fetched %d records from in-cluster client", len(records))
	return records
}

// pushTGSnapshotToAggregator sends a TG health snapshot to the aggregator
// via K8s API proxy. Non-blocking — errors are logged but don't fail the test.
func pushTGSnapshotToAggregator(ctx context.Context, cs clientset.Interface, namespace string, snap health.TargetSnapshot) {
	payload := struct {
		Timestamp      time.Time         `json:"timestamp"`
		Targets        map[string]string `json:"targets"`
		HealthyCount   int               `json:"healthy_count"`
		UnhealthyCount int               `json:"unhealthy_count"`
		InitialCount   int               `json:"initial_count"`
	}{
		Timestamp:      snap.Timestamp,
		Targets:        snap.Targets,
		HealthyCount:   snap.HealthyCount,
		UnhealthyCount: snap.UnhealthyCount,
		InitialCount:   snap.InitialCount,
	}
	data, _ := json.Marshal(payload)
	cs.CoreV1().RESTClient().Post().
		AbsPath(fmt.Sprintf("/api/v1/namespaces/%s/pods/healthtest-aggregator:%d/proxy/tg-snapshot", namespace, aggregatorPort)).
		Body(data).
		Do(ctx)
}

// fetchAggregatorTimeline retrieves the merged event timeline from the aggregator.
func fetchAggregatorTimeline(ctx context.Context, cs clientset.Interface, namespace string) []map[string]interface{} {
	result := cs.CoreV1().RESTClient().Get().
		AbsPath(fmt.Sprintf("/api/v1/namespaces/%s/pods/healthtest-aggregator:%d/proxy/timeline", namespace, aggregatorPort)).
		Timeout(30 * time.Second).
		Do(ctx)
	raw, _ := result.Raw()
	var timeline []map[string]interface{}
	json.Unmarshal(raw, &timeline)
	return timeline
}

// ─── CLB (Classic Load Balancer) support ────────────────────────────────────

// setupHealthTransitionCLB creates the same infrastructure as setupHealthTransition
// but uses a Classic Load Balancer instead of NLB. The CLB observer uses the
// ELB v1 DescribeInstanceHealth API. Everything else (healthserver deployment,
// aggregator, in-cluster client) is identical.
func setupHealthTransitionCLB(
	ctx context.Context,
	cs clientset.Interface,
	ns *v1.Namespace,
	deployName, svcName, image string,
	replicas int32,
	startupDelay time.Duration,
) (lbDNS string, clbObserver *health.CLBObserver, cfg serviceConfig, setupTimes transitionTimeline, clientPodName string) {

	// Deploy aggregator first
	By("deploying aggregator pod + service on worker node")
	aggregatorURL := deployAggregator(ctx, cs, ns.Name, image)
	framework.Logf("[aggregator] ready at %s", aggregatorURL)

	// SCC for hostNetwork
	By("granting privileged SCC to default service account")
	grantHostNetworkSCC(ctx, cs, ns.Name)

	// Healthserver deployment (same as NLB)
	By("creating healthserver Deployment (scheduled on master nodes, hostNetwork)")
	deploy := buildHealthserverDeployment(ns.Name, deployName, replicas, startupDelay, image, aggregatorURL)
	setupTimes.T0 = time.Now()
	_, err := cs.AppsV1().Deployments(ns.Name).Create(ctx, deploy, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create deployment")

	// CLB Service (no nlb annotation = CLB default)
	By("creating CLB Service (master-only targets, cross-zone, /readyz HC)")
	svc := buildHealthTransitionServiceCLB(ns.Name, svcName, deployName)
	_, err = cs.CoreV1().Services(ns.Name).Create(ctx, svc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create CLB service")
	cfg.ServiceAnnotations = svc.Annotations

	cfg.Platform = "AWS"
	if region, rErr := common.GetRegionFromInfrastructure(ctx); rErr == nil {
		cfg.Region = region
	}
	if isExternal, tErr := common.IsExternalTopology(ctx); tErr == nil {
		if isExternal {
			cfg.Topology = "External (HyperShift)"
		} else {
			cfg.Topology = "HighlyAvailable"
		}
	}

	DeferCleanup(func(cleanupCtx context.Context) {
		framework.Logf("cleaning up CLB health transition resources")
		_ = cs.CoreV1().Services(ns.Name).Delete(cleanupCtx, svcName, metav1.DeleteOptions{})
		_ = cs.AppsV1().Deployments(ns.Name).Delete(cleanupCtx, deployName, metav1.DeleteOptions{})
		_ = cs.CoreV1().Pods(ns.Name).Delete(cleanupCtx, "healthtest-aggregator", metav1.DeleteOptions{})
		_ = cs.CoreV1().Services(ns.Name).Delete(cleanupCtx, "healthtest-aggregator", metav1.DeleteOptions{})
		_ = cs.CoreV1().Pods(ns.Name).Delete(cleanupCtx, "healthtest-client", metav1.DeleteOptions{})
		if lbDNS != "" {
			// CLB deletion is handled by cloud-provider-aws when the Service is deleted
			waitForLBDeletion(cleanupCtx, lbDNS)
		}
	})

	// Wait for deployment
	By("waiting for Deployment rollout")
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(ns.Name).Get(ctx, deployName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		framework.Logf("deployment ready replicas: %d/%d", d.Status.ReadyReplicas, replicas)
		return d.Status.ReadyReplicas >= replicas, nil
	})
	framework.ExpectNoError(err, "deployment rollout")
	setupTimes.T1 = time.Now()

	// Wait for CLB provisioning
	By("waiting for CLB provisioning")
	err = wait.PollUntilContextTimeout(ctx, 10*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		s, err := cs.CoreV1().Services(ns.Name).Get(ctx, svcName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if len(s.Status.LoadBalancer.Ingress) > 0 {
			lbDNS = s.Status.LoadBalancer.Ingress[0].Hostname
			return lbDNS != "", nil
		}
		return false, nil
	})
	framework.ExpectNoError(err, "CLB provisioning")
	setupTimes.T2 = time.Now()
	cfg.LBDNS = lbDNS

	// Discover CLB by DNS name
	By("discovering CLB by DNS name")
	elbClient, err := createAWSClientCLB(ctx)
	framework.ExpectNoError(err, "create CLB client")

	lbName, err := getCLBByDNSNameWithRetry(ctx, elbClient, lbDNS)
	framework.ExpectNoError(err, "find CLB")
	cfg.LBARN = lbName // CLB uses name, not ARN
	cfg.TGTargetType = "instance (CLB)"

	// Create CLB observer
	clbObserver = health.NewCLBObserver(elbClient, lbName, 1*time.Second)

	// Wait for all instances healthy
	By("waiting for ALL CLB instances to become healthy")
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		snap, pollErr := clbObserver.PollOnce(ctx)
		if pollErr != nil {
			return false, nil
		}
		total := snap.HealthyCount + snap.UnhealthyCount + snap.InitialCount
		allHealthy := total > 0 && snap.UnhealthyCount == 0 && snap.InitialCount == 0
		if time.Now().Second()%10 == 0 {
			var details []string
			for id, state := range snap.Targets {
				details = append(details, fmt.Sprintf("%s=%s", id, state))
			}
			framework.Logf("[clb-wait] healthy=%d unhealthy=%d initial=%d total=%d | %s",
				snap.HealthyCount, snap.UnhealthyCount, snap.InitialCount, total,
				strings.Join(details, ", "))
		}
		if allHealthy {
			framework.Logf("[clb-wait] all %d instances healthy", snap.HealthyCount)
		}
		return allHealthy, nil
	})
	framework.ExpectNoError(err, "all CLB instances healthy")
	setupTimes.T3 = time.Now()

	// Deploy in-cluster client
	By("deploying in-cluster client on worker node")
	clientPodName = deployInClusterClient(ctx, cs, ns.Name, image, lbDNS, aggregatorURL)

	return lbDNS, clbObserver, cfg, setupTimes, clientPodName
}

// buildHealthTransitionServiceCLB creates a Service for a Classic Load Balancer.
// CLB is the default when no aws-load-balancer-type annotation is set.
// HC annotations are set to match the NLB test for fair comparison:
// HTTP /readyz on port 19443, interval=10s, threshold=2/2.
func buildHealthTransitionServiceCLB(namespace, name, deployName string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				// NO aws-load-balancer-type annotation = CLB (default)
				"service.beta.kubernetes.io/aws-load-balancer-target-node-labels":              "node-role.kubernetes.io/control-plane=",
				"service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled": "true",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-protocol":            "HTTP",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-path":                "/readyz",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-port":                fmt.Sprintf("%d", healthserverPort),
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-interval":            "10",
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-healthy-threshold":   "2",
				// CLB default unhealthy threshold is 6 — set to 2 for fair comparison with NLB
				"service.beta.kubernetes.io/aws-load-balancer-healthcheck-unhealthy-threshold": "2",
			},
		},
		Spec: v1.ServiceSpec{
			Type:                  v1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyLocal,
			Selector:              map[string]string{"app": deployName},
			Ports: []v1.ServicePort{{
				Name:       "http",
				Protocol:   v1.ProtocolTCP,
				Port:       int32(healthserverPort),
				TargetPort: intstr.FromInt(healthserverPort),
			}},
		},
	}
}

// waitForCLBUnhealthy blocks until at least one CLB instance reports OutOfService.
func waitForCLBUnhealthy(ctx context.Context, observer *health.CLBObserver, timeout time.Duration) {
	_ = wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		snap, err := observer.PollOnce(ctx)
		if err != nil {
			return false, nil
		}
		if snap.UnhealthyCount > 0 {
			framework.Logf("[clb-wait] detected %d unhealthy instance(s)", snap.UnhealthyCount)
			return true, nil
		}
		return false, nil
	})
}

// startCLBSnapshotPusher pushes CLB health snapshots to the aggregator every 2s.
func startCLBSnapshotPusher(ctx context.Context, cs clientset.Interface, namespace string, observer *health.CLBObserver) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap, err := observer.PollOnce(ctx)
				if err != nil {
					continue
				}
				pushTGSnapshotToAggregator(ctx, cs, namespace, snap)
			}
		}
	}()
	return cancel
}

func ptrBool(b bool) *bool    { return &b }
func ptrInt64(i int64) *int64 { return &i }
