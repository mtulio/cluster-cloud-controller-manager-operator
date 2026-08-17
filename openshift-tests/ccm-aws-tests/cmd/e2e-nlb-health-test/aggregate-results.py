#!/usr/bin/env python3
"""Aggregate NLB health-transition e2e stdout into summary tables and JSON.

Reads raw test logs from nlb-cases-res/*.txt, extracts the HEALTH TRANSITION
REPORT block from each file, and produces:
  - stdout summary table (drain × cluster variant), or per-run table (--full)
  - optional Markdown report (--markdown PATH)
  - JSON export (--json PATH)

Filter a single case/plan batch with --prefix, e.g.:
  python3 aggregate-results.py nlb-cases-res --prefix nlb-case12-plan26

Per-run counters (no aggregation) with --full:
  python3 aggregate-results.py nlb-cases-res --prefix nlb-case12-plan26 --full

Supports report formats:
  - legacy: SERVICE CONFIGURATION / TARGET GROUP CONFIGURATION (svc/* metadata)
  - current: E2E TEST METADATA + LOAD BALANCER CONFIGURATION (AWS API)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

REPORT_MARKER = "HEALTH TRANSITION REPORT"
REPORT_END_MARKERS = (
    "\n\n  I0",  # glog line after report (typical OTE stdout)
    "\n  I0",
)

FILENAME_CASE_RE = re.compile(r"^nlb-case([\d.]+)")
FILENAME_PLAN_RE = re.compile(r"plan[_-]?v?(\d+)", re.I)
FILENAME_DRAIN_RE = re.compile(r"plan(?:[_-]v?\d+-)?(\d+s)", re.I)
FILENAME_DRAIN_FALLBACK_RE = re.compile(r"-(\d+s)(?:_|\.txt)")
FILENAME_ITER_RE = re.compile(r"_v(\d+)\.txt$")
BATCH_PREFIX_RE = re.compile(r"nlb-case(?P<case>[\d.]+)-plan[_-]?v?(?P<plan>\d+)", re.I)

VARIANT_ALIASES = {
    "use1": "us-east-1 (all AZ)",
    "usw1": "us-west-1 (2 AZ)",
    "euw1": "eu-west-1 (2 AZ)",
    "2az": "us-east-1 (2 AZ)",
}

# A lone pre-readyz or [RESTART] count is a ctl-restart race artifact, not OCPBUGS-86789.
SPURIOUS_PRE_READYZ_COUNT = 1
SPURIOUS_RESTART_UNHEALTHY_COUNT = 1


@dataclass
class RunResult:
    filename: str
    case: str | None = None
    plan: int | None = None
    drain: str | None = None
    variant: str = "2az"
    iteration: int | None = None
    scenario: str | None = None
    platform: str | None = None
    region: str | None = None
    topology: str | None = None
    drain_observe: str | None = None
    restart_mode: str | None = None
    cross_zone_lb: str | None = None
    conn_termination: str | None = None
    draining_interval: str | None = None
    preserve_client_ip: str | None = None
    pre_readyz: int = 0
    pre_readyz_effective: int = 0
    pre_readyz_spurious: bool = False
    restart_unhealthy_reqs: int | None = None
    restart_unhealthy_effective: int = 0
    restart_unhealthy_spurious: bool = False
    unhealthy_reqs: int = 0
    late_conn_reqs: int = 0
    total_reqs: int | None = None
    errors: int | None = None
    avg_rate: str | None = None
    t_route_stop: str | None = None
    t_route_stop_sec: float | None = None
    t_container_restart: str | None = None
    t_container_restart_sec: float | None = None
    t_route_start: str | None = None
    t_route_start_sec: float | None = None
    t_tg_unhealthy: str | None = None
    t_tg_unhealthy_sec: float | None = None
    t_tg_healthy: str | None = None
    t_tg_healthy_sec: float | None = None
    t_total_cycle: str | None = None
    t_total_cycle_sec: float | None = None
    t_downtime_window: str | None = None
    t_downtime_window_sec: float | None = None
    verdict_bug: str | None = None
    reproduced: bool = False
    tg_states: list[str] = field(default_factory=list)
    t_tcp_up_from_t5_sec: float | None = None
    overlap_est_tcp_up_sec: float | None = None
    overlap_with_propagation: bool | None = None
    parse_errors: list[str] = field(default_factory=list)

    def group_key(self) -> tuple[str, str, str | None]:
        """Group by plan label, drain, cluster variant."""
        plan_label = f"v{self.plan}" if self.plan else "?"
        return (plan_label, self.drain or "?", self.variant)


def parse_duration(raw: str | None) -> float | None:
    """Parse '1m28.28s', '54.784s', or 'N/A' to seconds."""
    if not raw or raw.upper() == "N/A":
        return None
    total = 0.0
    matched = False
    for val, unit in re.findall(r"([\d.]+)(ms|h|m|s)", raw):
        matched = True
        v = float(val)
        if unit == "h":
            total += v * 3600
        elif unit == "m":
            total += v * 60
        elif unit == "ms":
            total += v / 1000.0
        else:
            total += v
    return round(total, 3) if matched else None


def sec_stats(values: list[float]) -> tuple[float, float, float]:
    if not values:
        return 0.0, 0.0, 0.0
    return sum(values) / len(values), min(values), max(values)


def effective_pre_readyz(raw: int) -> int:
    """Ignore a lone pre-readyz response (ctl in-place restart race)."""
    if raw == SPURIOUS_PRE_READYZ_COUNT:
        return 0
    return raw


def effective_restart_unhealthy(raw: int | None, pre_readyz_raw: int) -> int:
    """Ignore a lone [RESTART] unhealthy count when pre-readyz is also absent/spurious."""
    if raw is None:
        return 0
    if (
        raw == SPURIOUS_RESTART_UNHEALTHY_COUNT
        and pre_readyz_raw <= SPURIOUS_PRE_READYZ_COUNT
    ):
        return 0
    return raw


def extract_restart_unhealthy(report: str) -> int | None:
    m = re.search(
        r"\[RESTART\] Target node received (\d+) unhealthy/pre-readyz request",
        report,
    )
    return int(m.group(1)) if m else None


def extract_downtime_window(report: str) -> tuple[str | None, float | None]:
    """Return t8−t5 downtime (readyz 503 → readyz 200) from timeline delta."""
    m = re.search(r"t8\s+readyz→200\s+\[\+(\S+)\]", report)
    if m:
        raw = m.group(1)
        return raw, parse_duration(raw)
    return None, None


def parse_drain_seconds(drain: str | None) -> float | None:
    if not drain:
        return None
    m = re.match(r"(\d+)s?", drain)
    return float(m.group(1)) if m else None


CASE_DEFAULT_PLAN = {
    "9": 23,
    "9.1": 23,
    "9.2": 23,
    "9.3": 23,
    "9.4": 23,
    "10": 25,
    "10.1": 25,
    "10.2": 25,
    "10.3": 25,
    "11": 25,
    "12": 26,
}


def parse_filename(name: str) -> dict[str, Any]:
    meta: dict[str, Any] = {"filename": name}

    m = FILENAME_CASE_RE.match(name)
    case = m.group(1) if m else None
    meta["case"] = case

    m = FILENAME_PLAN_RE.search(name)
    if m:
        meta["plan"] = int(m.group(1))
    elif case and case in CASE_DEFAULT_PLAN:
        meta["plan"] = CASE_DEFAULT_PLAN[case]
    elif case and case.split(".")[0] in CASE_DEFAULT_PLAN:
        meta["plan"] = CASE_DEFAULT_PLAN[case.split(".")[0]]
    else:
        meta["plan"] = None

    m = FILENAME_DRAIN_RE.search(name) or FILENAME_DRAIN_FALLBACK_RE.search(name)
    meta["drain"] = m.group(1) if m else None

    if "_use1_" in name or name.endswith("_use1.txt"):
        meta["variant"] = "use1"
    elif "_usw1_" in name or name.endswith("_usw1.txt"):
        meta["variant"] = "usw1"
    elif "_euw1_" in name or name.endswith("_euw1.txt"):
        meta["variant"] = "euw1"
    else:
        meta["variant"] = "2az"

    m = FILENAME_ITER_RE.search(name)
    meta["iteration"] = int(m.group(1)) if m else None
    return meta


def normalize_prefix(prefix: str) -> str:
    """Strip wildcards/path; return filename glob stem (e.g. nlb-case12-plan26)."""
    p = prefix.strip().replace("*", "")
    if not p:
        return ""
    return Path(p).name


def parse_batch_prefix(prefix: str) -> dict[str, Any]:
    """Parse case/plan from a batch prefix like nlb-case12-plan26."""
    slug = normalize_prefix(prefix)
    info: dict[str, Any] = {"batch_id": slug, "case": None, "plan": None}
    m = BATCH_PREFIX_RE.search(slug)
    if m:
        info["case"] = m.group("case")
        info["plan"] = int(m.group("plan"))
    return info


def resolve_inputs(
    results_dir: Path,
    prefix: str | None,
) -> tuple[Path, str | None]:
    """Return (directory, normalized prefix stem or None)."""
    if not prefix:
        return results_dir, None

    raw = prefix.strip()
    path = Path(raw)
    if path.parent != Path(".") and str(path.parent) not in (".", ""):
        return path.parent, normalize_prefix(path.name)

    return results_dir, normalize_prefix(raw)


def default_output_paths(results_dir: Path, prefix: str | None) -> tuple[Path, Path]:
    if prefix:
        slug = normalize_prefix(prefix)
        return (
            results_dir / f"{slug}-summary.md",
            results_dir / f"{slug}-summary.json",
        )
    return (
        results_dir / "nlb-tests-summary.md",
        results_dir / "nlb-results-summary.json",
    )


def strip_test_framework_json(text: str) -> str:
    """Remove trailing OTE JSON blob that embeds a duplicate report string."""
    for marker in ("\n[\n  {", "\n[{"):
        idx = text.rfind(marker)
        if idx != -1:
            return text[:idx]
    return text


def extract_report_block(text: str) -> str:
    """Return the last HEALTH TRANSITION REPORT section from mixed stdout."""
    text = strip_test_framework_json(text)
    idx = text.rfind(REPORT_MARKER)
    if idx == -1:
        return ""

    # Walk back to the report banner line if marker appears mid-line.
    line_start = text.rfind("\n", 0, idx)
    if line_start == -1:
        line_start = 0
    else:
        line_start += 1

    chunk = text[line_start:]
    verdict_idx = chunk.find("\n  VERDICT")
    if verdict_idx != -1:
        rest = chunk[verdict_idx:]
        info = re.search(r"\[INFO\][^\n]*\n", rest)
        if info:
            chunk = chunk[: verdict_idx + info.end()]
        else:
            closing = chunk.find("\n  ═", verdict_idx)
            if closing != -1:
                chunk = chunk[: closing + 1]
    else:
        for marker in REPORT_END_MARKERS:
            end = chunk.find(marker)
            if end != -1:
                chunk = chunk[:end]
                break
    return chunk


def extract_kv(report: str, key: str) -> str | None:
    """Extract 'key: value' from report (AWS API or metadata lines)."""
    pat = rf"^\s*{re.escape(key)}:\s+(.+?)\s*$"
    m = re.search(pat, report, re.MULTILINE)
    return m.group(1).strip() if m else None


def extract_timing_metric(report: str, name: str) -> str | None:
    m = re.search(rf"^\s*{re.escape(name)}\s+(\S+)", report, re.MULTILINE)
    return m.group(1) if m else None


def parse_report(text: str, filename: str) -> RunResult | None:
    report = extract_report_block(text)
    if not report:
        return None

    meta = parse_filename(filename)
    run = RunResult(
        filename=filename,
        case=meta.get("case"),
        plan=meta.get("plan"),
        drain=meta.get("drain"),
        variant=meta.get("variant", "2az"),
        iteration=meta.get("iteration"),
    )

    m = re.search(r"HEALTH TRANSITION REPORT — Scenario (.+)", report)
    if m:
        run.scenario = m.group(1).strip()

    run.platform = extract_kv(report, "Platform")
    run.region = extract_kv(report, "Region")
    run.topology = extract_kv(report, "Topology")

    run.drain_observe = (
        extract_kv(report, "e2e/shutdown-drain-observe")
        or extract_kv(report, "svc/shutdown-drain-observe")
        or run.drain
    )
    run.restart_mode = extract_kv(report, "e2e/restart-mode") or extract_kv(
        report, "svc/restart-mode"
    )

    # AWS API — prefer DescribeLoadBalancerAttributes, fall back to TG attrs (legacy).
    run.cross_zone_lb = extract_kv(report, "load_balancing.cross_zone.enabled")
    # In new reports cross_zone appears twice; LB attrs section has the effective value.
    lb_attr_match = re.search(
        r"--- DescribeLoadBalancerAttributes ---\s*\n"
        r"(?:\s+.+\n)*?"
        r"\s+load_balancing\.cross_zone\.enabled:\s+(\S+)",
        report,
    )
    if lb_attr_match:
        run.cross_zone_lb = lb_attr_match.group(1)

    run.conn_termination = extract_kv(
        report, "target_health_state.unhealthy.connection_termination.enabled"
    )
    run.draining_interval = extract_kv(
        report, "target_health_state.unhealthy.draining_interval_seconds"
    )
    run.preserve_client_ip = extract_kv(report, "preserve_client_ip.enabled")

    timing_fields = [
        ("t_route_stop", "T_route_stop"),
        ("t_container_restart", "T_container_restart"),
        ("t_route_start", "T_route_start"),
        ("t_tg_unhealthy", "T_tg_unhealthy"),
        ("t_tg_healthy", "T_tg_healthy"),
        ("t_total_cycle", "T_total_cycle"),
    ]
    for attr, metric in timing_fields:
        raw = extract_timing_metric(report, metric)
        setattr(run, attr, raw)
        setattr(run, attr + "_sec", parse_duration(raw))

    run.pre_readyz = int(extract_timing_metric(report, "Pre_readyz_reqs") or 0)
    run.restart_unhealthy_reqs = extract_restart_unhealthy(report)
    run.pre_readyz_effective = effective_pre_readyz(run.pre_readyz)
    run.restart_unhealthy_effective = effective_restart_unhealthy(
        run.restart_unhealthy_reqs, run.pre_readyz
    )
    run.pre_readyz_spurious = (
        run.pre_readyz == SPURIOUS_PRE_READYZ_COUNT and run.pre_readyz_effective == 0
    )
    run.restart_unhealthy_spurious = (
        run.restart_unhealthy_reqs == SPURIOUS_RESTART_UNHEALTHY_COUNT
        and run.restart_unhealthy_effective == 0
    )
    run.unhealthy_reqs = int(extract_timing_metric(report, "Unhealthy_reqs") or 0)
    run.late_conn_reqs = int(extract_timing_metric(report, "Late_conn_reqs") or 0)

    m = re.search(r"^\s*Total:\s+(\d+)", report, re.MULTILINE)
    if m:
        run.total_reqs = int(m.group(1))
    m = re.search(r"^\s*Errors:\s+(\d+)", report, re.MULTILINE)
    if m:
        run.errors = int(m.group(1))
    m = re.search(r"^\s*Avg rate:\s+(\S+)", report, re.MULTILINE)
    if m:
        run.avg_rate = m.group(1)

    m = re.search(r"\[BUG\]\s+(.+)", report)
    if m:
        run.verdict_bug = m.group(1).strip()

    run.tg_states = re.findall(r"TG\s+([\w.]+→[\w.]+)\s+target=", report)
    run.reproduced = run.pre_readyz_effective > 0

    run.t_downtime_window, run.t_downtime_window_sec = extract_downtime_window(report)

    m = re.search(r"t7\.1 ctl restart[^\n]*\[\+(\S+)\]", report)
    t71_from_t5 = parse_duration(m.group(1)) if m else None
    m = re.search(r"t7\.3 TCP up\s+\[\+(\S+)\]", report)
    t73_from_t71 = parse_duration(m.group(1)) if m else None
    if t71_from_t5 is not None:
        run.t_tcp_up_from_t5_sec = round(
            t71_from_t5 + (t73_from_t71 or 0.0), 3
        )

    drain_sec = parse_drain_seconds(run.drain_observe)
    if run.t_tg_unhealthy_sec is not None and drain_sec is not None:
        restart_sec = run.t_container_restart_sec or 0.0
        run.overlap_est_tcp_up_sec = round(
            run.t_tg_unhealthy_sec + drain_sec + restart_sec, 3
        )

    tcp_up = run.t_tcp_up_from_t5_sec or run.overlap_est_tcp_up_sec
    if run.t_route_stop_sec is not None and tcp_up is not None:
        run.overlap_with_propagation = run.t_route_stop_sec >= tcp_up

    if not run.drain and run.drain_observe:
        run.drain = run.drain_observe
    if run.t_route_stop is None:
        run.parse_errors.append("missing T_route_stop")
    if run.t_total_cycle is None:
        run.parse_errors.append("missing T_total_cycle")
    if run.t_downtime_window_sec is None:
        run.parse_errors.append("missing downtime window (t8−t5)")
    if run.cross_zone_lb is None:
        run.parse_errors.append("missing cross_zone attribute")

    return run


def load_results(results_dir: Path, prefix: str | None = None) -> list[RunResult]:
    runs: list[RunResult] = []
    glob_pattern = f"{normalize_prefix(prefix)}*.txt" if prefix else "*.txt"
    paths = sorted(results_dir.glob(glob_pattern))
    if prefix and not paths:
        print(
            f"WARNING: no files match {results_dir / glob_pattern}",
            file=sys.stderr,
        )
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            print(f"WARNING: skip {path.name}: {exc}", file=sys.stderr)
            continue
        if REPORT_MARKER not in text:
            continue
        run = parse_report(text, path.name)
        if run:
            runs.append(run)
    return runs


def aggregate(runs: list[RunResult]) -> dict[tuple[str, str, str], list[RunResult]]:
    groups: dict[tuple[str, str, str], list[RunResult]] = defaultdict(list)
    for run in runs:
        groups[run.group_key()].append(run)
    return groups


def drain_sort_key(drain: str) -> float:
    sec = parse_drain_seconds(drain)
    return sec if sec is not None else 9999.0


def run_sort_key(run: RunResult) -> tuple[Any, ...]:
    return (run.plan or 0, drain_sort_key(run.drain or ""), run.variant, run.iteration or 0, run.filename)


def run_id(run: RunResult) -> str:
    """Stable per-run identifier (log filename without extension)."""
    return Path(run.filename).stem


def fmt_sec(sec: float | None) -> str:
    if sec is None:
        return "?"
    return f"{sec:.1f}s"


def fmt_bool(value: bool | None) -> str:
    if value is None:
        return "?"
    return "yes" if value else "no"


def print_full_table(
    runs: list[RunResult],
    *,
    batch: dict[str, Any] | None = None,
) -> None:
    """Print one row per run with individual timers/counters (no aggregation)."""
    width = 150
    print("=" * width)
    if batch and batch.get("batch_id"):
        case = batch.get("case") or "?"
        plan = batch.get("plan")
        plan_label = f"v{plan}" if plan else "?"
        print(
            f"NLB HEALTH TRANSITION — PER-RUN RESULTS "
            f"(batch {batch['batch_id']}, case {case}, plan {plan_label})"
        )
    else:
        print("NLB HEALTH TRANSITION — PER-RUN RESULTS")
    print("=" * width)
    if batch and batch.get("batch_id"):
        print(f"Prefix filter: {batch['batch_id']}*")
    print(f"Runs: {len(runs)}")
    print()
    header = (
        f"{'#':>4} {'Run ID':<40} {'Plan':>4} {'Drain':>6} {'Cluster':>20} "
        f"{'Repro':>5} {'PreRdz':>6} {'T_stop':>9} {'T_cycle':>10} {'Down':>9} "
        f"{'T_start':>9} {'Unhlth':>7} {'Ovlap':>5}"
    )
    print(header)
    print("-" * len(header))

    for idx, run in enumerate(sorted(runs, key=run_sort_key), start=1):
        plan = f"v{run.plan}" if run.plan else "?"
        cluster = VARIANT_ALIASES.get(run.variant, run.variant)
        repro = "REPRO" if run.reproduced else "no"
        print(
            f"{idx:>4} {run_id(run):<40} {plan:>4} {run.drain or '?':>6} {cluster:>20} "
            f"{repro:>5} {run.pre_readyz_effective:>6} "
            f"{fmt_sec(run.t_route_stop_sec):>9} {fmt_sec(run.t_total_cycle_sec):>10} "
            f"{fmt_sec(run.t_downtime_window_sec):>9} {fmt_sec(run.t_route_start_sec):>9} "
            f"{run.unhealthy_reqs:>7} {fmt_bool(run.overlap_with_propagation):>5}"
        )

    print()
    print(
        "KEY: # = row index; Run ID = log stem (test/run id); Repro = effective Pre_readyz > 0 "
        "(raw count of 1 ignored as spurious); T_stop = T_route_stop; T_cycle = T_total_cycle "
        "(t10−t5); Down = downtime window (t8−t5); T_start = T_route_start; Unhlth = Unhealthy_reqs; "
        "Ovlap = T_route_stop >= est. TCP-up from timeline."
    )


def print_summary_table(
    groups: dict[tuple[str, str, str], list[RunResult]],
    *,
    batch: dict[str, Any] | None = None,
) -> None:
    print("=" * 120)
    if batch and batch.get("batch_id"):
        case = batch.get("case") or "?"
        plan = batch.get("plan")
        plan_label = f"v{plan}" if plan else "?"
        print(
            f"NLB HEALTH TRANSITION — BATCH {batch['batch_id']} "
            f"(case {case}, plan {plan_label})"
        )
    else:
        print("NLB HEALTH TRANSITION — AGGREGATED RESULTS")
    print("=" * 120)
    if batch and batch.get("batch_id"):
        print(f"Prefix filter: {batch['batch_id']}*")
    print()
    header = (
        f"{'Plan':>6} {'Drain':>8} {'Cluster':>22} {'Runs':>4} {'Repro':>5} "
        f"{'Rate':>6} {'PreRdz_avg':>10} {'PreRdz_max':>10} "
        f"{'T_stop_avg':>10} {'T_cycle_avg':>11} {'Down_avg':>10}"
    )
    print(header)
    print("-" * len(header))

    for (plan, drain, variant), group_runs in sorted(
        groups.items(),
        key=lambda item: (item[0][0], drain_sort_key(item[0][1]), item[0][2]),
    ):
        n = len(group_runs)
        repro = sum(1 for r in group_runs if r.reproduced)
        rate = f"{repro / n * 100:.0f}%" if n else "n/a"
        pre_vals = [r.pre_readyz_effective for r in group_runs]
        stop_vals = [r.t_route_stop_sec for r in group_runs if r.t_route_stop_sec is not None]
        cycle_vals = [r.t_total_cycle_sec for r in group_runs if r.t_total_cycle_sec is not None]
        down_vals = [
            r.t_downtime_window_sec for r in group_runs if r.t_downtime_window_sec is not None
        ]
        pre_avg = sum(pre_vals) / n
        pre_max = max(pre_vals)
        stop_avg, _, _ = sec_stats(stop_vals)
        cycle_avg, _, _ = sec_stats(cycle_vals)
        down_avg, _, _ = sec_stats(down_vals)

        overlaps = [r.overlap_with_propagation for r in group_runs if r.overlap_with_propagation is not None]
        if overlaps:
            overlap_pct = f"{sum(overlaps) / len(overlaps) * 100:.0f}%"
        else:
            overlap_pct = "?"

        cluster = VARIANT_ALIASES.get(variant, variant)
        print(
            f"{plan:>6} {drain:>8} {cluster:>22} {n:>4} {repro:>5} "
            f"{rate:>6} {pre_avg:>10.0f} {pre_max:>10} "
            f"{stop_avg:>9.1f}s {cycle_avg:>10.1f}s {down_avg:>9.1f}s "
        )

    print()
    print(
        "KEY: Repro = effective Pre_readyz > 0 (raw count of 1 ignored as spurious); Rate = reproduction rate; "
        "T_stop = T_route_stop; T_cycle = T_total_cycle (t10−t5); "
        "Down = downtime window (t8−t5, readyz 503 → readyz 200); "
        "Overlap = fraction of runs where T_route_stop >= est. TCP-up from timeline "
        "(or T_tg_unhealthy + drain + T_container_restart). Note: repro can still "
        "occur via unhealthy.draining after T_route_stop."
    )


def write_markdown_report(
    runs: list[RunResult],
    groups: dict[tuple[str, str, str], list[RunResult]],
    path: Path,
    *,
    batch: dict[str, Any] | None = None,
    source_glob: str | None = None,
) -> None:
    title = "# NLB Health Transition — Aggregated Results"
    if batch and batch.get("batch_id"):
        case = batch.get("case") or "?"
        plan = batch.get("plan")
        plan_label = f"v{plan}" if plan else "?"
        title = (
            f"# NLB Health Transition — Batch `{batch['batch_id']}` "
            f"(case {case}, plan {plan_label})"
        )

    source_desc = source_glob or f"{path.parent.name}/*.txt"
    lines: list[str] = [
        title,
        "",
        f"Generated from `{len(runs)}` report(s) matching `{source_desc}`.",
        "",
        "## Summary matrix",
        "",
        "Grouped by **drain** × **cluster variant** within the batch.",
        "",
        "| Plan | Drain | Cluster | Runs | Repro | Rate | PreRdz avg | PreRdz max | "
        "T_stop avg | T_cycle avg | Down avg | Cross-zone |",
        "|------|-------|---------|------|-------|------|------------|------------|"
        "------------|-------------|----------|------------|",
    ]

    for (plan, drain, variant), group_runs in sorted(
        groups.items(),
        key=lambda item: (item[0][0], drain_sort_key(item[0][1]), item[0][2]),
    ):
        n = len(group_runs)
        repro = sum(1 for r in group_runs if r.reproduced)
        rate = f"{repro / n * 100:.0f}%" if n else "n/a"
        pre_avg = sum(r.pre_readyz_effective for r in group_runs) / n
        pre_max = max(r.pre_readyz_effective for r in group_runs)
        stop_vals = [r.t_route_stop_sec for r in group_runs if r.t_route_stop_sec is not None]
        cycle_vals = [r.t_total_cycle_sec for r in group_runs if r.t_total_cycle_sec is not None]
        down_vals = [
            r.t_downtime_window_sec for r in group_runs if r.t_downtime_window_sec is not None
        ]
        stop_avg, _, _ = sec_stats(stop_vals)
        cycle_avg, _, _ = sec_stats(cycle_vals)
        down_avg, _, _ = sec_stats(down_vals)
        cross_zones = {r.cross_zone_lb for r in group_runs if r.cross_zone_lb}
        cross = ", ".join(sorted(cross_zones)) if cross_zones else "?"
        overlaps = [r.overlap_with_propagation for r in group_runs if r.overlap_with_propagation is not None]
        overlap_pct = f"{sum(overlaps) / len(overlaps) * 100:.0f}%" if overlaps else "?"
        cluster = VARIANT_ALIASES.get(variant, variant)
        lines.append(
            f"| {plan} | {drain} | {cluster} | {n} | {repro} | {rate} | "
            f"{pre_avg:.0f} | {pre_max} | {stop_avg:.1f}s | {cycle_avg:.1f}s | "
            f"{down_avg:.1f}s | {cross} |"
        )

    lines.extend(["", "## Per-run details", ""])
    for run in sorted(runs, key=run_sort_key):
        cluster = VARIANT_ALIASES.get(run.variant, run.variant)
        repro = "REPRO" if run.reproduced else "no repro"
        lines.append(f"### `{run.filename}` — {repro}")
        lines.append("")
        if run.scenario:
            lines.append(f"- **Scenario:** {run.scenario}")
        lines.append(f"- **Plan / drain / cluster / iter:** v{run.plan} / {run.drain} / {cluster} / v{run.iteration}")
        lines.append(f"- **Region / topology:** {run.region} / {run.topology}")
        pre_line = f"- **Pre_readyz:** {run.pre_readyz_effective}"
        if run.pre_readyz != run.pre_readyz_effective:
            pre_line += f" (raw {run.pre_readyz}, spurious single-request artifact)"
        pre_line += (
            f" | **T_route_stop:** {run.t_route_stop} "
            f"| **Unhealthy_reqs:** {run.unhealthy_reqs}"
        )
        lines.append(pre_line)
        if run.restart_unhealthy_reqs is not None:
            restart_line = f"- **[RESTART] unhealthy reqs:** {run.restart_unhealthy_effective}"
            if run.restart_unhealthy_reqs != run.restart_unhealthy_effective:
                restart_line += (
                    f" (raw {run.restart_unhealthy_reqs}, spurious single-request artifact)"
                )
            lines.append(restart_line)
        lines.append(
            f"- **T_total_cycle:** {run.t_total_cycle} "
            f"| **Downtime window (t8−t5):** {run.t_downtime_window}"
        )
        lines.append(
            f"- **Cross-zone (LB):** {run.cross_zone_lb} | **conn_term:** {run.conn_termination} "
            f"| **draining:** {run.draining_interval}s | **preserve_client_ip:** {run.preserve_client_ip}"
        )
        if run.overlap_est_tcp_up_sec is not None:
            lines.append(
                f"- **Est. TCP-up after t5:** ~{run.overlap_est_tcp_up_sec}s | "
                f"**Overlap with propagation:** {run.overlap_with_propagation}"
            )
        if run.verdict_bug:
            lines.append(f"- **Verdict:** {run.verdict_bug}")
        if run.tg_states:
            lines.append(f"- **TG transitions:** {', '.join(run.tg_states)}")
        if run.parse_errors:
            lines.append(f"- **Parse warnings:** {', '.join(run.parse_errors)}")
        lines.append("")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def export_json(
    runs: list[RunResult],
    groups: dict[tuple[str, str, str], list[RunResult]],
    path: Path,
    *,
    batch: dict[str, Any] | None = None,
    source_glob: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "run_count": len(runs),
        "groups": {},
        "runs": [asdict(r) for r in runs],
    }
    if batch:
        payload["batch"] = batch
    if source_glob:
        payload["source_glob"] = source_glob
    for key, group_runs in groups.items():
        plan, drain, variant = key
        label = f"{plan}_{drain}_{variant}"
        payload["groups"][label] = {
            "plan": plan,
            "drain": drain,
            "variant": variant,
            "cluster": VARIANT_ALIASES.get(variant, variant),
            "runs": len(group_runs),
            "reproduced": sum(1 for r in group_runs if r.reproduced),
            "pre_readyz_values": [r.pre_readyz for r in group_runs],
            "pre_readyz_effective_values": [r.pre_readyz_effective for r in group_runs],
            "restart_unhealthy_values": [r.restart_unhealthy_reqs for r in group_runs],
            "restart_unhealthy_effective_values": [
                r.restart_unhealthy_effective for r in group_runs
            ],
            "t_route_stop_sec_values": [
                r.t_route_stop_sec for r in group_runs if r.t_route_stop_sec is not None
            ],
            "t_total_cycle_sec_values": [
                r.t_total_cycle_sec for r in group_runs if r.t_total_cycle_sec is not None
            ],
            "t_total_cycle_sec_avg": sec_stats(
                [r.t_total_cycle_sec for r in group_runs if r.t_total_cycle_sec is not None]
            )[0],
            "t_downtime_window_sec_values": [
                r.t_downtime_window_sec
                for r in group_runs
                if r.t_downtime_window_sec is not None
            ],
            "t_downtime_window_sec_avg": sec_stats(
                [
                    r.t_downtime_window_sec
                    for r in group_runs
                    if r.t_downtime_window_sec is not None
                ]
            )[0],
            "cross_zone_values": [r.cross_zone_lb for r in group_runs],
            "overlap_values": [
                r.overlap_with_propagation
                for r in group_runs
                if r.overlap_with_propagation is not None
            ],
            "filenames": [r.filename for r in group_runs],
        }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results_dir",
        nargs="?",
        default="nlb-cases-res",
        help="Directory containing nlb-case*.txt logs (default: nlb-cases-res)",
    )
    parser.add_argument(
        "--prefix",
        metavar="STEM",
        help=(
            "Only include logs whose filename starts with STEM "
            "(e.g. nlb-case12-plan26 or nlb-cases-res/nlb-case11-plan_v25). "
            "Default output: {STEM}-summary.md / .json in results_dir."
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "Print per-run table with individual timers/counters (Run ID index) "
            "instead of drain × cluster aggregated averages"
        ),
    )
    parser.add_argument(
        "--json",
        default=None,
        help="Write JSON summary (default: batch-specific or nlb-results-summary.json)",
    )
    parser.add_argument(
        "--markdown",
        default=None,
        help="Write Markdown report (default: batch-specific or nlb-tests-summary.md)",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip JSON export",
    )
    parser.add_argument(
        "--no-markdown",
        action="store_true",
        help="Skip Markdown export",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.is_dir():
        print(f"ERROR: not a directory: {results_dir}", file=sys.stderr)
        return 1

    results_dir, prefix = resolve_inputs(results_dir, args.prefix)
    if not results_dir.is_dir():
        print(f"ERROR: not a directory: {results_dir}", file=sys.stderr)
        return 1

    batch = parse_batch_prefix(prefix) if prefix else None
    source_glob = f"{prefix}*.txt" if prefix else "*.txt"

    runs = load_results(results_dir, prefix)
    if not runs:
        target = results_dir / source_glob
        print(f"No HEALTH TRANSITION REPORT found matching {target}", file=sys.stderr)
        return 1

    groups = aggregate(runs)
    if args.full:
        print_full_table(runs, batch=batch)
    else:
        print_summary_table(groups, batch=batch)

    default_md, default_json = default_output_paths(results_dir, prefix)
    md_path = Path(args.markdown) if args.markdown else default_md
    json_path = Path(args.json) if args.json else default_json

    if not args.no_json:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        export_json(runs, groups, json_path, batch=batch, source_glob=source_glob)
        print(f"\nJSON exported to {json_path}")

    if not args.no_markdown:
        md_path.parent.mkdir(parents=True, exist_ok=True)
        write_markdown_report(
            runs, groups, md_path, batch=batch, source_glob=source_glob
        )
        print(f"Markdown report written to {md_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
