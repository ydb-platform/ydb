#!/usr/bin/env python3
"""Check bounded keeper-heartbeat/disconnect routing, including queued bridge proposals and broken controls."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spin_runner import classify_search, execute


CASES = [
    ('queued_guard_preserves_publication', 'queued_old_quorum.pml', {}, 'no_same_generation_fork', 'pass'),
    ('queued_guard_converges_after_resume', 'queued_old_quorum.pml', {}, 'newest_after_resume', 'pass'),
    ('queued_old_completion_reachable', 'queued_old_quorum.pml', {}, 'reach_old_queued_completion', 'witness'),
    (
        'queued_negative_reused_generation', 'queued_old_quorum.pml',
        {'USE_OBSERVED_FLOOR': 0}, 'no_same_generation_fork', 'assertion',
    ),
    ('queued_partial_reads_preserve_floor', 'queued_old_quorum.pml', {}, 'partial_floor_preserved', 'pass'),
    ('queued_mutation_requires_fresh_read', 'queued_old_quorum.pml', {}, 'mutation_requires_fresh_read', 'pass'),
    ('queued_partial_proposal_visible', 'queued_old_quorum.pml', {}, 'reach_partial_proposal_seen', 'witness'),
    ('queued_partial_proposal_hidden', 'queued_old_quorum.pml', {}, 'reach_partial_proposal_unseen', 'witness'),
    ('queued_failed_read_blocks_mutation', 'queued_old_quorum.pml', {}, 'reach_partial_read_guard', 'witness'),
    (
        'queued_negative_discarded_partial_floor', 'queued_old_quorum.pml',
        {'RETAIN_PARTIAL_FLOOR': 0, 'FIRST_READ_MASK': 2}, 'partial_floor_preserved', 'assertion',
    ),
    (
        'queued_negative_lost_partial_floor_forks', 'queued_old_quorum.pml',
        {'RETAIN_PARTIAL_FLOOR': 0, 'REQUIRE_FRESH_GUARD': 0, 'FIRST_READ_MASK': 2},
        'no_same_generation_fork', 'assertion',
    ),
    (
        'queued_negative_unseen_proposal_forks', 'queued_old_quorum.pml',
        {'REQUIRE_FRESH_GUARD': 0, 'FIRST_READ_MASK': 4}, 'no_same_generation_fork', 'assertion',
    ),
    ('disconnect_three_intermediate_hung', 'silent_parent.pml', {}, 'eventual_success', 'pass'),
    ('disconnect_three_intermediate_converges', 'silent_parent.pml', {}, 'eventual_all_healthy_reached', 'pass'),
    ('disconnect_three_intermediate_rebinds', 'silent_parent.pml', {}, 'eventual_new_binding', 'pass'),
    ('disconnect_three_intermediate_terminates', 'silent_parent.pml', {}, 'request_terminates', 'pass'),
    ('disconnect_six_two_piles_intermediate_hung', 'silent_parent.pml', {'NODES': 6}, 'eventual_success', 'pass'),
    ('disconnect_six_two_piles_converges', 'silent_parent.pml', {'NODES': 6}, 'eventual_all_healthy_reached', 'pass'),
    ('disconnect_three_root_hung', 'silent_parent.pml', {'ROOT_HUNG': 1}, 'eventual_success', 'pass'),
    ('disconnect_three_root_converges', 'silent_parent.pml', {'ROOT_HUNG': 1}, 'eventual_all_healthy_reached', 'pass'),
    ('disconnect_six_root_hung', 'silent_parent.pml', {'ROOT_HUNG': 1, 'NODES': 6}, 'eventual_success', 'pass'),
    (
        'disconnect_six_root_converges', 'silent_parent.pml',
        {'ROOT_HUNG': 1, 'NODES': 6}, 'eventual_all_healthy_reached', 'pass',
    ),
    ('disconnect_three_false_suspicion', 'silent_parent.pml', {'FALSE_SUSPICION': 1}, 'eventual_success', 'pass'),
    (
        'disconnect_three_false_suspicion_converges', 'silent_parent.pml',
        {'FALSE_SUSPICION': 1}, 'eventual_all_healthy_reached', 'pass',
    ),
    (
        'disconnect_three_false_suspicion_rebinds', 'silent_parent.pml',
        {'FALSE_SUSPICION': 1}, 'eventual_new_binding', 'pass',
    ),
    (
        'disconnect_six_false_suspicion', 'silent_parent.pml',
        {'FALSE_SUSPICION': 1, 'NODES': 6}, 'eventual_success', 'pass',
    ),
    (
        'disconnect_six_false_suspicion_converges', 'silent_parent.pml',
        {'FALSE_SUSPICION': 1, 'NODES': 6}, 'eventual_all_healthy_reached', 'pass',
    ),
    (
        'disconnect_three_late_middle_recovery', 'silent_parent.pml',
        {'LATE_RECOVERY': 1}, 'eventual_all_healthy_reached', 'pass',
    ),
    (
        'disconnect_six_late_middle_recovery', 'silent_parent.pml',
        {'LATE_RECOVERY': 1, 'NODES': 6}, 'eventual_all_healthy_reached', 'pass',
    ),
    ('disconnect_late_write_completes', 'silent_parent.pml', {}, 'old_write_completes', 'pass'),
    ('disconnect_session_drops_old_vote', 'silent_parent.pml', {}, 'reach_session_drop', 'witness'),
    ('disconnect_aborted_task_ignores_callback', 'silent_parent.pml', {}, 'reach_old_callback_ignore', 'witness'),
    ('disconnect_old_heartbeat_rejected', 'silent_parent.pml', {}, 'reach_heartbeat_rejection', 'witness'),
    (
        'disconnect_middle_recovery_reached', 'silent_parent.pml',
        {'LATE_RECOVERY': 1}, 'reach_recovered_middle', 'witness',
    ),
    (
        'disconnect_negative_old_intermediate_routing', 'silent_parent.pml',
        {'ENABLE_DISCONNECT': 0}, 'eventual_success', 'cycle',
    ),
    (
        'disconnect_negative_old_root_routing', 'silent_parent.pml',
        {'ENABLE_DISCONNECT': 0, 'ROOT_HUNG': 1}, 'eventual_success', 'cycle',
    ),
    ('disconnect_negative_no_cooldown', 'silent_parent.pml', {'COOLDOWN': 0}, 'eventual_success', 'cycle'),
    (
        'disconnect_negative_no_session_fence', 'silent_parent.pml',
        {'SESSION_FENCE': 0}, 'request_terminates', 'assertion',
    ),
    (
        'disconnect_negative_missing_pile', 'silent_parent.pml',
        {'MISSING_PILE': 1, 'NODES': 6}, 'eventual_success', 'cycle',
    ),
    ('heartbeat_three_consecutive_misses', 'heartbeat.pml', {'CASE': 1}, 'finishes', 'pass'),
    ('heartbeat_reply_invalidates_deadline', 'heartbeat.pml', {'CASE': 2}, 'finishes', 'pass'),
    ('heartbeat_binding_session_changed', 'heartbeat.pml', {'CASE': 3}, 'finishes', 'pass'),
    ('heartbeat_stale_reply_cookie', 'heartbeat.pml', {'CASE': 4}, 'finishes', 'pass'),
    ('heartbeat_nonroot_alive_no_scepter', 'heartbeat.pml', {'CASE': 5}, 'finishes', 'pass'),
    ('heartbeat_brief_pause_two_misses_reset', 'heartbeat.pml', {'CASE': 6}, 'finishes', 'pass'),
    ('heartbeat_wrong_sender', 'heartbeat.pml', {'CASE': 7}, 'finishes', 'pass'),
    ('heartbeat_wrong_response_session', 'heartbeat.pml', {'CASE': 8}, 'finishes', 'pass'),
    ('heartbeat_binding_cookie_changed', 'heartbeat.pml', {'CASE': 9}, 'finishes', 'pass'),
    ('heartbeat_reply_deadline_race', 'heartbeat.pml', {'CASE': 10}, 'finishes', 'pass'),
    ('heartbeat_reach_poison_on_third_miss', 'heartbeat.pml', {'CASE': 1}, 'reached_poison', 'witness'),
    ('heartbeat_reach_nonroot_alive', 'heartbeat.pml', {'CASE': 5}, 'reached_alive_without_scepter', 'witness'),
    (
        'heartbeat_negative_missing_cookie_bump', 'heartbeat.pml',
        {'CASE': 2, 'BUMP_COOKIE': 0}, 'finishes', 'assertion',
    ),
    (
        'heartbeat_negative_stale_actor_poisons_session', 'heartbeat.pml',
        {'CASE': 3, 'CHECK_BINDING': 0}, 'finishes', 'assertion',
    ),
    (
        'heartbeat_negative_old_reply_resets_streak', 'heartbeat.pml',
        {'CASE': 4, 'CHECK_RESPONSE_COOKIE': 0}, 'finishes', 'assertion',
    ),
    (
        'heartbeat_negative_scepter_required', 'heartbeat.pml',
        {'CASE': 5, 'REQUIRE_SCEPTER': 1}, 'finishes', 'assertion',
    ),
    (
        'heartbeat_negative_wrong_sender_accepted', 'heartbeat.pml',
        {'CASE': 7, 'CHECK_RESPONSE_ORIGIN': 0}, 'finishes', 'assertion',
    ),
    (
        'heartbeat_negative_wrong_session_accepted', 'heartbeat.pml',
        {'CASE': 8, 'CHECK_RESPONSE_ORIGIN': 0}, 'finishes', 'assertion',
    ),
    (
        'heartbeat_negative_old_binding_cookie', 'heartbeat.pml',
        {'CASE': 9, 'CHECK_BINDING': 0}, 'finishes', 'assertion',
    ),
]


def check(snapshot, directory, definitions, claim, expected, args):
    directory.mkdir()
    shutil.copy2(snapshot, directory / snapshot.name)
    flags = [f"-D{key}={value}" for key, value in sorted(definitions.items())]
    commands = [
        [args.spin, "-a", *flags, snapshot.name],
        [args.cc, "-O2", "-DNFAIR=3", "-DVECTORSZ=4096", f"-DMEMLIM={args.memory_limit}",
         "-o", "pan", "pan.c"],
        ["./pan", "-a", "-f", "-N", claim, "-m100000", "-w22"],
    ]
    result = {"directory": str(directory), "defines": definitions, "claim": claim,
              "expected": expected, "sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(), "stages": []}
    (directory / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    for stage, command in zip(("generate", "compile", "search"), commands):
        run, output = execute(command, directory, stage + ".txt", args.timeout)
        result["stages"].append(run)
        if stage != "search" and run["timed_out"]:
            return dict(result, status="INCOMPLETE", reason=stage + " timed out")
        if stage != "search" and run["returncode"]:
            return dict(result, status="SETUP_FAILED", reason=stage + " failed")
    status, reason, stats = classify_search(run, output)
    result["states"] = str(stats["states_stored"]) if stats["states_stored"] is not None else None
    result["errors"] = stats["errors"]
    if status not in ("PASS", "FAIL"):
        return dict(result, status=status, reason=reason)
    if status == "PASS":
        return dict(result, status="PASS" if expected == "pass" else "CONTROL_FAILED",
                    reason="exhaustive search completed" if expected == "pass" else "expected witness absent")
    replay, trace = execute([args.spin, "-t", "-p", "-g", *flags, snapshot.name],
                            directory, "trail.txt", args.timeout)
    result["replay"] = replay
    if replay["timed_out"] or replay["returncode"]:
        return dict(result, status="INCOMPLETE", reason="counterexample replay failed")
    if expected == "cycle" and "acceptance cycle" in output:
        return dict(result, status="EXPECTED_COUNTEREXAMPLE", reason="expected no-success acceptance cycle")
    if expected == "assertion" and "assertion violated" in output:
        return dict(result, status="EXPECTED_COUNTEREXAMPLE", reason="expected broken-protocol assertion")
    witness = {
        "reach_session_drop": "oldSessionDropped",
        "reach_old_queued_completion": "oldQueuedCompletionBeforeDisconnect",
        "reach_partial_proposal_seen": "partialProposalSeen",
        "reach_partial_proposal_unseen": "partialProposalUnseen",
        "reach_partial_read_guard": "partialReadGuarded",
        "reach_old_callback_ignore": "oldCallbackIgnored",
        "reach_heartbeat_rejection": "heartbeatReplyRejected",
        "reach_recovered_middle": "recoveredMiddleRejoined",
        "reached_poison": "poisoned",
        "reached_alive_without_scepter": "aliveReplyAccepted",
    }.get(claim)
    if (expected == "witness" and witness and "assertion violated" in output
            and witness in output and re.search(rf"\b{witness}\s*=\s*1\b", trace)):
        return dict(result, status="EXPECTED_REACHABILITY", reason="negated reachability property reached")
    return dict(result, status="FAIL", reason="unexpected counterexample")


def positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=[case[0] for case in CASES], dest="cases")
    parser.add_argument("--timeout", type=positive, default=90, help="Seconds per subprocess")
    parser.add_argument("--memory-limit", type=positive, default=512, help="Verifier memory limit in MB")
    parser.add_argument("--spin", default="spin")
    parser.add_argument("--cc", default="cc")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    sources = Path(__file__).resolve().parent
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="distconf-silent-parent-spin-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("--output must be empty")
    summary = {
        "scope": "Bounded heartbeat cookies and routing after actual session disconnect. "
                 "3 identities or 6 identities in two abstract piles; healthy routes and physical "
                 "operations fit a scatter window after the finite fault prefix. "
                 "Also checks a queued bridge-quorum history with eight identities, arbitrary partial reads, "
                 "deadline failure and a successful fresh-read retry after a finite fault prefix. "
                 "Not a complete config consensus, general bridge erasure, or arbitrary timing proof.",
        "storage": "Exact full-state search, partial-order reduction, weak process fairness",
        "limits": {"subprocess_seconds": args.timeout, "memory_mb": args.memory_limit},
        "models": {}, "tools": {}, "results": [],
    }
    for filename in ("silent_parent.pml", "heartbeat.pml", "queued_old_quorum.pml"):
        snapshot = output / filename
        shutil.copy2(sources / filename, snapshot)
        summary["models"][filename] = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    for name, command in (("spin", [args.spin, "-V"]), ("cc", [args.cc, "--version"])):
        try:
            run, version = execute(command, output, name + "-version.txt", args.timeout)
        except OSError as error:
            parser.error(f"Cannot run {name}: {error}")
        if run["timed_out"] or run["returncode"]:
            parser.error(f"Cannot run {name}")
        summary["tools"][name] = version.splitlines()[0]
    print(f"Artifacts: {output}", flush=True)
    for name, filename, definitions, claim, expected in CASES:
        if args.cases and name not in args.cases:
            continue
        claims = [(claim, expected)]
        if filename == "silent_parent.pml" and expected == "pass":
            claims.insert(0, ("session_fencing", "pass"))
        for check_claim, check_expected in claims:
            directory = output / f"{name}-{check_claim}"
            try:
                result = check(output / filename, directory, definitions, check_claim, check_expected, args)
            except (OSError, RuntimeError) as error:
                result = {"status": "SETUP_FAILED", "reason": str(error), "directory": str(directory)}
            result["case"] = name
            summary["results"].append(result)
            summary["counts"] = dict(Counter(item["status"] for item in summary["results"]))
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(f"{name}/{check_claim}: {result['status']}; {result['reason']}", flush=True)
    print(json.dumps(summary["counts"], sort_keys=True), flush=True)
    statuses = summary["counts"]
    if statuses.get("INCOMPLETE") or statuses.get("SETUP_FAILED"):
        return 2
    return int(any(status not in ("PASS", "EXPECTED_COUNTEREXAMPLE", "EXPECTED_REACHABILITY") for status in statuses))


if __name__ == "__main__":
    raise SystemExit(main())
