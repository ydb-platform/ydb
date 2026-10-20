#!/usr/bin/env python3
"""Check the distconf candidate and upgrade/rollback scenarios with SPIN."""

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


SCENARIOS = {
    1: "lost_first_committed_delivery",
    2: "committed_quorum_then_format_one_replica",
    3: "partial_committed_without_OK_then_failover",
    4: "new_OK_supersedes_partial_rollout",
    5: "split_proposals_then_successful_write",
    6: "old_root_OK_then_new_root_write",
    7: "minority_committed_then_reused_generation",
    8: "rollback_after_formatting",
    9: "blank_node_must_not_vote",
    10: "root_cancellation_preserves_replica_IO",
    11: "bounded_commit_cuts",
    12: "format_between_confirmation_snapshots",
    13: "competing_publications_without_OK",
    14: "higher_applied_before_committed_IO_completes",
    15: "upgrade_rollback_and_next_write",
    16: "rollback_with_unfinished_committed_IO",
    17: "rollback_with_unfinished_proposal_IO",
    18: "committed_quorum_without_OK_then_format",
    19: "candidate_race_with_delayed_IO_and_read_snapshots",
    20: "successful_third_write_after_candidate_race",
    21: "stale_root_refills_blank_node_between_sequential_formats",
    22: "published_body_survives_only_as_minority_proposed",
    23: "unpublished_minority_proposed_does_not_block_recovery",
    24: "stale_applied_metadata_between_sequential_formats",
    25: "blank_child_rejects_old_unsolicited_body_from_intermediary",
    26: "refill_reply_before_new_write_delivered_after_recipient_restart",
    27: "root_change_while_causal_refill_query_is_pending",
    28: "new_blank_replica_resumes_ordinary_legacy_publication",
    29: "legacy_query_body_cannot_bypass_failed_recovery",
    30: "legacy_fork_committed_quorum_next_Replace",
    31: "legacy_fork_without_body_quorum_next_Replace",
    32: "legacy_fork_root_differs_from_committed_quorum_next_Replace",
    33: "legacy_fork_client_timeout_server_continues",
    34: "legacy_repair_requires_persistent_current_body",
    35: "legacy_repair_must_not_bypass_higher_proposed_quorum",
    36: "legacy_repair_requires_stateful_read_quorum",
    37: "acknowledged_nonReplace_payload_survives_root_loss_and_Advance",
    38: "acknowledged_payload_survives_partial_reads_after_restart",
    39: "NoMetadata_is_unknown_for_acknowledged_payload_recovery",
    40: "possibly_chosen_proposal_with_obsolete_base_blocks_recovery",
    41: "possibly_chosen_proposal_requires_read_lineage",
    42: "possibly_chosen_proposal_uses_read_intermediate_lineage",
    43: "possibly_chosen_same_generation_conflict_blocks_recovery",
    44: "unpublished_recovery_waits_for_proposal_quorum_across_double_turnover",
    45: "possibly_chosen_different_generation_siblings_block_recovery",
    46: "exact_reproposal_preserves_acknowledged_payload_after_partial_recovery",
    47: "normal_child_preserves_acknowledged_parent_history",
    48: "full_read_missing_mandatory_parent_blocks",
    49: "nonbootstrap_history_requires_nonzero_recovery_base",
}
COMPATIBILITY_SCENARIOS = {6, 8, 15, 16, 17, 28, 29}
LEGACY_SCENARIOS = set(range(30, 37))
PAYLOAD_SCENARIOS = {37, 38, 39}
RECOVERY_SCENARIOS = PAYLOAD_SCENARIOS | {40, 41, 42, 43, 44, 45, 46, 47, 48, 49}
CANDIDATE_SCENARIOS = set(SCENARIOS) - COMPATIBILITY_SCENARIOS - LEGACY_SCENARIOS
VIOLATIONS = {
    1: "different_OK_values_at_same_generation",
    2: "different_committed_contents_at_same_generation",
    3: "blank_node_participates_in_vote",
    4: "same_generation_committed_fork_is_reachable",
    5: "candidate_recovery_blocked_after_its_own_race",
    6: "next_candidate_write_cannot_advance_or_get_quorum",
    7: "recovery_does_not_distinguish_published_and_unpublished_proposed",
    8: "blank_refill_accepts_unauthorized_or_stale_query_reply",
    9: "legacy_query_refill_enables_loss_of_last_quorum_committed_copy",
    10: "acknowledged_payload_lost_after_Advance",
    11: "acknowledged_payload_recovery_unexpectedly_blocked",
    12: "Advance_did_not_pass_acknowledged_generation",
    13: "possibly_chosen_lineage_selection_is_incorrect",
    14: "configuration_visible_without_durable_choice",
    15: "recovery_reproposal_cannot_get_quorum",
    16: "nonbootstrap_history_fell_back_to_zero_generation",
}


def command(args, directory, log_name, timeout):
    run, output = execute(args, directory, log_name, timeout)
    if run["timed_out"]:
        raise TimeoutError(f"{args[0]} timed out: {directory / log_name}")
    if run["returncode"]:
        raise RuntimeError(f"{args[0]} exited {run['returncode']}: {directory / log_name}")
    return output


def check(source, directory, scenario, kind, timeout, depth, *, all_old=False,
          legacy_repair=1, conservative_recovery=1, recovery_repropose=1, recovery_fresh_generation=0, parent_witness=1, require_nonzero_recovery_base=1, proof_part=None):
    directory.mkdir()
    shutil.copy2(source, directory / source.name)
    definitions = [f"-DSCENARIO={scenario}", f"-DCHECK_SAFETY={int(kind == 'safety')}",
                   f"-DALL_OLD={int(all_old)}", f"-DCONSERVATIVE_RECOVERY={conservative_recovery}",
                   f"-DRECOVERY_REPROPOSE={recovery_repropose}",
                   f"-DRECOVERY_FRESH_GENERATION={recovery_fresh_generation}",
                   f"-DPARENT_WITNESS={parent_witness}",
                   f"-DREQUIRE_NONZERO_RECOVERY_BASE={require_nonzero_recovery_base}"]
    if scenario in LEGACY_SCENARIOS:
        definitions.append(f"-DLEGACY_REPAIR={legacy_repair}")
        if proof_part:
            definitions += [f"-DLEGACY_CLOSURE={int(proof_part == 'closure')}",
                            f"-DLEGACY_REACH={int(proof_part == 'fair_reach')}"]
    spin = ["spin", *definitions]
    compile_args = ["cc", "-O1", "-DNFAIR=5", "-DVECTORSZ=4096"]
    if kind == "safety":
        compile_args += ["-DSAFETY", "-DNOCLAIM"]
    compile_args += ["-o", "pan", "pan.c"]
    search = ["./pan", f"-m{depth}", "-w20"]
    if kind == "liveness":
        search += ["-a", "-f"]
    commands = [spin + ["-a", source.name], compile_args, search]
    (directory / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    command(commands[0], directory, "generate.out", timeout)
    command(commands[1], directory, "compile.out", timeout)
    run, output = execute(commands[2], directory, "pan.out", timeout)
    status, reason, stats = classify_search(run, output)
    if status not in ("PASS", "FAIL") or stats["depth_reached"] is None:
        raise RuntimeError(f"{reason}: {directory / 'pan.out'}")
    error_count = stats["errors"]
    result = {
        "scenario": scenario, "name": SCENARIOS[scenario], "check": kind,
        "deployment": "all_old_reference" if all_old else "scenario",
        "status": "counterexample" if error_count else "pass", "errors": error_count,
        "states_stored": stats["states_stored"], "depth": stats["depth_reached"],
        "conservative_recovery": bool(conservative_recovery),
        "recovery_repropose": bool(recovery_repropose),
        "recovery_fresh_generation": bool(recovery_fresh_generation),
        "parent_witness": bool(parent_witness),
        "require_nonzero_recovery_base": bool(require_nonzero_recovery_base),
        "directory": str(directory),
    }
    if scenario in LEGACY_SCENARIOS:
        result["legacy_repair"] = bool(legacy_repair)
        if proof_part:
            result["proof_part"] = proof_part
    if error_count:
        replay = command(spin + ["-t", "-p", "-g", source.name], directory, "replay.out", timeout)
        violations = re.findall(r"\bviolation = (\d+)", replay)
        result["violation"] = VIOLATIONS.get(int(violations[-1]), "none") if violations else "none"
        result["acceptance_cycle"] = "acceptance cycle" in output
        if kind == "safety" and result["violation"] == "none":
            raise RuntimeError(f"Model/harness assertion or invalid end state: {directory / 'replay.out'}")
        if kind == "liveness" and not result["acceptance_cycle"]:
            raise RuntimeError(f"Failure before a liveness cycle: {directory / 'pan.out'}")
    return result


def assess(result, reference):
    if not reference:
        return "property_pass" if result["status"] == "pass" else "property_counterexample"
    if result["status"] == reference["status"] == "pass":
        return "no_transition_regression"
    if result["status"] == "pass" and reference["status"] == "counterexample":
        return "improvement_over_old"
    if result["status"] == "counterexample" and reference["status"] == "pass":
        return "transition_regression"
    # Cases 8/29 retain A=2/B=empty/C=1 under the old coordinator. Case29's
    # safety assertion separately rejects newly enabled loss of its last copy.
    if (result["scenario"] in {8, 29} and result["check"] == "liveness"
            and result["status"] == reference["status"] == "counterexample"
            and result.get("acceptance_cycle") and reference.get("acceptance_cycle")
            and result.get("violation") == reference.get("violation") == "none"):
        return "inherited_legacy_behavior"
    return "unresolved_comparison"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=["candidate", "compatibility", "legacy", "recovery", "all"], default="candidate")
    parser.add_argument("--scenario", type=int, choices=SCENARIOS, nargs="+")
    parser.add_argument("--check", choices=["safety", "liveness", "all"], default="all")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--max-depth", type=int, default=200000)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--legacy-repair", type=int, choices=[0, 1], default=1,
                        help="Disable the explicit legacy repair for negative controls")
    parser.add_argument("--conservative-recovery", type=int, choices=[0, 1], default=1,
                        help="Set 0 for the recovery-suite payload-loss negative control (safety only)")
    parser.add_argument("--recovery-repropose", type=int, choices=[0, 1], default=1,
                        help="Set 0 for scenario 44's unsafe immediate-publication control (safety only)")
    parser.add_argument("--recovery-fresh-generation", type=int, choices=[0, 1], default=0,
                        help="Set 1 for scenario 46's partial-recovery overwrite control (safety only)")
    parser.add_argument("--parent-witness", type=int, choices=[0, 1], default=1,
                        help="Set 0 for scenario 47's normal-child overwrite control (safety only)")
    parser.add_argument("--require-nonzero-recovery-base", type=int, choices=[0, 1], default=1,
                        help="Set 0 for scenario 49's zero-generation fallback control (safety only)")
    parser.add_argument("--decompose", action="store_true",
                        help="For legacy Replace cases, prove goal closure and fair reachability")
    args = parser.parse_args()
    suite_scenarios = (CANDIDATE_SCENARIOS if args.suite == "candidate" else
                       COMPATIBILITY_SCENARIOS if args.suite == "compatibility" else
                      RECOVERY_SCENARIOS if args.suite == "recovery" else
                      LEGACY_SCENARIOS if args.suite == "legacy" else set(SCENARIOS))
    if args.scenario is None:
        args.scenario = sorted(suite_scenarios)
    elif not set(args.scenario) <= suite_scenarios:
        parser.error("The requested scenarios do not belong to the selected --suite")
    if args.decompose and (not set(args.scenario) & LEGACY_SCENARIOS or not args.legacy_repair):
        parser.error("--decompose requires legacy cases with --legacy-repair 1")
    if not args.conservative_recovery and (not set(args.scenario) <= PAYLOAD_SCENARIOS
                                          or args.check == "liveness"):
        parser.error("--conservative-recovery 0 requires payload scenarios 37-39 and safety/all checks")
    if not args.recovery_repropose and (set(args.scenario) != {44} or args.check == "liveness"
                                      or not args.conservative_recovery):
        parser.error("--recovery-repropose 0 requires scenario 44, conservative recovery and safety/all checks")
    if args.recovery_fresh_generation and (set(args.scenario) != {46} or args.check == "liveness"
                                           or not args.conservative_recovery or not args.recovery_repropose
                                           or args.parent_witness):
        parser.error("--recovery-fresh-generation 1 requires scenario 46, --parent-witness 0 and conservative reproposal safety/all checks")
    if not args.parent_witness and ((set(args.scenario) != {47}
                                    and not (set(args.scenario) == {46} and args.recovery_fresh_generation))
                                   or args.check == "liveness" or not args.conservative_recovery
                                   or not args.recovery_repropose):
        parser.error("--parent-witness 0 requires scenario 47, or scenario 46 with fresh recovery, and conservative reproposal safety/all checks")
    if not args.require_nonzero_recovery_base and (set(args.scenario) != {49} or args.check == "liveness"
                                                  or not args.conservative_recovery):
        parser.error("--require-nonzero-recovery-base 0 requires scenario 49 and conservative recovery safety/all checks")
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="distconf-spin-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("--output must be empty")
    source = Path(__file__).resolve().with_name("distconf.pml")
    snapshot = output / source.name
    shutil.copy2(source, snapshot)
    summary = {
        "model": str(source), "sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
        "scope": "explicit bounded prefixes; cases 11, 19-20 vary IO/read/delivery cuts; "
                 "cases 37-39 use proposal-quorum non-Replace OK and preserve a semantic payload through Advance; "
                 "case 38 varies partial read masks; missing/error/NoMetadata disks are unknown acceptors; "
                 "cases 40-43 and 45 require read ancestry and comparable possible candidates; "
                 "possible-only recovery replays the exact identity before visibility; "
                 "case 44 varies pending replica writes across a second root turnover; case 46 retries after partial recovery and restarts before Advance; cases 47-49 cover normal-child overwrite, missing parents and absent nonzero bases; "
                 "other cases retain Replace committed-quorum OK; concurrent fair stable suffix; "
                 "N=3/Q=2, nonzero BASE except scenario 49's bounded bootstrap prefix, direct publications and selected two-hop refill RPC paths, "
                 "one disk/node, fixed membership; C++ emergency invocation and Scepter orchestration are outside scope; "
                 "case25 unsolicited stale body is an explicit overapproximation",
        "suite": args.suite,
        "tools": {
            "spin": command(["spin", "-V"], output, "spin-version.out", args.timeout).strip(),
            "cc": command(["cc", "--version"], output, "cc-version.out", args.timeout).splitlines()[0],
        },
        "results": [],
        "references": [],
        "compatibility_contract": "Only additional failures caused by upgrade/downgrade are regressions; "
                                  "the same pre-existing behavior of the old coordinator is permitted.",
    }
    checks = ["safety", "liveness"] if args.check == "all" else [args.check]
    print(f"Artifacts: {output}", flush=True)
    references = {}
    for scenario in sorted(COMPATIBILITY_SCENARIOS.intersection(args.scenario)):
        for kind in checks:
            directory = output / f"all-old-s{scenario}-{kind}"
            try:
                result = check(snapshot, directory, scenario, kind, args.timeout,
                               args.max_depth, all_old=True)
            except (RuntimeError, OSError) as error:
                result = {"scenario": scenario, "check": kind, "status": "tool_error",
                          "detail": str(error), "directory": str(directory)}
            references[scenario, kind] = result
            summary["references"].append(result)
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(json.dumps(result), flush=True)
    for scenario in args.scenario:
        case_checks = (["safety"] if scenario in {34, 35, 36, 40, 41, 42, 43, 45, 48, 49}
                       or not args.conservative_recovery or not args.recovery_repropose
                       or args.recovery_fresh_generation or not args.parent_witness
                       or not args.require_nonzero_recovery_base else checks)
        decomposed = args.decompose and scenario in {30, 31, 32, 33}
        if decomposed:
            case_checks = ["safety", "liveness"]
        for kind in case_checks:
            proof_part = ("closure" if kind == "safety" else "fair_reach") if decomposed else None
            directory = output / f"s{scenario}-{proof_part or kind}"
            try:
                result = check(snapshot, directory, scenario, kind, args.timeout, args.max_depth,
                               legacy_repair=args.legacy_repair, conservative_recovery=args.conservative_recovery,
                               recovery_repropose=args.recovery_repropose,
                               recovery_fresh_generation=args.recovery_fresh_generation,
                               parent_witness=args.parent_witness,
                               require_nonzero_recovery_base=args.require_nonzero_recovery_base,
                               proof_part=proof_part)
            except (RuntimeError, OSError) as error:
                result = {"scenario": scenario, "check": kind,
                          "status": "tool_error", "detail": str(error), "directory": str(directory)}
            result["assessment"] = assess(result, references.get((scenario, kind)))
            if not args.conservative_recovery and result["status"] != "tool_error":
                if (result["status"] == "counterexample"
                        and result.get("violation") == VIOLATIONS[10]):
                    result["assessment"] = "expected_old_recovery_payload_loss"
                else:
                    result.update(status="control_failed", assessment="old_recovery_payload_loss_not_reproduced")
            if not args.recovery_repropose and result["status"] != "tool_error":
                if result["status"] == "counterexample" and result.get("violation") == VIOLATIONS[14]:
                    result["assessment"] = "expected_unsafe_recovery_publication"
                else:
                    result.update(status="control_failed", assessment="unsafe_recovery_publication_not_reproduced")
            if args.recovery_fresh_generation and result["status"] != "tool_error":
                if result["status"] == "counterexample" and result.get("violation") == VIOLATIONS[10]:
                    result["assessment"] = "expected_fresh_recovery_payload_loss"
                else:
                    result.update(status="control_failed", assessment="fresh_recovery_payload_loss_not_reproduced")
            if not args.parent_witness and scenario == 47 and result["status"] != "tool_error":
                if result["status"] == "counterexample" and result.get("violation") == VIOLATIONS[10]:
                    result["assessment"] = "expected_normal_child_payload_loss"
                else:
                    result.update(status="control_failed", assessment="normal_child_payload_loss_not_reproduced")
            if not args.require_nonzero_recovery_base and result["status"] != "tool_error":
                if result["status"] == "counterexample" and result.get("violation") == VIOLATIONS[16]:
                    result["assessment"] = "expected_zero_generation_fallback"
                else:
                    result.update(status="control_failed", assessment="zero_generation_fallback_not_reproduced")
            if scenario in LEGACY_SCENARIOS and not args.legacy_repair and kind == "liveness":
                if result.get("acceptance_cycle") and result.get("violation") == "none":
                    result["assessment"] = "expected_disabled_repair_cycle"
                elif result["status"] == "pass":
                    result.update(status="control_failed", assessment="disabled_repair_control_failed")
            summary["results"].append(result)
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(json.dumps(result), flush=True)
            if proof_part == "closure" and result["status"] != "pass":
                break
    counts = Counter(result["status"] for result in summary["results"])
    assessments = Counter(result["assessment"] for result in summary["results"])
    print(json.dumps({"statuses": dict(counts), "assessments": dict(assessments)}), flush=True)
    if counts["tool_error"] or any(row["status"] == "tool_error" for row in summary["references"]):
        return 2
    return int(any(row["status"] != "pass" and row["assessment"] not in {
                       "inherited_legacy_behavior", "expected_disabled_repair_cycle", "expected_old_recovery_payload_loss",
                       "expected_unsafe_recovery_publication", "expected_fresh_recovery_payload_loss",
                       "expected_normal_child_payload_loss", "expected_zero_generation_fallback"}
                   for row in summary["results"]))


if __name__ == "__main__":
    raise SystemExit(main())
