#!/usr/bin/env python3
"""Check targeted membership admission, with expected broken-protocol controls."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spin_runner import classify_search, execute


CASES = {
    "expand_1_to_2": ({"SCENARIO": 1, "OLD_N": 1, "TOTAL_N": 2}, "pass"),
    "expand_3_to_6": ({"SCENARIO": 1, "OLD_N": 3, "TOTAL_N": 6}, "pass"),
    "expand_3_to_7": ({"SCENARIO": 1, "OLD_N": 3, "TOTAL_N": 7}, "pass"),
    "pre_pr_3_to_6": ({"SCENARIO": 1, "OLD_N": 3, "TOTAL_N": 6, "MODE": 1}, "pass"),
    "old_guard_3_to_5": ({"SCENARIO": 1, "OLD_N": 3, "TOTAL_N": 5, "MODE": 0}, "pass"),
    "old_guard_3_to_6": ({"SCENARIO": 1, "OLD_N": 3, "TOTAL_N": 6, "MODE": 0}, "cycle"),
    "stale_target_after_format": ({"SCENARIO": 2}, "pass"),
    "stale_reply_after_restart": ({"SCENARIO": 3}, "pass"),
    "valid_grant_after_cancellation": ({"SCENARIO": 4}, "pass"),
    "blank_existing_member": ({"SCENARIO": 5}, "pass"),
    "recovery_generation_changed": ({"SCENARIO": 6}, "pass"),
    "node_not_pending": ({"SCENARIO": 7}, "pass"),
    "recovery_required": ({"SCENARIO": 8}, "pass"),
    "valid_conflict_advance": ({"SCENARIO": 10}, "pass"),
    "conflict_advance_epoch_changed": ({"SCENARIO": 11}, "pass"),
    "confirmation_recovery_3_to_6": ({"SCENARIO": 12, "OLD_N": 3, "TOTAL_N": 6}, "pass"),
    "old_confirmation_3_to_6": ({"SCENARIO": 12, "OLD_N": 3, "TOTAL_N": 6,
                                 "CONFIRM_RECOVERY": 0}, "cycle"),
    "second_target_after_proposed_only": ({"SCENARIO": 9}, "pass"),
    "old_guard_proposed_only_bypass": ({"SCENARIO": 9, "MODE": 0}, "assertion"),
    "unsafe_prev_exception": ({"SCENARIO": 2, "MODE": 1}, "assertion"),
    "missing_root_fence": ({"SCENARIO": 2, "ROOT_FENCE": 0}, "assertion"),
    "missing_actor_fence": ({"SCENARIO": 3, "ACTOR_FENCE": 0}, "assertion"),
}
BASE = {"MODE": 2, "OLD_N": 1, "TOTAL_N": 2, "ROOT_FENCE": 1, "ACTOR_FENCE": 1}


def check(source, directory, definitions, kind, expected, args):
    directory.mkdir()
    shutil.copy2(source, directory / source.name)
    flags = [f"-D{key}={value}" for key, value in sorted(definitions.items())]
    flags.append(f"-DCHECK_SAFETY={int(kind == 'safety')}")
    commands = [
        ["spin", "-a", *flags, source.name],
        ["cc", "-O1", "-DCOLLAPSE", "-DSEPQS", "-DNO_RESIZE", "-DNFAIR=8",
         "-DVECTORSZ=4096", f"-DMEMLIM={args.memory_limit}",
         *(["-DSAFETY", "-DNOCLAIM"] if kind == "safety" else []),
         "-o", "pan", "pan.c"],
        ["./pan", "-b", "-m200000", "-w20", *(["-a", "-f"] if kind == "liveness" else [])],
    ]
    (directory / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    result = {"check": kind, "defines": definitions, "expected": expected,
              "directory": str(directory), "stages": []}
    for stage, command in zip(("generate", "compile", "search"), commands):
        run, output = execute(command, directory, stage + ".txt", args.timeout)
        result["stages"].append(run)
        if stage != "search":
            if run["timed_out"] or run["returncode"]:
                result.update(status="INCOMPLETE" if run["timed_out"] else "SETUP_FAILED",
                              reason=stage + " failed")
                return result
        else:
            status, reason, stats = classify_search(run, output)
            result.update(status=status, reason=reason, **stats)
            if status == "FAIL":
                replay, trace = execute(["spin", "-t", "-p", "-g", *flags, source.name],
                                        directory, "trail.txt", args.timeout)
                result["replay"] = replay
                if replay["timed_out"] or replay["returncode"]:
                    result.update(status="INCOMPLETE", reason="counterexample replay failed")
                elif expected == "cycle" and "acceptance cycle" in output:
                    result.update(status="EXPECTED_COUNTEREXAMPLE", reason="expected liveness cycle")
                elif (expected == "assertion" and "assertion violated" in output
                      and "failed_safety = 1" in trace):
                    result.update(status="EXPECTED_COUNTEREXAMPLE", reason="stale targeted admission")
            elif status == "PASS" and expected != "pass":
                result.update(status="CONTROL_FAILED", reason="expected counterexample was not reached")
    return result


def positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=CASES, dest="cases")
    parser.add_argument("--timeout", type=positive, default=90, help="Seconds per subprocess")
    parser.add_argument("--memory-limit", type=positive, default=512, help="Verifier memory bound in MB")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source = Path(__file__).resolve().with_name("targeted_admission.pml")
    output = (args.output.resolve() if args.output else
              Path(tempfile.mkdtemp(prefix="distconf-admission-spin-")))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("--output must be empty")
    snapshot = output / source.name
    shutil.copy2(source, snapshot)
    summary = {
        "model": str(source), "sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
        "scope": "Finite targeted membership-admission histories; one DC, one disk/member; "
                 "exact strict-majority new quorum plus healthy old quorum. No erasure, bridge, "
                 "full recovery, binding tree, or arbitrary failures.",
        "storage": "Exact full-state COLLAPSE+SEPQS, partial-order reduction, weak process fairness",
        "limits": {"subprocess_seconds": args.timeout, "memory_mb": args.memory_limit},
        "tools": {}, "results": [],
    }
    for tool, command in (("spin", ["spin", "-V"]), ("cc", ["cc", "--version"])):
        run, version = execute(command, output, tool + "-version.txt", args.timeout)
        if run["timed_out"] or run["returncode"]:
            parser.error(f"Cannot run {tool}")
        summary["tools"][tool] = version.splitlines()[0]
    print(f"Artifacts: {output}", flush=True)
    for name in args.cases or CASES:
        overrides, expected = CASES[name]
        definitions = {**BASE, **overrides}
        checks = ["safety"] if expected == "assertion" else ["safety", "liveness"]
        for kind in checks:
            case_expected = expected if expected != "cycle" or kind == "liveness" else "pass"
            directory = output / f"{name}-{kind}"
            try:
                result = check(snapshot, directory, definitions, kind, case_expected, args)
            except (OSError, RuntimeError) as error:
                result = {"check": kind, "directory": str(directory),
                          "status": "SETUP_FAILED", "reason": str(error)}
            result["case"] = name
            summary["results"].append(result)
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(f"{name}/{kind}: {result['status']}; {result['reason']}", flush=True)
    counts = Counter(result["status"] for result in summary["results"])
    print(json.dumps(dict(counts)), flush=True)
    if counts["INCOMPLETE"] or counts["SETUP_FAILED"]:
        return 2
    return int(any(status not in {"PASS", "EXPECTED_COUNTEREXAMPLE"} for status in counts))


if __name__ == "__main__":
    raise SystemExit(main())
