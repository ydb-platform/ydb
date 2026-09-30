#!/usr/bin/env python3
"""Run exact SPIN searches in a temporary directory; never build YDB."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time

CASES = {
    "split": {},
    "split_host2": {"STORAGE_NODE": 2},
    "split_host3": {"STORAGE_NODE": 3},
    "one_timeout": {"TIMEOUT_BUDGET": 1},
    "one_disconnect": {"FAULT_BUDGET": 1},
    "timeout_and_disconnect": {"TIMEOUT_BUDGET": 1, "FAULT_BUDGET": 1},
    "healed_partition": {"INITIAL_LINKS": 4},
    "joined_then_disconnect": {"INITIAL_TREE": 2, "FAULT_BUDGET": 1},
    "singletons": {"INITIAL_TREE": 0},
    "one_peer_offline": {"STORAGE_NODE": 3, "ONLINE_MASK": 6, "INITIAL_TREE": 0},
    "storage_host_alone": {"ONLINE_MASK": 1},
    "weighted_drives": {"DRIVE_LAYOUT": 1},
    "old_policy": {"OLD_POLICY": 1},
    "reverse_bootstrap_root": {"BOOTSTRAP_ROOT": 3},
    "all_working_views": {"COMMITTED_MASK": 7, "INITIAL_TREE": 0},
    "two_timeouts": {"TIMEOUT_BUDGET": 2},
    "two_disconnects": {"FAULT_BUDGET": 2, "QUEUE_CAPACITY": 8},
    "yield_working_root": {"YIELD_WORKING_ROOT": 1},
    "ignore_probe_cookie": {"IGNORE_PROBE_COOKIE": 1, "TIMEOUT_BUDGET": 1},
    "operations_continuous": {"OP_MASK": 7},
    "operations_host2": {"OP_MASK": 7, "STORAGE_NODE": 2},
    "operations_host3": {"OP_MASK": 7, "STORAGE_NODE": 3},
    "operations_idle_gaps": {"OP_MASK": 7, "OP_WORKLOAD": 1},
    "operations_never_complete": {"OP_MASK": 7, "OP_WORKLOAD": 3},
    "operations_timeout": {"OP_MASK": 2, "TIMEOUT_BUDGET": 1},
    "operations_disconnect": {"OP_MASK": 2, "FAULT_BUDGET": 1},
    "operations_disconnect_all": {"OP_MASK": 7, "FAULT_BUDGET": 1},
    "operations_offline_peer": {"OP_MASK": 7, "STORAGE_NODE": 3, "ONLINE_MASK": 6, "INITIAL_TREE": 0},
    "queue_blocks_discovery": {"OP_MASK": 7, "QUEUE_BLOCKS_DISCOVERY": 1},
    "wait_for_operation": {"OP_MASK": 7, "OP_WORKLOAD": 3, "WAIT_OPERATION_BEFORE_HANDOFF": 1},
    "skip_operation_fence": {"OP_MASK": 7, "SKIP_OPERATION_FENCE": 1},
    "admit_work_during_handoff": {"OP_MASK": 2, "ADMIT_WORK_DURING_HANDOFF": 1},
    "ignore_operation_token": {"OP_MASK": 2, "TIMEOUT_BUDGET": 1, "IGNORE_OPERATION_TOKEN": 1},
    "repeat_first_candidate": {"STORAGE_NODE": 3, "ONLINE_MASK": 6, "INITIAL_TREE": 0, "REPEAT_FIRST_CANDIDATE": 1},
}
for host in range(1, 4):
    for mask in range(1, 7):
        if mask & (1 << (host - 1)) and mask != (1 << (host - 1)):
            CASES[f"host{host}_views{mask}"] = {"STORAGE_NODE": host, "COMMITTED_MASK": mask}
for event in range(1, 10):
    CASES[f"reach_event{event}"] = {"CHECK_EVENT": event, "TIMEOUT_BUDGET": 1, "FAULT_BUDGET": 1}
for event in range(10, 17):
    CASES[f"reach_event{event}"] = {"CHECK_EVENT": event, "OP_MASK": 7}
CASES["reach_event12"]["TIMEOUT_BUDGET"] = 1
CASES["reach_event15"]["OP_WORKLOAD"] = 3
CASES["reach_event16"].update(OP_MASK=2, FAULT_BUDGET=1)
for event in (17, 18):
    CASES[f"reach_event{event}"] = {"CHECK_EVENT": event, "OP_MASK": 2, "FAULT_BUDGET": 1}

EXPECTED = {"old_policy": "network_stable&&", "yield_working_root": "assertion violated (scepters",
            "ignore_probe_cookie": "scepters==old_scepters",
            "queue_blocks_discovery": "acceptance cycle",
            "wait_for_operation": "network_stable&&",
            "skip_operation_fence": "handoff_ready[audit_n]",
            "admit_work_during_handoff": "handoff_ready[audit_n]",
            "ignore_operation_token": "op_current[n]==previous_operation",
            "repeat_first_candidate": "acceptance cycle"}

BASELINE_CASES = tuple(CASES)
QUORUM_CASES = {
    "no_serviceset_split": {"NO_SERVICE_SET_MASK": 6},
    "no_serviceset_singletons": {"NO_SERVICE_SET_MASK": 6, "INITIAL_TREE": 0},
    "no_serviceset_weighted": {"NO_SERVICE_SET_MASK": 6, "DRIVE_LAYOUT": 1},
    "no_serviceset_host2": {"NO_SERVICE_SET_MASK": 5, "STORAGE_NODE": 2},
    "no_serviceset_host3": {"NO_SERVICE_SET_MASK": 3, "STORAGE_NODE": 3},
    "no_serviceset_mixed": {"NO_SERVICE_SET_MASK": 2, "DRIVE_LAYOUT": 1},
    "no_serviceset_operations": {"NO_SERVICE_SET_MASK": 6, "OP_MASK": 7},
    "no_serviceset_timeout": {"NO_SERVICE_SET_MASK": 6, "TIMEOUT_BUDGET": 1},
    "no_serviceset_disconnect": {"NO_SERVICE_SET_MASK": 6, "FAULT_BUDGET": 1},
    "no_serviceset_working_alone": {"NO_SERVICE_SET_MASK": 6, "ONLINE_MASK": 1},
    "no_serviceset_ignore_majority": {"NO_SERVICE_SET_MASK": 6, "INITIAL_TREE": 0, "IGNORE_BOOTSTRAP_NODE_MAJORITY": 1},
    "no_serviceset_use_drives": {"NO_SERVICE_SET_MASK": 6, "DRIVE_LAYOUT": 1, "USE_DRIVES_FOR_BOOTSTRAP": 1},
    "working_require_majority": {"NO_SERVICE_SET_MASK": 6, "ONLINE_MASK": 1, "REQUIRE_WORKING_NODE_MAJORITY": 1},
}
CASES.update(QUORUM_CASES)
EXPECTED.update({
    "no_serviceset_ignore_majority": 'assertion violated ( !((scepters&',
    "no_serviceset_use_drives": 'assertion violated (((binding[audit_n]||error_wait[audit_n])',
    "working_require_majority": 'acceptance cycle',
})

BOOTSTRAP_CASES = {
    "bootstrap_majority": {},
    "bootstrap_drive_quorum": {"NO_SERVICE_SET_MASK": 0},
    "bootstrap_weighted_drives": {"NO_SERVICE_SET_MASK": 0, "DRIVE_LAYOUT": 1},
    "bootstrap_weighted_majority": {"DRIVE_LAYOUT": 1},
    "bootstrap_offline1": {"ONLINE_MASK": 6},
    "bootstrap_offline2": {"ONLINE_MASK": 5},
    "bootstrap_offline3": {"ONLINE_MASK": 3},
    "bootstrap_timeout": {"TIMEOUT_BUDGET": 1, "QUEUE_CAPACITY": 10},
    "bootstrap_disconnect": {"FAULT_BUDGET": 1},
    "bootstrap_timeout_disconnect": {"TIMEOUT_BUDGET": 1, "FAULT_BUDGET": 1, "QUEUE_CAPACITY": 10},
    "bootstrap_partition": {"INITIAL_LINKS": 0},
    "bootstrap_busy": {"OP_MASK": 7},
    "bootstrap_never_complete": {"OP_MASK": 7, "OP_WORKLOAD": 3},
    "bootstrap_ignore_majority": {"IGNORE_BOOTSTRAP_NODE_MAJORITY": 1},
    "bootstrap_repeat_first_candidate": {"ONLINE_MASK": 6, "REPEAT_FIRST_CANDIDATE": 1},
}
for event in range(19, 22):
    BOOTSTRAP_CASES[f"reach_event{event}"] = {"CHECK_EVENT": event, "FAULT_BUDGET": 1, "QUEUE_CAPACITY": 8}
# Wakeup and ErrorTimeout can each add one queued local event.
BOOTSTRAP_CASES = {name: {"COMMITTED_MASK": 0, "INITIAL_TREE": 0, "NO_SERVICE_SET_MASK": 7,
                         "QUEUE_CAPACITY": 8, **defines}
                   for name, defines in BOOTSTRAP_CASES.items()}
CASES.update(BOOTSTRAP_CASES)
EXPECTED.update({
    "bootstrap_ignore_majority": 'assertion violated ( !((scepters&',
    "bootstrap_repeat_first_candidate": 'acceptance cycle',
})

def execute(command, directory, output_name, timeout):
    started = time.monotonic()
    output = directory / output_name
    with output.open("w") as stream:
        try:
            result = subprocess.run(command, cwd=directory, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout)
            return result.returncode, time.monotonic() - started, output.read_text()
        except subprocess.TimeoutExpired:
            return None, time.monotonic() - started, output.read_text()

def classify_pan_result(code, output):
    match = re.search(r"errors:\s*(\d+)", output)
    errors = int(match.group(1)) if match else None
    if code is None or re.search(r"max search depth too small|out of memory", output, re.I):
        status = "INCOMPLETE"
    elif errors:
        status = "FAIL"
    elif re.search(r"Search not completed", output, re.I):
        status = "INCOMPLETE"
    elif code != 0 or errors is None:
        status = "FAIL"
    else:
        status = "PASS"
    return status, errors

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=CASES, dest="cases")
    parser.add_argument("--suite", choices=("baseline", "quorum", "bootstrap"))
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--memory-limit", type=int, default=8192, metavar="MB")
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    source = Path(__file__).with_name("three_node_convergence.pml")
    source_bytes = source.read_bytes()
    digest = hashlib.sha256(source_bytes).hexdigest()
    output_root = args.output_root or Path(tempfile.mkdtemp(prefix="distconf-spin-"))
    output_root.mkdir(parents=True, exist_ok=True)
    report = {"source_sha256": digest, "runs": []}
    print(f"Artifacts: {output_root}", flush=True)
    failed = False
    suites = {"baseline": BASELINE_CASES, "quorum": QUORUM_CASES, "bootstrap": BOOTSTRAP_CASES}
    for name in args.cases or suites.get(args.suite, CASES):
        definitions = CASES[name]
        reachability = name.startswith("reach_event")
        model_flags = [*(f"-D{k}={v}" for k, v in definitions.items()), *(["-DNO_LTL"] if reachability else [])]
        directory = output_root / name
        directory.mkdir(exist_ok=True)
        (directory / "model.pml").write_bytes(source_bytes)
        commands = [
            ["spin", "-a", *model_flags, "model.pml"],
            ["gcc", "-O1", "-w", "-DCOLLAPSE", f"-DNFAIR={5 if definitions.get('OP_MASK') else 4}",
             "-DVECTORSZ=2048", f"-DMEMLIM={args.memory_limit}", *(["-DSAFETY"] if reachability else []),
             "-o", "pan", "pan.c"],
        ]
        run = {"case": name, "defines": definitions, "commands": commands, "directory": str(directory)}
        setup_ok = True
        for index, command in enumerate(commands):
            code, _, output = execute(command, directory, f"setup-{index}.txt", args.timeout)
            if code != 0:
                run.update(status="SETUP_FAILED", output=output[:1000])
                setup_ok = False
                break
        if setup_ok:
            command = ["./pan", "-b", "-m1000000", "-w20"]
            if not reachability:
                command.extend(["-a", "-f", "-N", "convergence"])
            run["commands"].append(command)
            code, seconds, output = execute(command, directory, "convergence.txt", args.timeout)
            status, errors = classify_pan_result(code, output)
            states = re.search(r"([\d.e+]+) states, stored", output)
            expected_failure = name in EXPECTED or reachability
            run.update(status=status, seconds=round(seconds, 3), errors=errors,
                       states=states.group(1) if states else None, expected_failure=expected_failure)
            if errors:
                replay = ["spin", "-t", "-p", "-g", *model_flags, "model.pml"]
                run["commands"].append(replay)
                replay_code, _, replay_output = execute(replay, directory, "trail.txt", args.timeout)
                expected_text = (f"REACHED event={definitions['CHECK_EVENT']}" if reachability
                                 else EXPECTED.get(name))
                evidence = replay_output if reachability else output
                if expected_failure and status == "FAIL" and replay_code == 0 and expected_text in evidence:
                    run["status"] = "EXPECTED_COUNTEREXAMPLE"
            if not expected_failure:
                safety = ["./pan", "-a", "-b", "-m1000000", "-w20", "-N", "role_exclusion"]
                run["commands"].append(safety)
                code, seconds, output = execute(safety, directory, "safety.txt", args.timeout)
                safety_status, safety_errors = classify_pan_result(code, output)
                run["safety_status"] = safety_status
                if safety_status == "FAIL" or run["status"] == "PASS":
                    run["status"] = safety_status
                run["safety_errors"] = safety_errors
                run["safety_seconds"] = round(seconds, 3)
        failed |= run["status"] not in ("PASS", "EXPECTED_COUNTEREXAMPLE")
        if (name in EXPECTED or reachability) and run["status"] == "PASS":
            failed = True
            run["status"] = "CONTROL_FAILED"
        report["runs"].append(run)
        (output_root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{name}: {run['status']}, states={run.get('states')}, seconds={run.get('seconds')}", flush=True)
    return int(failed)

if __name__ == "__main__":
    raise SystemExit(main())
