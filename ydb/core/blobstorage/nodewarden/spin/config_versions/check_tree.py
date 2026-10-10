#!/usr/bin/env python3
"""Run bounded exact checks of the combined tree/configuration SPIN model."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time


CASES = {
    "tree_chain": {"INITIAL_TREE": 3},
    "tree_star": {"INITIAL_TREE": 2},
    "tree_split": {"INITIAL_TREE": 1},
    "tree_singletons": {"INITIAL_TREE": 0},
    "tree_cut": {"INITIAL_TREE": 3, "FAULT_BUDGET": 1},
    "committed_chain": {"INITIAL_TREE": 3, "CFG_HIGH_MASK": 3},
    "committed_star": {"INITIAL_TREE": 2, "CFG_HIGH_MASK": 3},
    "minority_committed_chain": {"INITIAL_TREE": 3, "CFG_HIGH_MASK": 1},
    "committed_chain_cut": {"INITIAL_TREE": 3, "CFG_HIGH_MASK": 3, "FAULT_BUDGET": 1,
                            "QUEUE_CAPACITY": 8},
    "formatted_member_chain": {"INITIAL_TREE": 3, "CFG_HIGH_MASK": 3, "CFG_EMPTY_MASK": 2},
    "formatted_root_star": {"INITIAL_TREE": 2, "CFG_HIGH_MASK": 3, "CFG_EMPTY_MASK": 1},
    "replace_chain": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 1},
    "replace_star": {"INITIAL_TREE": 2, "CFG_WRITE_MODE": 1},
    "replace_ok_witness": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 1, "CHECK_EVENT": 23},
    "replace_race_chain": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 2},
    "replace_race_cut": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 2, "FAULT_BUDGET": 1,
                         "QUEUE_CAPACITY": 8},
    "replace_after_race": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 3},
    "replace_later_ok_witness": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 3, "CHECK_EVENT": 24},
    "samegen_witness": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 2, "CFG_TARGET_SAMEGEN": 1,
                         "FAULT_BUDGET": 1, "QUEUE_CAPACITY": 8, "CHECK_EVENT": 25},
    "samegen_later_ok_witness": {"INITIAL_TREE": 3, "CFG_WRITE_MODE": 3, "CFG_TARGET_SAMEGEN": 1,
                                 "FAULT_BUDGET": 1, "QUEUE_CAPACITY": 8, "CHECK_EVENT": 27},
    "root_overlap_witness": {"INITIAL_TREE": 3, "FAULT_BUDGET": 1, "CHECK_EVENT": 22,
                             "QUEUE_CAPACITY": 8},
}
DEFAULT_CASES = ("tree_chain", "committed_chain", "formatted_member_chain")
EXPECTED_WITNESSES = {"root_overlap_witness": "REACHED event=22",
                      "replace_ok_witness": "REACHED event=23",
                      "replace_later_ok_witness": "REACHED event=24",
                      "samegen_witness": "REACHED event=25",
                      "samegen_later_ok_witness": "REACHED event=27"}
BASE_DEFINES = {"CONFIG_VERSIONS": 1, "COMMITTED_MASK": 7,
                "FAULT_BUDGET": 0, "QUEUE_CAPACITY": 6, "OP_MASK": 0}


def execute(command, directory, output_name, timeout):
    started = time.monotonic()
    with (directory / output_name).open("w") as stream:
        process = subprocess.Popen(command, cwd=directory, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        timed_out = False
        try:
            # Leave time for pan's SIGINT handler to print partial search statistics.
            verifier = command[0] == "./pan"
            process.wait(timeout=max(0.1, timeout - 2) if verifier else timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            if command[0] == "./pan":
                os.killpg(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=min(2, timeout))
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            else:
                # Compilers also spawn children. Terminate the entire owned process group.
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    output = (directory / output_name).read_text(errors="replace")
    return {"command": command, "seconds": round(time.monotonic() - started, 3),
            "returncode": process.returncode, "timed_out": timed_out,
            "output": output_name}, output


def number(output, pattern, *, integer=False):
    match = re.search(pattern, output)
    if not match:
        return None
    value = float(match.group(1))
    return int(value) if integer else value


def statistics(output):
    return {
        "states_stored": number(output, r"([\d.e+]+)\s+states, stored", integer=True),
        "states_visited": number(output, r"\(([\d.e+]+)\s+visited\)", integer=True),
        "states_matched": number(output, r"([\d.e+]+)\s+states, matched", integer=True),
        "state_vector_bytes": number(output, r"State-vector\s+(\d+)\s+byte", integer=True),
        "depth_reached": number(output, r"depth reached\s+(\d+)", integer=True),
        "errors": number(output, r"errors:\s*(\d+)", integer=True),
        "memory_mb": number(output, r"([\d.e+]+)\s+total actual memory usage"),
        "pan_seconds": number(output, r"pan: elapsed time\s+([\d.e+]+)"),
    }


def classify_search(run, output):
    metrics = statistics(output)
    if run["timed_out"]:
        return "INCOMPLETE", "search timeout", metrics
    if re.search(r"max search depth too small|out of memory|VECTORSZ.*too small|"
                 r"NFAIR.*too small|fairness.*too small|too many processes|cannot allocate|"
                 r"reached.*MEMLIM.*bound", output, re.I):
        return "INCOMPLETE", "search resource or encoding bound exhausted", metrics
    if re.search(r"assertion violated[^\n]*(?:q_len\s*\(|len\s*\(|QUEUE_CAPACITY|"
                 r"fresh_used|op_used|cfg_used|CONFIG_BOUND|QUEUE_BOUND)", output, re.I):
        return "INCOMPLETE", "model queue or finite-name bound exhausted", metrics
    if "MODEL_BOUND" in output:
        return "INCOMPLETE", "model bound exhausted", metrics
    if metrics["errors"]:
        return "FAIL", "acceptance cycle" if "acceptance cycle" in output else "model violation", metrics
    if "Search not completed" in output:
        return "INCOMPLETE", "search did not complete", metrics
    if run["returncode"] != 0 or metrics["errors"] is None or metrics["states_stored"] is None:
        return "SETUP_FAILED", "verifier did not produce valid search statistics", metrics
    return "PASS", "exact search completed", metrics


def check(snapshot, directory, definitions, kind, args):
    directory.mkdir()
    shutil.copy2(snapshot, directory / snapshot.name)
    flags = [f"-D{key}={value}" for key, value in sorted(definitions.items())]
    if kind == "safety":
        flags.append("-DNO_LTL=1")
    commands = [
        ["spin", "-a", *flags, snapshot.name],
        ["cc", f"-O{args.opt_level}", "-w", "-DCOLLAPSE", "-DSEPQS", "-DNO_RESIZE", f"-DMEMLIM={args.memory_limit}",
         f"-DNFAIR={args.nfair}", f"-DVECTORSZ={args.vector_size}",
         *(["-DSAFETY", "-DNOCLAIM"] if kind == "safety" else []), "-o", "pan", "pan.c"],
        ["./pan", "-b", f"-m{args.max_depth}", f"-w{args.hash_bits}",
         *(["-a", "-f", "-N", args.claim] if kind == "liveness" else [])],
    ]
    (directory / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    result = {"check": kind, "defines": definitions, "directory": str(directory),
              "commands": commands, "stages": [], "exact_storage": "COLLAPSE+SEPQS", "por": True}
    started = time.monotonic()
    for stage, command in zip(("generate", "compile", "search"), commands):
        print(json.dumps({"stage": stage, "check": kind, "directory": str(directory)}), flush=True)
        run, output = execute(command, directory, stage + ".txt", args.timeout)
        result["stages"].append(run)
        if stage != "search":
            if run["timed_out"] or run["returncode"] != 0:
                result.update(status="INCOMPLETE" if run["timed_out"] else "SETUP_FAILED",
                              reason=stage + (" timeout" if run["timed_out"] else " failed"))
                break
        else:
            status, reason, metrics = classify_search(run, output)
            result.update(status=status, reason=reason, **metrics)
            if metrics["errors"]:
                replay = ["spin", "-t", "-p", "-g", *flags, snapshot.name]
                replay_run, replay_output = execute(replay, directory, "trail.txt", args.timeout)
                result["replay"] = replay_run
                if replay_run["timed_out"] or replay_run["returncode"] != 0:
                    result.update(status="INCOMPLETE", reason="violation trace could not be classified")
                elif re.search(r"MODEL_BOUND|queue bound exceeded|finite name bound exceeded|"
                             r"configuration bound exceeded", replay_output, re.I):
                    result.update(status="INCOMPLETE", reason="model bound exhausted")
                elif (status == "FAIL" and args.expected_witness
                      and args.expected_witness in replay_output
                      and "assertion violated" in output):
                    result.update(status="EXPECTED_WITNESS", reason=args.expected_witness)
    result["seconds"] = round(time.monotonic() - started, 3)
    if args.expected_witness and result.get("status") == "PASS":
        result.update(status="CONTROL_FAILED", reason="expected reachability witness was not reached")
    return result


def parse_define(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*=[-+A-Za-z_0-9]+", value):
        raise argparse.ArgumentTypeError("Use a preprocessor definition NAME=VALUE")
    key, value = value.split("=", 1)
    return key, value


def positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=CASES, dest="cases")
    parser.add_argument("--check", choices=("safety", "liveness", "all"), default="all")
    parser.add_argument("--define", action="append", type=parse_define, default=[], metavar="NAME=VALUE")
    parser.add_argument("--claim", default="config_convergence")
    parser.add_argument("--witness", type=positive, metavar="EVENT",
                        help="Require CHECK_EVENT reachability instead of an invariant PASS")
    parser.add_argument("--decompose", action="store_true",
                        help="Prove goal closure, then fair reachability; both must pass")
    parser.add_argument("--timeout", type=positive, default=60, help="Per subprocess, in seconds")
    parser.add_argument("--memory-limit", type=positive, default=512, metavar="MB")
    parser.add_argument("--max-depth", type=positive, default=200000)
    parser.add_argument("--hash-bits", type=positive, default=22)
    parser.add_argument("--nfair", type=positive, default=8)
    parser.add_argument("--vector-size", type=positive, default=4096)
    parser.add_argument("--opt-level", choices=("0", "1", "2"), default="0")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--model", type=Path, help="Check an existing model snapshot")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.decompose and (args.check != "all" or args.witness):
        parser.error("--decompose requires --check all and cannot be combined with --witness")
    source = (args.model.resolve() if args.model else
              Path(__file__).resolve().parent.parent / "three_node_convergence.pml")
    source_bytes = source.read_bytes()
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="distconf-tree-spin-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("--output must be empty")
    snapshot = output / source.name
    snapshot.write_bytes(source_bytes)
    summary = {"model": str(source), "sha256": hashlib.sha256(source_bytes).hexdigest(),
               "search": "exact full-state search with COLLAPSE+SEPQS and default partial-order reduction",
               "scope": "Bounded three-node model; finite failures and administration. "
                        "A PASS applies only to the recorded defines and claim.",
               "limits": {"per_process_seconds": args.timeout, "pan_memory_mb": args.memory_limit,
                          "depth": args.max_depth},
               "results": []}
    print(f"Artifacts: {output}", flush=True)
    print(f"Source SHA256: {summary['sha256']}", flush=True)
    checks = ("safety", "liveness") if args.check == "all" else (args.check,)
    for case in args.cases or DEFAULT_CASES:
        definitions = {**BASE_DEFINES, **CASES[case], **dict(args.define)}
        if int(definitions.get("CFG_WRITE_MODE", 0)) and b"CFG_WRITE_MODE" not in source_bytes:
            parser.error("This model snapshot does not implement the requested Replace mode")
        if args.witness:
            definitions["CHECK_EVENT"] = args.witness
        args.expected_witness = (f"REACHED event={args.witness}" if args.witness else
                                 EXPECTED_WITNESSES.get(case))
        if args.decompose and (args.expected_witness or args.claim != "config_convergence"):
            parser.error("--decompose applies to config_convergence invariant cases")
        if args.decompose and b"CFG_GOAL_CLOSURE" not in source_bytes:
            parser.error("This model snapshot does not implement goal decomposition")
        case_checks = (("safety",) if args.expected_witness else checks)
        for kind in case_checks:
            label = ("closure" if kind == "safety" else "reach") if args.decompose else kind
            check_definitions = dict(definitions)
            if args.decompose:
                check_definitions.update(CFG_GOAL_CLOSURE=int(kind == "safety"),
                                         CFG_GOAL_REACHABILITY=int(kind == "liveness"))
            directory = output / f"{case}-{label}"
            try:
                result = check(snapshot, directory, check_definitions, kind, args)
            except (OSError, RuntimeError) as error:
                result = {"check": kind, "defines": definitions, "directory": str(directory),
                          "status": "SETUP_FAILED", "reason": str(error)}
            result["case"] = case
            if args.decompose:
                result["proof_part"] = label
                if kind == "safety" and result.get("status") == "FAIL":
                    result.update(status="DECOMPOSITION_FAILED",
                                  reason="goal closure or a model invariant failed; no liveness conclusion")
            summary["results"].append(result)
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            if args.verbose:
                print(json.dumps(result), flush=True)
            else:
                print(f"{case}/{label}: {result['status']}; states={result.get('states_stored')}; "
                      f"search={result.get('pan_seconds')}s; memory={result.get('memory_mb')}MB; "
                      f"{result.get('reason')}", flush=True)
            if args.decompose and result.get("status") != "PASS":
                break
    statuses = [result["status"] for result in summary["results"]]
    if all(status in ("PASS", "EXPECTED_WITNESS") for status in statuses):
        return 0
    return 2 if any(status in ("INCOMPLETE", "SETUP_FAILED", "DECOMPOSITION_FAILED") for status in statuses) else 1


if __name__ == "__main__":
    raise SystemExit(main())
