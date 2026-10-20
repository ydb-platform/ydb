"""Subprocess isolation and search-result parsing shared by the SPIN runners."""

import os
import re
import signal
import subprocess
import time


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
            if verifier:
                try:
                    os.killpg(process.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=min(2, timeout))
                except subprocess.TimeoutExpired:
                    pass
            # A compiler or verifier may leave children even after its own exit.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
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
