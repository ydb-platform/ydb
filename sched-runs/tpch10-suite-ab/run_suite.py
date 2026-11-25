#!/usr/bin/env python3
import contextlib
import csv
import importlib.util
import json
import math
import os
from pathlib import Path
import resource
import re
import shutil
import signal
import statistics
import subprocess
import time
from collections import defaultdict
from types import SimpleNamespace

ROOT = Path("/home/mfilitov/tpch10-suite-ab")
REPO = Path("/home/mfilitov/ydb-pr56258-local-consumer")
DUCK = Path("/home/mfilitov/tpch1-kqprun/bin/duckdb")
BINARY = ROOT / "kqprun"
GIB = 1024 ** 3
TIMEOUT = 600
spec = importlib.util.spec_from_file_location("q5", "/home/mfilitov/tpch100-q5-ab/q5_regression.py")
q5 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q5)


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(path)


def status(phase, **details):
    save(ROOT / "progress.json", dict(phase=phase, updated_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **details))


def metric(value, aggregate="Sum"):
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, dict):
        return value.get(aggregate, value.get(aggregate.lower(), 0))
    return 0


def plan_stats(path):
    if not path.exists() or not path.stat().st_size:
        return {}
    tree = json.loads(path.read_text())
    stages = {}
    def walk(node):
        stats = node.get("Stats", {})
        stage = stats.get("PhysicalStageId")
        if stage is not None and "Tasks" in stats:
            stages[stage] = dict(
                id=stage, operators=[op.get("Name", "") for op in node.get("Operators", [])],
                tasks=stats["Tasks"], nodes=stats.get("Nodes", []),
                accounted_compute_us=metric(stats.get("CpuTimeUs")),
                input_rows=metric(stats.get("InputRows")), output_rows=metric(stats.get("OutputRows")),
                input_bytes=metric(stats.get("InputBytes")), output_bytes=metric(stats.get("OutputBytes")),
                spill_compute_bytes=metric(stats.get("SpillingComputeBytes")),
                spill_channel_bytes=metric(stats.get("SpillingChannelBytes")),
                wait_input_us=metric(stats.get("WaitInputTimeUs")),
                wait_output_us=metric(stats.get("WaitOutputTimeUs")),
                max_task_duration_us=metric(stats.get("DurationUs"), "Max"),
            )
        for child in node.get("Plans", []):
            walk(child)
    walk(tree["Plan"])
    return dict(
        tasks=sum(s["tasks"] for s in stages.values()), stages=len(stages),
        spill_compute_bytes=sum(s["spill_compute_bytes"] for s in stages.values()),
        spill_channel_bytes=sum(s["spill_channel_bytes"] for s in stages.values()),
        stage_stats=[stages[k] for k in sorted(stages)],
    )


def launch_kqp(cmd, **kwargs):
    config = Path(cmd[cmd.index("-c") + 1])
    content = config.read_text()
    assert content.count("MaxTotalSize: 3221225472") == 1
    content = content.replace("MaxTotalSize: 3221225472", "MaxTotalSize: 17179869184")
    if "ScriptResultSizeLimit:" in content:
        content, count = re.subn(r"ScriptResultSizeLimit:\s*\d+", "ScriptResultSizeLimit: 0", content)
        assert count == 1
    else:
        content = content.replace("ScriptResultRowsLimit: 0", "ScriptResultRowsLimit: 0\n  ScriptResultSizeLimit: 0")
    config.write_text(content)
    enabled = "EnableTaskScheduling: true" in content
    cmd.extend(["-u", str(ROOT / "libmath_udf.so")])
    kwargs["env"]["YDB_DQ_TASK_PREFER_CONSUMERS"] = str(int(enabled))
    manifest = json.loads((ROOT / "manifest.json").read_text())
    assert BINARY.stat().st_ino == manifest["binary_inode"]
    save(config.parent / "command.json", dict(
        argv=cmd, cwd=str(kwargs["cwd"]), binary_inode=BINARY.stat().st_ino,
        scheduler_enabled=enabled, prefer_consumers=enabled,
        environment={key: kwargs["env"][key] for key in ("PORT_SYNC_PATH", "YDB_DQ_TASK_PREFER_CONSUMERS")},
    ))
    return subprocess.Popen(cmd, **kwargs)


def reference(number):
    folder = ROOT / "references" / f"q{number:02}"
    folder.mkdir(parents=True, exist_ok=True)
    summary_path = folder / "summary.json"
    if summary_path.exists():
        existing = json.loads(summary_path.read_text())
        if existing["status"] == "ok":
            return existing
    temp = folder / "temp"
    temp.mkdir(exist_ok=True)
    script = [
        "SET threads=8;", "SET memory_limit='24GB';",
        "SET max_temp_directory_size='32GB';",
        f"SET temp_directory='{temp}';",
    ]
    for table in ("customer", "lineitem", "orders", "part", "partsupp", "supplier", "nation", "region"):
        location = Path("/home/mfilitov/tpch10-kqprun/data32") / (f"{table}/*.parquet" if table in ("lineitem", "orders", "supplier") else f"{table}.parquet")
        script.append(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{location}');")
    query = (ROOT / "reference-sql" / f"q{number:02}.sql").read_text().strip().rstrip(";")
    script.append(f"COPY ({query}) TO '{folder / 'result.json'}' (FORMAT JSON, ARRAY true);")
    sql = "\n".join(script) + "\n"
    (folder / "run.sql").write_text(sql)
    start = time.monotonic()
    with (folder / "duckdb.log").open("w") as output:
        process = subprocess.Popen([str(DUCK), "-batch", "-bail", ":memory:"], stdin=subprocess.PIPE,
                                   stdout=output, stderr=subprocess.STDOUT, text=True, start_new_session=True)
        try:
            process.communicate(sql, timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=20)
    summary = dict(query=number, status="ok" if process.returncode == 0 else "error",
                   exit_code=process.returncode, seconds=time.monotonic() - start)
    if summary["status"] == "ok":
        summary["rows"] = len(json.loads((folder / "result.json").read_text()))
    else:
        summary["error"] = (folder / "duckdb.log").read_text(errors="replace")[-3000:]
    shutil.rmtree(temp)
    save(summary_path, summary)
    return summary


def compare_rows(actual, expected):
    if len(actual) != len(expected):
        return dict(ok=False, error=f"row count {len(actual)} != {len(expected)}")
    if not expected:
        return dict(ok=True, rows=0)
    expected_columns = list(expected[0])
    actual_columns = list(actual[0])
    if len(expected_columns) != len(actual_columns):
        return dict(ok=False, error=f"columns {actual_columns} != {expected_columns}")
    mapping = dict(zip(expected_columns, actual_columns))
    if set(expected_columns) == set(actual_columns):
        mapping = {key: key for key in expected_columns}
    float_columns = {
        key for key in expected_columns
        if any(isinstance(row[key], float) for row in expected)
    }
    sample_types = {}
    for key in expected_columns:
        sample_types[key] = next((type(row[key]) for row in expected if row[key] is not None), type(None))
    def normalized(row):
        result = {}
        for key in expected_columns:
            value = row[mapping[key]]
            if value is not None and isinstance(value, str) and sample_types[key] in (int, float):
                value = sample_types[key](value)
            result[key] = value
        return result
    exact_columns = [key for key in expected_columns if key not in float_columns]
    def grouped(rows, normalize=False):
        groups = defaultdict(list)
        for row in rows:
            if normalize:
                row = normalized(row)
            groups[tuple(row[key] for key in exact_columns)].append(row)
        return groups
    left = grouped(actual, normalize=True)
    right = grouped(expected)
    if left.keys() != right.keys():
        return dict(ok=False, error="non-floating values differ",
                    actual_example=str(next(iter(left.keys() - right.keys()), None))[:500],
                    expected_example=str(next(iter(right.keys() - left.keys()), None))[:500])
    max_relative = max_absolute = 0.0
    for key, ref_rows in right.items():
        got_rows = left[key]
        if len(got_rows) != len(ref_rows):
            return dict(ok=False, error=f"duplicate count differs for {str(key)[:200]}")
        order = lambda row: tuple((row[c] is not None, row[c] or 0) for c in sorted(float_columns))
        for got, want in zip(sorted(got_rows, key=order), sorted(ref_rows, key=order)):
            for column in float_columns:
                a, b = got[column], want[column]
                if a is None or b is None:
                    if a != b:
                        return dict(ok=False, error=f"NULL mismatch in {column}")
                    continue
                a, b = float(a), float(b)
                if not math.isclose(a, b, rel_tol=1e-8, abs_tol=1e-5):
                    return dict(ok=False, error=f"numeric mismatch in {column}: {a} != {b}", key=str(key)[:200])
                max_absolute = max(max_absolute, abs(a - b))
                max_relative = max(max_relative, abs(a - b) / max(abs(a), abs(b), 1e-300))
    return dict(ok=True, rows=len(actual), max_absolute_error=max_absolute, max_relative_error=max_relative,
                column_mapping=mapping)


def execute(number, repetition, enabled):
    mode = "on" if enabled else "off"
    stem = f"q{number:02}-r{repetition}-{mode}"
    saved = ROOT / stem / "summary.json"
    if saved.exists():
        previous = json.loads(saved.read_text())
        if "validation" in previous or previous.get("final_error"):
            return previous
    for attempt in range(1, 4):
        label = stem if attempt == 1 else f"{stem}-retry{attempt}"
        folder = ROOT / label
        if folder.exists():
            folder.rename(ROOT / (label + f".incomplete.{time.time_ns()}"))
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        q5.subprocess = SimpleNamespace(Popen=launch_kqp, STDOUT=subprocess.STDOUT, TimeoutExpired=subprocess.TimeoutExpired)
        try:
            with open(os.devnull, "w") as quiet, contextlib.redirect_stdout(quiet):
                row = q5.run(ROOT, label, BINARY, enabled, 16, ROOT / "queries" / f"q{number:02}.yql",
                             TIMEOUT, "profile")
        finally:
            q5.subprocess = subprocess
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        actual = row.pop("result", [])
        row.update(query=number, repetition=repetition, mode=mode, attempt=attempt,
                   user_cpu_seconds=after.ru_utime - before.ru_utime, system_cpu_seconds=after.ru_stime - before.ru_stime,
                   result_rows=len(actual), prefer_consumers=enabled)
        log = (folder / "kqprun.log").read_text(errors="replace")
        startup_failure = row.get("error") and "RpcActor is not set before read request" in log and "Executing query..." not in log
        if startup_failure and attempt < 3:
            row["startup_retry"] = True
            save(folder / "summary.json", row)
            print(f"Retrying {stem} after LocalLeaderElection startup failure", flush=True)
            continue
        row.update(plan_stats(folder / "plan.json"))
        ref_path = ROOT / "references" / f"q{number:02}" / "result.json"
        ref_summary = json.loads((ref_path.parent / "summary.json").read_text())
        if row.get("error"):
            row["final_error"] = row["error"]
            row["validation"] = dict(ok=False, error="execution failed")
            row["error_log_tail"] = log[-4000:]
        elif ref_summary["status"] == "ok":
            row["validation"] = compare_rows(actual, json.loads(ref_path.read_text()))
        else:
            row["validation"] = dict(ok=False, error="DuckDB reference failed")
        spill = folder / "spill"
        if spill.exists():
            shutil.rmtree(spill)
            spill.mkdir()
        save(folder / "summary.json", row)
        if attempt != 1:
            save(saved, row)
        print(json.dumps({key: row.get(key) for key in ("label", "query_seconds", "cpu_seconds", "tasks", "spill_compute_bytes", "validation", "final_error")}), flush=True)
        return row


def write_reports(runs):
    save(ROOT / "runs.json", runs)
    flat_fields = [
        "query", "repetition", "mode", "attempt", "query_seconds", "cpu_seconds", "user_cpu_seconds",
        "system_cpu_seconds", "peak_rss_bytes", "tasks", "stages", "spill_compute_bytes",
        "spill_channel_bytes", "result_rows", "exit_code", "final_error", "validation_ok",
    ]
    with (ROOT / "runs.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=flat_fields)
        writer.writeheader()
        for row in runs:
            data = {key: row.get(key) for key in flat_fields}
            data["validation_ok"] = row["validation"]["ok"]
            writer.writerow(data)
    summaries = []
    for query in sorted({row["query"] for row in runs}):
        item = dict(query=query)
        for mode in ("off", "on"):
            rows = [r for r in runs if r["query"] == query and r["mode"] == mode]
            good = [r for r in rows if not r.get("error") and r["validation"]["ok"]]
            item[f"{mode}_successful"] = len(good)
            item[f"{mode}_attempted"] = len(rows)
            for key in ("query_seconds", "cpu_seconds", "peak_rss_bytes", "tasks", "spill_compute_bytes", "spill_channel_bytes"):
                item[f"{mode}_{key}"] = statistics.median(r.get(key, 0) for r in good) if good else None
        for metric_name, output_name in (("query_seconds", "wall_change_percent"), ("cpu_seconds", "cpu_change_percent")):
            a, b = (item[f"{mode}_{metric_name}"] for mode in ("off", "on"))
            item[output_name] = (b / a - 1) * 100 if a and b else None
        summaries.append(item)
    save(ROOT / "comparison.json", summaries)
    if summaries:
        with (ROOT / "comparison.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(summaries[0]))
            writer.writeheader()
            writer.writerows(summaries)


def main():
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, resource.getrlimit(resource.RLIMIT_NOFILE)[1]), resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
    status("waiting-data")
    while True:
        path = ROOT / "dataset-status.json"
        if path.exists() and json.loads(path.read_text()).get("phase") == "complete":
            break
        time.sleep(5)
    assert (ROOT / "udf-build.status").read_text().strip() == "0"
    manifest = json.loads((ROOT / "manifest.json").read_text())
    assert BINARY.stat().st_ino == manifest["binary_inode"]
    runs = json.loads((ROOT / "runs.json").read_text()) if (ROOT / "runs.json").exists() else []
    finished = {(r["query"], r["repetition"], r["mode"]) for r in runs}
    for repetition in (1,):
        numbers = range(1, 23) if repetition == 1 else range(22, 0, -1)
        for number in numbers:
            status("reference", query=number, repetition=repetition, runs_completed=len(runs), total_runs=44)
            ref = reference(number)
            if ref["status"] != "ok":
                print(f"DuckDB Q{number} failed: {ref.get('error')}", flush=True)
            order = (False, True) if (number + repetition) % 2 == 0 else (True, False)
            for enabled in order:
                mode = "on" if enabled else "off"
                if (number, repetition, mode) in finished:
                    continue
                status("running", query=number, repetition=repetition, mode=mode,
                       runs_completed=len(runs), total_runs=44)
                runs.append(execute(number, repetition, enabled))
                write_reports(runs)
    status("completed", runs_completed=len(runs), total_runs=44,
           validated=sum(r["validation"]["ok"] for r in runs))


if __name__ == "__main__":
    try:
        main()
    except BaseException as error:
        status("failed", error=repr(error))
        raise
