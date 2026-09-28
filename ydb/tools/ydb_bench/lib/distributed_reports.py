"""Read copied host counters without assuming one machine's clock or node IDs."""

from collections import deque
from itertools import islice
import json
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.distributed_workload import result_path
from ydb.tools.ydb_bench.lib.ydb_telemetry import MAX_VIEW_BYTES, read_metrics


def attempt_counters(root, profile, attempt):
    root = Path(root)
    if attempt == "verification":
        directory = root / "verification"
    else:
        item = next((item for item in profile.get("attempts", []) if str(item.get("attempt")) == str(attempt)), None)
        if item is None:
            progress = profile.get("progress", {})
            item = progress if str(progress.get("attempt")) == str(attempt) else {}
        nodes, load = item.get("dynamic_nodes"), item.get("load")
        if type(nodes) is not int or nodes < 1 or type(load) is not int or load < 1:
            return {"samples": [], "truncated": False}
        directory = root / "dynamic-nodes-{:02d}".format(nodes) / "load-{:08d}".format(load)
    samples, host_sizes = {}, {}
    retained, count, files, invalid = 0, 0, 0, 0
    artifacts = []
    truncated = False
    for index in islice(sorted(directory.glob("repeat-*/host-metrics.json")), 101):
        if files >= 100:
            truncated = True
            break
        path = result_path(root, index.relative_to(root).as_posix())
        if path.stat().st_size > 1024 * 1024:
            raise BenchmarkError("Distributed host metrics index is too large")
        try:
            record = json.loads(path.read_text())
        except (ValueError, OSError) as error:
            raise BenchmarkError("Cannot read distributed host metrics index") from error
        if not isinstance(record, dict):
            raise BenchmarkError("Invalid distributed host metrics index")
        hosts = record.get("artifact_directories", {})
        if not isinstance(hosts, dict) or len(hosts) > 100:
            raise BenchmarkError("Invalid distributed host metrics index")
        for host, relative in hosts.items():
            if files >= 100:
                truncated = True
                break
            if not isinstance(host, str) or not host or len(host) > 256 or not isinstance(relative, str):
                raise BenchmarkError("Invalid distributed metrics host")
            samples.setdefault(host, deque())
            host_sizes.setdefault(host, 0)
            metrics_path = result_path(path.parent, relative + "/ydb-metrics.jsonl")
            value = read_metrics(metrics_path, attempt)
            files += 1
            truncated |= value["truncated"]
            invalid += value.get("invalid_records", 0)
            if metrics_path.is_file():
                artifacts.append({"host_id": host, "path": metrics_path.relative_to(root).as_posix()})
            for sample in value["samples"]:
                # Preserve each host's timestamps: counter rates are calculated
                # on that host. The UI selects a host, never merges wall clocks.
                sample["host_id"] = host
                for node in sample["nodes"]:
                    node["host_id"] = host
                size = len(json.dumps(sample).encode())
                samples[host].append((sample, size))
                host_sizes[host] += size
                retained += size
                count += 1
                while count > 300 or retained > MAX_VIEW_BYTES:
                    # Drop from the largest host history, not always the first
                    # host in the template. Every host stays inspectable.
                    largest = max(host_sizes, key=host_sizes.get)
                    removed = samples[largest].popleft()[1]
                    host_sizes[largest] -= removed
                    retained -= removed
                    count -= 1
                    truncated = True
    return {
        "samples": [sample for history in samples.values() for sample, _ in history],
        "artifacts": artifacts,
        "invalid_records": invalid,
        "truncated": truncated,
    }
