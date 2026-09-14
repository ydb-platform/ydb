"""Bounded, checksummed transfer of immutable workload result files."""

import base64
import hashlib
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_bytes, atomic_write_json

MAX_RESULT_FILE_BYTES = 32 * 1024 * 1024
MAX_RESULT_BYTES = 128 * 1024 * 1024
RESULT_CHUNK_BYTES = 1024 * 1024
DIAGNOSTIC_TAIL_BYTES = 128 * 1024


def snapshot_diagnostics(root):
    """Retain bounded log tails and configs, never native executables or data."""
    root = Path(root).resolve()
    destination = root / "results" / "diagnostics"
    paths = [root / "worker.json"]
    paths.extend(sorted((root / "control").glob("*")))
    paths.extend(sorted((root / "nodes").glob("*/cluster.yaml")))
    for name in ("stdout.txt", "stderr.txt"):
        paths.extend(sorted((root / "nodes").glob("*/" + name)))
        paths.extend(sorted((root / "cli").glob("*/" + name))[-4:])
    index = []
    for source in paths:
        if source.is_symlink() or not source.resolve().is_relative_to(root):
            raise BenchmarkError("Diagnostic source escapes its generation")
        if not source.is_file():
            continue
        if len(index) >= 400:
            raise BenchmarkError("Too many distributed diagnostic files")
        size = source.stat().st_size
        offset = max(0, size - DIAGNOSTIC_TAIL_BYTES)
        relative = source.relative_to(root).as_posix()
        target = relative + ".tail.txt" if offset else relative
        with source.open("rb") as stream:
            stream.seek(offset)
            data = stream.read(DIAGNOSTIC_TAIL_BYTES)
        atomic_write_bytes(destination / target, data)
        index.append({"source": relative, "artifact": target, "original_size": size, "truncated": bool(offset)})
    atomic_write_json(destination / "index.json", index)
    result = {"artifacts": snapshot_results(root / "results", destination)}
    atomic_write_json(root / "diagnostics.json", result)
    return result


def snapshot_results(root, directory):
    root, directory = Path(root).resolve(), Path(directory)
    if not directory.exists():
        return []
    result, total = [], 0
    for path in sorted(directory.rglob("*")):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise BenchmarkError("Distributed artifacts must not contain symlinks")
        if not path.is_file():
            continue
        size = path.stat().st_size
        total += size
        if size > MAX_RESULT_FILE_BYTES or total > MAX_RESULT_BYTES or len(result) >= 1000:
            raise BenchmarkError("Distributed workload artifacts exceed the transfer limit")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(RESULT_CHUNK_BYTES), b""):
                digest.update(chunk)
        result.append({"path": path.relative_to(root).as_posix(), "size": size, "sha256": digest.hexdigest()})
    return result


def copy_results(call, reference, job_id, artifacts, prefix, directory, operation="read-result"):
    prefix = prefix.rstrip("/") + "/"
    if not isinstance(artifacts, list) or len(artifacts) > 1000:
        raise BenchmarkError("Invalid distributed artifact list")
    total, seen = 0, set()
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise BenchmarkError("Invalid distributed artifact descriptor")
        path, size, digest = artifact.get("path"), artifact.get("size"), artifact.get("sha256")
        if (
            not isinstance(path, str)
            or not path.startswith(prefix)
            or path in seen
            or type(size) is not int
            or not 0 <= size <= MAX_RESULT_FILE_BYTES
        ):
            raise BenchmarkError("Invalid distributed artifact descriptor")
        seen.add(path)
        total += size
        if total > MAX_RESULT_BYTES:
            raise BenchmarkError("Distributed workload artifacts exceed the transfer limit")
        relative = path[len(prefix) :].split("/")
        if any(part in ("", ".", "..") or "\\" in part or "\0" in part for part in relative):
            raise BenchmarkError("Distributed artifact path escapes its destination")
        destination = Path(directory).joinpath(*relative)
        if not destination.resolve().is_relative_to(Path(directory).resolve()):
            raise BenchmarkError("Distributed artifact destination contains an escaping symlink")
        content = bytearray()
        while len(content) < size:
            response = call(operation, {**reference, "job_id": job_id, "path": path, "offset": len(content)})
            try:
                chunk = base64.b64decode(response["data"], validate=True)
            except (ValueError, KeyError, TypeError) as error:
                raise BenchmarkError("Invalid distributed artifact chunk") from error
            if not chunk or len(chunk) > RESULT_CHUNK_BYTES or len(content) + len(chunk) > size:
                raise BenchmarkError("Distributed artifact changed or was truncated")
            content.extend(chunk)
        if hashlib.sha256(content).hexdigest() != digest:
            raise BenchmarkError("Distributed artifact checksum mismatch")
        atomic_write_bytes(destination, content)
