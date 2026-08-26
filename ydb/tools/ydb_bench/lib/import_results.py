"""Safe import of portable benchmark result archives.

The portable format is a ZIP containing ``import.json``, ``run.json`` and the
artifacts named by ``import.json.files``.  The import manifest is intentionally
small and explicit: ``format_version`` is 1 and every member (including
``run.json``) has an exact relative path, byte size, and SHA-256 digest.
"""

import hashlib
import io
import json
import os
import shutil
import stat
import tempfile
import uuid
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.results import TERMINAL_STATES, load_manifest

MAX_FILES = 512
MAX_MEMBER_SIZE = 64 * 1024 * 1024
MAX_TOTAL_SIZE = 256 * 1024 * 1024
IMPORT_MANIFEST = "import.json"
_COPY_CHUNK_SIZE = 1024 * 1024


def _manifest_error(message):
    raise BenchmarkError("malformed portable run manifest: {}".format(message))


def _nonempty_string(value, field):
    if not isinstance(value, str) or not value:
        _manifest_error("{} must be a non-empty string".format(field))


def _timestamp(value, field):
    _nonempty_string(value, field)
    try:
        datetime.fromisoformat(value)
    except ValueError:
        _manifest_error("{} must be an ISO-8601 timestamp".format(field))


def _portable_path(value, field, files):
    _nonempty_string(value, field)
    try:
        path = _safe_name(value)
    except BenchmarkError:
        _manifest_error("{} is not a safe relative path".format(field))
    normalized = path.as_posix()
    if normalized not in files:
        _manifest_error("{} does not name a file in the archive: {}".format(field, value))
    return normalized


def _integer(value, field, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _manifest_error("{} must be an integer greater than or equal to {}".format(field, minimum))


def _cpu_list(value, field):
    if not isinstance(value, list):
        _manifest_error("{} must be a list".format(field))
    for index, cpu in enumerate(value):
        _integer(cpu, "{}[{}]".format(field, index))


def _validate_topology(value):
    if not isinstance(value, dict):
        _manifest_error("topology must be an object")
    required = (
        "version",
        "allowed_cpus",
        "numa_nodes",
        "chiplets",
        "physical_cores",
        "smt_siblings",
        "hierarchy_reasons",
    )
    missing = [field for field in required if field not in value]
    if missing:
        _manifest_error("topology is missing {}".format(", ".join(missing)))
    _integer(value["version"], "topology.version", 1)
    _cpu_list(value["allowed_cpus"], "topology.allowed_cpus")
    for collection, id_field in (("numa_nodes", "id"), ("chiplets", "numa_node")):
        if not isinstance(value[collection], list):
            _manifest_error("topology.{} must be a list".format(collection))
        for index, item in enumerate(value[collection]):
            if not isinstance(item, dict) or id_field not in item or "cpus" not in item:
                _manifest_error("topology.{}[{}] is malformed".format(collection, index))
            _integer(item[id_field], "topology.{}[{}].{}".format(collection, index, id_field))
            _cpu_list(item["cpus"], "topology.{}[{}].cpus".format(collection, index))
    for collection in ("physical_cores", "smt_siblings"):
        if not isinstance(value[collection], list):
            _manifest_error("topology.{} must be a list".format(collection))
        for index, cpus in enumerate(value[collection]):
            _cpu_list(cpus, "topology.{}[{}]".format(collection, index))
    if not isinstance(value["hierarchy_reasons"], list):
        _manifest_error("topology.hierarchy_reasons must be a list")
    for index, reason in enumerate(value["hierarchy_reasons"]):
        if not isinstance(reason, dict):
            _manifest_error("topology.hierarchy_reasons[{}] must be an object".format(index))
        _nonempty_string(reason.get("level"), "topology.hierarchy_reasons[{}].level".format(index))
        _nonempty_string(reason.get("reason"), "topology.hierarchy_reasons[{}].reason".format(index))


def _validate_portable_run_manifest(manifest, files):
    """Validate the top-level run contract before installing immutable data.

    Profile manifests deliberately have a different schema and continue to be
    accepted by ``load_manifest``.  Portable archives, however, always name a
    top-level run and must be safe to expose through the read model.
    """
    required = ("status", "state", "started_at", "finished_at", "runs", "steps", "topology")
    missing = [field for field in required if field not in manifest]
    if missing:
        _manifest_error("missing {}".format(", ".join(missing)))

    state, status = manifest["state"], manifest["status"]
    compatible_statuses = {
        "passed": frozenset(("completed",)),
        "failed": frozenset(("failed",)),
        "cancelled": frozenset(("cancelled", "interrupted")),
        "unsupported": frozenset(("completed", "unsupported")),
    }
    if state not in TERMINAL_STATES:
        _manifest_error("state must be terminal")
    if status not in compatible_statuses[state]:
        _manifest_error("status {} is inconsistent with state {}".format(status, state))
    _timestamp(manifest["started_at"], "started_at")
    _timestamp(manifest["finished_at"], "finished_at")
    _validate_topology(manifest["topology"])
    if not isinstance(manifest["runs"], list):
        _manifest_error("runs must be a list")
    if not isinstance(manifest["steps"], list):
        _manifest_error("steps must be a list")
    if "config" in manifest and not isinstance(manifest["config"], dict):
        _manifest_error("config must be an object")
    if "events" in manifest:
        _integer(manifest["events"], "events")

    terminal_run_statuses = frozenset(("completed", "failed", "cancelled", "interrupted", "unsupported"))
    for index, record in enumerate(manifest["runs"]):
        prefix = "runs[{}]".format(index)
        if not isinstance(record, dict):
            _manifest_error("{} must be an object".format(prefix))
        _nonempty_string(record.get("benchmark"), prefix + ".benchmark")
        _nonempty_string(record.get("profile"), prefix + ".profile")
        if record.get("status") not in terminal_run_statuses:
            _manifest_error("{}.status must be terminal".format(prefix))
        for field in ("manifest", "summary"):
            if field in record:
                _portable_path(record[field], prefix + "." + field, files)
        if "directory" in record:
            _nonempty_string(record["directory"], prefix + ".directory")
            try:
                _safe_name(record["directory"])
            except BenchmarkError:
                _manifest_error("{}.directory is not a safe relative path".format(prefix))

    step_ids = set()
    for index, step in enumerate(manifest["steps"]):
        prefix = "steps[{}]".format(index)
        if not isinstance(step, dict):
            _manifest_error("{} must be an object".format(prefix))
        for field in ("id", "benchmark", "profile", "affinity"):
            _nonempty_string(step.get(field), prefix + "." + field)
        if step["id"] in step_ids:
            _manifest_error("duplicate step id {}".format(step["id"]))
        step_ids.add(step["id"])
        _integer(step.get("threads"), prefix + ".threads", 1)
        _integer(step.get("repeat"), prefix + ".repeat", 1)
        # ``case`` is the one-based, stable index of the expanded parameter
        # combination.  The actual values from that combination are recorded
        # separately in ``parameters``.
        _integer(step.get("case"), prefix + ".case", 1)
        if not isinstance(step.get("parameters"), dict):
            _manifest_error("{}.parameters must be an object".format(prefix))
        if step.get("state") not in TERMINAL_STATES:
            _manifest_error("{}.state must be terminal".format(prefix))
        artifacts = step.get("artifacts")
        if not isinstance(artifacts, list):
            _manifest_error("{}.artifacts must be a list".format(prefix))
        seen_artifacts = set()
        for artifact_index, artifact in enumerate(artifacts):
            normalized = _portable_path(artifact, "{}.artifacts[{}]".format(prefix, artifact_index), files)
            if normalized in seen_artifacts:
                _manifest_error("{}.artifacts contains a duplicate path".format(prefix))
            seen_artifacts.add(normalized)

    return manifest


def _safe_name(name):
    path = PurePosixPath(name)
    if not name or path.is_absolute() or "\\" in name or any(part in ("", ".", "..") for part in path.parts):
        raise BenchmarkError("unsafe import member path: {}".format(name))
    return path


def _read_import_manifest(data):
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise BenchmarkError("malformed import manifest") from error
    if not isinstance(value, dict) or value.get("format_version") != 1 or not isinstance(value.get("files"), list):
        raise BenchmarkError("unsupported or malformed import manifest")
    files = value["files"]
    if not files or len(files) > MAX_FILES:
        raise BenchmarkError("import manifest has invalid file count")
    expected = {}
    for item in files:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "size"}:
            raise BenchmarkError("malformed import manifest entry")
        path, digest, size = item["path"], item["sha256"], item["size"]
        _safe_name(path)
        if (
            path in expected
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)
            or not isinstance(size, int)
            or size < 0
            or size > MAX_MEMBER_SIZE
        ):
            raise BenchmarkError("malformed import manifest entry")
        expected[path] = (digest, size)
    if "run.json" not in expected:
        raise BenchmarkError("import manifest does not list run.json")
    return expected


def _archive_members(archive):
    infos = archive.infolist()
    if len(infos) > MAX_FILES + 1:
        raise BenchmarkError("import archive exceeds file-count limit")
    members = {}
    total = 0
    for info in infos:
        _safe_name(info.filename)
        # ZIP's Unix mode exposes symlinks and special files; do not extract any.
        mode = info.external_attr >> 16
        file_type = stat.S_IFMT(mode)
        if info.is_dir() or (file_type and file_type != stat.S_IFREG):
            raise BenchmarkError("import archive contains an unexpected member type")
        if info.filename in members or info.file_size > MAX_MEMBER_SIZE:
            raise BenchmarkError("import archive has duplicate or oversized member")
        total += info.file_size
        if total > MAX_TOTAL_SIZE:
            raise BenchmarkError("import archive exceeds size limit")
        members[info.filename] = info
    if IMPORT_MANIFEST not in members:
        raise BenchmarkError("import archive is missing import.json")
    return members


def import_archive(output, archive_data):
    """Validate then atomically install an immutable portable ZIP result."""
    if len(archive_data) > MAX_TOTAL_SIZE:
        raise BenchmarkError("import archive exceeds size limit")
    try:
        with zipfile.ZipFile(io.BytesIO(archive_data)) as archive:
            members = _archive_members(archive)
            expected = _read_import_manifest(archive.read(members[IMPORT_MANIFEST]))
            if set(members) != set(expected) | {IMPORT_MANIFEST}:
                raise BenchmarkError("import archive members do not match manifest")
            for name, (digest, size) in expected.items():
                info = members.get(name)
                if info is None or info.file_size != size:
                    raise BenchmarkError("import manifest size mismatch: {}".format(name))
                data = archive.read(info)
                if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                    raise BenchmarkError("import manifest hash mismatch: {}".format(name))
            # Validate compatibility before allocating a durable destination.
            with tempfile.TemporaryDirectory(prefix="ydb-bench-import-check-") as check:
                run_path = Path(check) / "run.json"
                run_path.write_bytes(archive.read(members["run.json"]))
                manifest = load_manifest(run_path)
                _validate_portable_run_manifest(manifest, set(expected))
            root = Path(output).resolve()
            root.mkdir(parents=True, exist_ok=True)
            destination = root / "imports" / ("import-" + uuid.uuid4().hex)
            destination.parent.mkdir(exist_ok=True)
            if destination.exists():
                raise BenchmarkError("import destination collision")
            staging = Path(tempfile.mkdtemp(prefix=".import-", dir=str(destination.parent)))
            try:
                for name in expected:
                    target = staging / _safe_name(name)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(members[name]) as source, target.open("xb") as sink:
                        shutil.copyfileobj(source, sink)
                (staging / ".imported").write_text("portable-format-v1\n", encoding="ascii")
                for path in staging.rglob("*"):
                    if path.is_file():
                        path.chmod(0o444)
                for path in sorted((p for p in staging.rglob("*") if p.is_dir()), reverse=True):
                    path.chmod(0o555)
                staging.chmod(0o555)
                os.replace(staging, destination)
            except Exception:
                _remove_staging(staging)
                raise
    except zipfile.BadZipFile as error:
        raise BenchmarkError("invalid import archive") from error
    return {"id": str(destination.relative_to(Path(output).resolve())), "source": "imported"}


def _remove_staging(staging):
    """Make an immutable staging tree removable after a failed install."""
    if not staging.exists():
        return
    for path in staging.rglob("*"):
        try:
            path.chmod(0o700 if path.is_dir() else 0o600)
        except OSError:
            pass
    try:
        staging.chmod(0o700)
    except OSError:
        pass
    shutil.rmtree(staging)


@contextmanager
def export_archive(run_directory):
    """Create a portable, self-describing result archive for one completed run.

    Export deliberately uses the same strict format accepted by ``import_archive``.
    That keeps a downloaded archive useful on another host and, more importantly,
    avoids giving the HTTP layer a looser archive format than the importer.
    """
    root = Path(run_directory).resolve()
    if not root.is_dir():
        raise BenchmarkError("result directory does not exist: {}".format(root))
    load_manifest(root / "run.json")
    files = []
    total = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink() or path.name == ".imported":
            continue
        relative = path.relative_to(root).as_posix()
        # An imported archive never carries its own import manifest.  A fresh
        # one is produced below so a re-export is deterministic and valid.
        if relative == IMPORT_MANIFEST:
            continue
        size = path.stat().st_size
        if size > MAX_MEMBER_SIZE:
            raise BenchmarkError("result artifact exceeds export size limit: {}".format(relative))
        total += size
        if total > MAX_TOTAL_SIZE:
            raise BenchmarkError("result archive exceeds export size limit")
        files.append((relative, path, size))
    if not any(relative == "run.json" for relative, _, _ in files):
        raise BenchmarkError("result archive is missing run.json")
    if not files or len(files) > MAX_FILES:
        raise BenchmarkError("result archive has invalid file count")
    temporary = tempfile.NamedTemporaryFile(prefix="ydb-bench-export-", suffix=".zip", delete=False)
    destination = Path(temporary.name)
    temporary.close()
    try:
        entries = []
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for relative, path, expected_size in files:
                digest = hashlib.sha256()
                actual_size = 0
                with path.open("rb") as source, archive.open(relative, "w", force_zip64=True) as sink:
                    while True:
                        chunk = source.read(_COPY_CHUNK_SIZE)
                        if not chunk:
                            break
                        actual_size += len(chunk)
                        if actual_size > expected_size or actual_size > MAX_MEMBER_SIZE:
                            raise BenchmarkError("result artifact changed during export: {}".format(relative))
                        digest.update(chunk)
                        sink.write(chunk)
                if actual_size != expected_size:
                    raise BenchmarkError("result artifact changed during export: {}".format(relative))
                entries.append({"path": relative, "sha256": digest.hexdigest(), "size": actual_size})
            manifest = {"format_version": 1, "files": entries}
            archive.writestr(IMPORT_MANIFEST, json.dumps(manifest, sort_keys=True, separators=(",", ":")))
        yield destination
    finally:
        destination.unlink(missing_ok=True)
