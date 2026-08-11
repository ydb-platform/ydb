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
from pathlib import Path, PurePosixPath

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.results import load_manifest


MAX_FILES = 512
MAX_MEMBER_SIZE = 64 * 1024 * 1024
MAX_TOTAL_SIZE = 256 * 1024 * 1024
IMPORT_MANIFEST = "import.json"


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
        if path in expected or not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest) or not isinstance(size, int) or size < 0 or size > MAX_MEMBER_SIZE:
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
                run_path = Path(check) / "run.json"; run_path.write_bytes(archive.read(members["run.json"]))
                load_manifest(run_path)
            root = Path(output).resolve(); root.mkdir(parents=True, exist_ok=True)
            destination = root / "imports" / ("import-" + uuid.uuid4().hex)
            destination.parent.mkdir(exist_ok=True)
            if destination.exists(): raise BenchmarkError("import destination collision")
            staging = Path(tempfile.mkdtemp(prefix=".import-", dir=str(destination.parent)))
            try:
                for name in expected:
                    target = staging / _safe_name(name); target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(members[name]) as source, target.open("xb") as sink:
                        shutil.copyfileobj(source, sink)
                (staging / ".imported").write_text("portable-format-v1\n", encoding="ascii")
                for path in staging.rglob("*"):
                    if path.is_file(): path.chmod(0o444)
                for path in sorted((p for p in staging.rglob("*") if p.is_dir()), reverse=True):
                    path.chmod(0o555)
                staging.chmod(0o555)
                os.replace(staging, destination)
            except Exception:
                shutil.rmtree(staging, ignore_errors=True)
                raise
    except zipfile.BadZipFile as error:
        raise BenchmarkError("invalid import archive") from error
    return {"id": str(destination.relative_to(Path(output).resolve())), "source": "imported"}
