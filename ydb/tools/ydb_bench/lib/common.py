import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path


class BenchmarkError(RuntimeError):
    pass


class BenchmarkInterrupted(BenchmarkError):
    pass


@dataclass(frozen=True)
class BinaryArtifact:
    path: Path
    sha256: str
    size: int
    source_path: str = ""

    def manifest_record(self):
        record = {"name": self.path.name, "sha256": self.sha256, "size": self.size}
        if self.source_path:
            record["source_path"] = self.source_path
        return record


def atomic_write_bytes(path, data, mode=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if mode is not None:
            os.chmod(temporary_path, mode)
        os.replace(temporary_path, path)
    except BaseException:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def atomic_write_text(path, text):
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path, value):
    try:
        text = json.dumps(value, allow_nan=False, indent=2, sort_keys=True) + "\n"
    except (TypeError, ValueError) as error:
        raise BenchmarkError("cannot serialize JSON with only finite values") from error
    atomic_write_text(path, text)


def atomic_copy_file(source, destination, mode=None):
    source = Path(source)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix="." + destination.name + ".", dir=str(destination.parent))
    temporary_path = Path(temporary_name)
    try:
        with source.open("rb") as input_stream, os.fdopen(fd, "wb") as output_stream:
            shutil.copyfileobj(input_stream, output_stream)
            output_stream.flush()
            os.fsync(output_stream.fileno())
        if mode is not None:
            os.chmod(temporary_path, mode)
        os.replace(temporary_path, destination)
    except BaseException:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def extract_executable(data, directory, name):
    if not data:
        raise BenchmarkError("bundled executable {!r} is empty".format(name))

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / name
    atomic_write_bytes(destination, data, mode=0o755)
    return BinaryArtifact(
        path=destination,
        sha256=hashlib.sha256(data).hexdigest(),
        size=len(data),
    )


def copy_executable(source, directory, name):
    source = Path(source)
    destination = Path(directory) / name
    try:
        if not source.is_file() or not os.access(source, os.R_OK | os.X_OK):
            raise BenchmarkError("external executable {!s} must be a readable executable file".format(source))
        atomic_copy_file(source, destination, mode=0o755)
        digest = hashlib.sha256()
        size = 0
        with destination.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
        if not size:
            raise BenchmarkError("external executable {!s} is empty".format(source))
        return BinaryArtifact(destination, digest.hexdigest(), size, str(source))
    except OSError as error:
        raise BenchmarkError("cannot prepare external executable {!s}: {}".format(source, error)) from error


def load_profile_binaries(configuration, resource_loader, directory, cache):
    profile = configuration.parameters.get("local_ydb", {})
    result = {}
    for name in configuration.benchmark.resources:
        source = profile.get("ydbd_binary") if name == "ydbd" else None
        key = (name, source)
        if key not in cache:
            if source:
                identity = hashlib.sha256(source.encode("utf-8")).hexdigest()
                cache[key] = copy_executable(source, Path(directory) / "external" / identity, name)
            else:
                cache[key] = extract_executable(resource_loader(name), directory, name)
        result[name] = cache[key]
    return result


def binary_catalog(directory, limit=1000):
    root = Path(directory).resolve()
    result = {"root": str(root), "ydbd": [], "truncated": False}
    try:
        with os.scandir(root / "ydbd") as entries:
            for index, entry in enumerate(entries):
                if index >= limit:
                    result["truncated"] = True
                    break
                try:
                    if (
                        entry.name.startswith(".")
                        or not entry.is_file()
                        or not os.access(entry.path, os.R_OK | os.X_OK)
                    ):
                        continue
                    size = entry.stat().st_size
                    if size:
                        result["ydbd"].append({"version": entry.name, "path": entry.path, "size": size})
                except OSError:
                    continue
    except FileNotFoundError:
        pass
    except OSError as error:
        result["error"] = "Cannot read binary catalog: {}".format(error)
    result["ydbd"].sort(key=lambda item: item["version"])
    return result
