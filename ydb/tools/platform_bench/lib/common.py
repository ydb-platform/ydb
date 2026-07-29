import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


class BenchmarkError(RuntimeError):
    pass


@dataclass(frozen=True)
class BinaryArtifact:
    path: Path
    sha256: str
    size: int


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
    atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


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
