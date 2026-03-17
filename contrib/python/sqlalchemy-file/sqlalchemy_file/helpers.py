import json
import mimetypes
import os
import re
from builtins import RuntimeError
from tempfile import SpooledTemporaryFile
from typing import Any, Dict, List, Union

INMEMORY_FILESIZE = 1024 * 1024
LOCAL_STORAGE_DRIVER_NAME = "Local Storage"


def get_metadata_file_obj(metadata: Dict[str, Any]) -> "SpooledTemporaryFile[bytes]":
    f = SpooledTemporaryFile(INMEMORY_FILESIZE)
    f.write(json.dumps(metadata).encode())
    f.seek(0)
    return f


def get_content_from_file_obj(fileobj: Any) -> Any:
    """Provides a real file object from file content.

    Converts ``str`` and ``bytes`` to an actual file.
    """
    if isinstance(fileobj, (str, bytes)):
        f = SpooledTemporaryFile(INMEMORY_FILESIZE)
        f.write(fileobj.encode() if isinstance(fileobj, str) else fileobj)
        f.seek(0)
        return f
    if getattr(fileobj, "file", None) is not None:
        return fileobj.file
    return fileobj


def get_filename_from_fileob(fileobj: Any) -> Any:
    if getattr(fileobj, "filename", None) is not None:
        return fileobj.filename
    if getattr(fileobj, "name", None) is not None:
        return os.path.basename(fileobj.name)
    return "unnamed"


def get_content_type_from_fileobj(fileobj: Any, filename: str) -> Any:
    if getattr(fileobj, "content_type", None) is not None:
        return fileobj.content_type
    return mimetypes.guess_type(filename, strict=False)[0] or "application/octet-stream"


def get_content_size_from_fileobj(file: Any) -> Any:
    if hasattr(file, "size"):
        return file.size
    if hasattr(file, "name"):
        try:
            return os.path.getsize(file.name)
        except (OSError, TypeError):
            pass
    if hasattr(file, "tell") and hasattr(file, "seek"):
        pos = file.tell()
        file.seek(0, os.SEEK_END)
        size = file.tell()
        file.seek(pos)
        return size
    raise RuntimeError("Unable to determine the file's size.")  # pragma: no cover


def convert_size(size: Union[str, int]) -> int:
    # convert size to number of bytes ex: 1k -> 1000; 1Ki->1024
    if isinstance(size, str):
        pattern = re.compile(r"^(\d+)\s*(k|([KM]i?))$")
        m = re.fullmatch(pattern, size)
        if m is None:
            raise ValueError("Invalid size %s" % size)
        value, si, _ = m.groups()
        si_map = {"k": 1000, "K": 1000, "M": 1000**2, "Ki": 1024, "Mi": 1024**2}
        return int(value) * si_map[si]
    return size


def flatmap(lists: List[List[Any]]) -> List[Any]:
    """Flattens a list of lists into a single list."""
    return [value for _list in lists for value in _list]
