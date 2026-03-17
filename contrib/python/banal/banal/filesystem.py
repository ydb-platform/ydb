import sys
from typing import Optional


def decode_path(file_path: Optional[str]) -> Optional[str]:
    """Turn a path name into unicode."""
    if file_path is None:
        return None
    if isinstance(file_path, bytes):
        file_path = file_path.decode(sys.getfilesystemencoding())
    return file_path
