import hashlib
from pathlib import Path
from typing import Iterable, Union

import srsly
from wasabi import msg


def get_hash(data, exclude: Iterable[str] = tuple()) -> str:
    """Get the hash for a JSON-serializable object.

    data: The data to hash.
    exclude (Iterable[str]): Top-level keys to exclude if data is a dict.
    RETURNS (str): The hash.
    """
    if isinstance(data, dict):
        data = {k: v for k, v in data.items() if k not in exclude}
    data_str = srsly.json_dumps(data, sort_keys=True).encode("utf8")
    return hashlib.md5(data_str).hexdigest()


def get_checksum(path: Union[Path, str]) -> str:
    """Get the checksum for a file or directory given its file path. If a
    directory path is provided, this uses all files in that directory.

    path (Union[Path, str]): The file or directory path.
    RETURNS (str): The checksum.
    """
    path = Path(path)
    if not (path.is_file() or path.is_dir()):
        msg.fail(f"Can't get checksum for {path}: not a file or directory", exits=1)
    if path.is_file():
        return hashlib.md5(Path(path).read_bytes()).hexdigest()
    else:
        # TODO: this is currently pretty slow
        dir_checksum = hashlib.md5()
        for sub_file in sorted(fp for fp in path.rglob("*") if fp.is_file()):
            dir_checksum.update(sub_file.read_bytes())
        return dir_checksum.hexdigest()
