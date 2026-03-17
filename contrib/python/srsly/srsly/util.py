from pathlib import Path
from typing import Union, Dict, Any, List, Tuple
from collections import OrderedDict


# fmt: off
FilePath = Union[str, Path]
# Superficial JSON input/output types
# https://github.com/python/typing/issues/182#issuecomment-186684288
JSONOutput = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
JSONOutputBin = Union[bytes, str, int, float, bool, None, Dict[str, Any], List[Any]]
# For input, we also accept tuples, ordered dicts etc.
JSONInput = Union[str, int, float, bool, None, Dict[str, Any], List[Any], Tuple[Any, ...], OrderedDict]
JSONInputBin = Union[bytes, str, int, float, bool, None, Dict[str, Any], List[Any], Tuple[Any, ...], OrderedDict]
YAMLInput = JSONInput
YAMLOutput = JSONOutput
# fmt: on


def force_path(location, require_exists=True):
    if not isinstance(location, Path):
        location = Path(location)
    if require_exists and not location.exists():
        raise ValueError(f"Can't read file: {location}")
    return location


def force_string(location):
    if isinstance(location, str):
        return location
    return str(location)
