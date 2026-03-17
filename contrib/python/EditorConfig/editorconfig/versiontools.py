"""EditorConfig version tools

Provides ``join_version`` and ``split_version`` classes for converting
__version__ strings to VERSION tuples and vice versa.

"""

import re
from typing import Optional


__all__ = ['join_version', 'split_version']


_version_re = re.compile(r'^(\d+)\.(\d+)\.(\d+)(\..*)?$', re.VERBOSE)

VersionTuple = tuple[int, int, int, str]

def join_version(version_tuple: VersionTuple) -> str:
    """Return a string representation of version from given VERSION tuple"""
    version = "%s.%s.%s" % version_tuple[:3]
    if version_tuple[3] != "final":
        version += "-%s" % version_tuple[3]
    return version


def split_version(version: str) -> Optional[VersionTuple]:
    """Return VERSION tuple for given string representation of version"""
    match = _version_re.search(version)
    if not match:
        return None
    else:
        split_version = list(match.groups())
        if split_version[3] is None:
            split_version[3] = "final"
        return (int(split_version[0]), int(split_version[1]), int(split_version[2]), split_version[3])
