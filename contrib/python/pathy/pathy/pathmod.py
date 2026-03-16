import os
import posixpath
from typing import Tuple

sep = "/"
schemesep = "://"
altsep = None
join = posixpath.join
normcase = posixpath.normcase
splitdrive = posixpath.splitdrive
dirname = posixpath.dirname


def isabs(s: str) -> bool:
    """Test whether a path is absolute"""
    s = os.fspath(s)
    scheme_tail = s.split(schemesep, 1)
    return len(scheme_tail) == 2


def splitroot(input_path: str, resolve: bool = False) -> Tuple[str, str, str]:
    """Split a pathname into scheme, bucket, and path. For cloud storage all three
    are required. The scheme is one Pathy's supported scehemes, e.g. 'gs', 's3', etc.

        splitroot('gs://bucket/foo/bar') == ('gs', 'bucket', 'foo/bar')
        splitroot('s3://bucket3/bar') == ('s3', 'bucket3', 'bar')
        splitroot('azure://cool/foo/bar') == ('azure', 'cool', 'foo/bar')
    """
    p = os.fspath(input_path)
    empty = ""

    scheme_tail = p.split(schemesep, 1)
    if len(scheme_tail) == 1:
        return empty, empty, p

    scheme, tail = scheme_tail
    parts = tail.split(sep)
    if resolve:
        # Remove any .. parts
        for part in parts[1:]:
            if part == "..":
                index = parts.index(part)
                parts.pop(index - 1)
                parts.remove(part)
    bucket = parts[0]
    path = sep.join(parts[1:]) if len(parts) > 1 else empty
    return scheme, bucket, path
