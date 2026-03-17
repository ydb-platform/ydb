# Generic helpers for working with a Python executable.

import hashlib

from pyperformance import _pythoninfo


def get_id(python=None, prefix=None, *, short=True):
    """Return a string that uniquely identifies the given Python executable."""
    if isinstance(python, str):
        python = _pythoninfo.get_info(python)

    data = [
        # "executable" represents the install location
        # (and build, to an extent).
        python.sys.executable,
        # sys.version encodes version, git info, build_date, and build_tool.
        python.sys.version,
        python.sys.implementation.name.lower(),
        ".".join(str(v) for v in python.sys.implementation.version),
        str(python.sys.api_version),
        python.pyc_magic_number.hex(),
    ]
    # XXX Add git info if a dev build.

    h = hashlib.sha256()
    for value in data:
        h.update(value.encode("utf-8"))
    # XXX Also include the sorted output of "python -m pip freeze"?
    py_id = h.hexdigest()
    if short:
        py_id = py_id[:12]

    if prefix:
        if prefix is True:
            major, minor = python.sys.version_info[:2]
            py_id = f"{python.sys.implementation.name}{major}.{minor}-{py_id}"
        else:
            py_id = prefix + py_id

    return py_id
