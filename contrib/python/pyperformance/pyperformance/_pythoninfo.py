# A utility library for getting information about a Python executable.
#
# This may be used as a script.

import importlib.util
import json
import os
import os.path
import sys
import sysconfig

INFO = {
    # sys
    "executable (sys)": "sys.executable",
    "executable (sys;realpath)": "executable_realpath",
    "prefix (sys)": "sys.prefix",
    "exec_prefix (sys)": "sys.exec_prefix",
    "stdlib_dir (sys)": "sys._stdlib_dir",
    "base_executable (sys)": "sys._base_executable",
    "base_prefix (sys)": "sys.base_prefix",
    "base_exec_prefix (sys)": "sys.base_exec_prefix",
    "version_str (sys)": "sys.version",
    "version_info (sys)": "sys.version_info",
    "hexversion (sys)": "sys.hexversion",
    "api_version (sys)": "sys.api_version",
    "implementation_name (sys)": "sys.implementation.name",
    "implementation_version (sys)": "sys.implementation.version",
    "platform (sys)": "sys.platform",
    # sysconfig
    "stdlib_dir (sysconfig)": "sysconfig.paths.stdlib",
    "is_dev (sysconfig)": "sysconfig.is_python_build",
    # other
    "base_executable": "base_executable",
    "stdlib_dir": "stdlib_dir",
    "pyc_magic_number": "pyc_magic_number",
    "is_venv": "is_venv",
}


def get_info(python=sys.executable):
    """Return an object with details about the given Python executable.

    Most of the details are grouped by their source.

    By default the current Python is used.
    """
    if python and python != sys.executable:
        # Run _pythoninfo.py to get the raw info.
        import subprocess

        argv = [python, __file__]
        try:
            text = subprocess.check_output(argv, encoding="utf-8")
        except subprocess.CalledProcessError:
            raise Exception(f"could not get info for {python or sys.executable}")
        data = _unjsonify_info(text)
    else:
        data = _get_current_info()
    return _build_info(data)


def _build_info(data):
    # Map the data into a new types.SimpleNamespace object.
    info = type(sys.implementation)()
    for key, value in data.items():
        try:
            field = INFO[key]
        except KeyError:
            raise NotImplementedError(repr(key))
        parent = info
        while "." in field:
            pname, _, field = field.partition(".")
            try:
                parent = getattr(parent, pname)
            except AttributeError:
                setattr(parent, pname, type(sys.implementation)())
                parent = getattr(parent, pname)
        setattr(parent, field, value)
    return info


def _get_current_info():
    is_venv = sys.prefix != sys.base_prefix
    base_executable = getattr(sys, "_base_executable", None)
    if is_venv:
        # XXX There is probably a bug related to venv, since
        # sys._base_executable should be different.
        if base_executable == sys.executable:
            # Indicate that we don't know.
            base_executable = None
    elif not base_executable:
        base_executable = sys.executable
    info = {
        # locations
        "executable (sys)": sys.executable,
        "executable (sys;realpath)": os.path.realpath(sys.executable),
        "prefix (sys)": sys.prefix,
        "exec_prefix (sys)": sys.exec_prefix,
        "stdlib_dir": os.path.dirname(os.__file__),
        "stdlib_dir (sys)": getattr(sys, "_stdlib_dir", None),
        "stdlib_dir (sysconfig)": (
            sysconfig.get_path("stdlib")
            if "stdlib" in sysconfig.get_path_names()
            else None
        ),
        # base locations
        "base_executable": base_executable,
        "base_executable (sys)": getattr(sys, "_base_executable", None),
        "base_prefix (sys)": sys.base_prefix,
        "base_exec_prefix (sys)": sys.base_exec_prefix,
        # version
        "version_str (sys)": sys.version,
        "version_info (sys)": sys.version_info,
        "hexversion (sys)": sys.hexversion,
        "api_version (sys)": sys.api_version,
        # implementation
        "implementation_name (sys)": sys.implementation.name,
        "implementation_version (sys)": sys.implementation.version,
        # build
        "is_dev (sysconfig)": sysconfig.is_python_build(),
        # host
        "platform (sys)": sys.platform,
        # virtual envs
        "is_venv": is_venv,
        # import system
        # importlib.util.MAGIC_NUMBER has been around since 3.5.
        "pyc_magic_number": importlib.util.MAGIC_NUMBER,
    }
    return info


def _jsonify_info(info):
    data = dict(info)
    if isinstance(data["pyc_magic_number"], bytes):
        data["pyc_magic_number"] = data["pyc_magic_number"].hex()
    return data


def _unjsonify_info(data):
    if isinstance(data, str):
        data = json.loads(data)
    info = dict(data)
    for key in ("version_info (sys)", "implementation_version (sys)"):
        if isinstance(info[key], list):
            # We would use type(sys.version_info) if it allowed it.
            info[key] = tuple(info[key])
    for key in ("pyc_magic_number",):
        if isinstance(info[key], str):
            info[key] = bytes.fromhex(data[key])
    return info


#######################################
# use as a script

if __name__ == "__main__":
    info = _get_current_info()
    data = _jsonify_info(info)
    json.dump(data, sys.stdout, indent=4)
    print()
