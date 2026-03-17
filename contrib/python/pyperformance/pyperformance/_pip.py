import os
import os.path
import sys

from . import _utils

GET_PIP_URL = "https://bootstrap.pypa.io/get-pip.py"
# pip 6 is the first version supporting environment markers
MIN_PIP = "6.0"
OLD_SETUPTOOLS = "18.5"


def get_pkg_name(req):
    """Return the name of the package in the given requirement text."""
    # strip env markers
    req = req.partition(";")[0]
    # strip version
    req = req.partition("==")[0]
    req = req.partition(">=")[0]
    return req


def run_pip(cmd, *args, **kwargs):
    """Return the result of running pip with the given args."""
    return _utils.run_python("-m", "pip", cmd, *args, **kwargs)


def is_pip_installed(python, *, env=None):
    """Return True if pip is installed on the given Python executable."""
    ec, _, _ = run_pip(
        "--version",
        python=python,
        env=env,
        capture=True,
        verbose=False,
    )
    return ec == 0


def install_pip(
    python=sys.executable,
    *,
    info=None,
    downloaddir=None,
    env=None,
    upgrade=True,
    **kwargs,
):
    """Install pip on the given Python executable."""
    if not python:
        python = getattr(info, "executable", None) or sys.executable

    # python -m ensurepip
    args = ["-m", "ensurepip", "-v"]  # --verbose
    if upgrade:
        args.append("-U")  # --upgrade
    res = _utils.run_python(*args, python=python, **kwargs)
    ec, _, _ = res
    if ec == 0 and is_pip_installed(python, env=env):
        return res

    ##############################
    # Fall back to get-pip.py.

    if not downloaddir:
        downloaddir = "."
    os.makedirs(downloaddir, exist_ok=True)

    # download get-pip.py
    filename = os.path.join(downloaddir, "get-pip.py")
    if not os.path.exists(filename):
        print("Download %s into %s" % (GET_PIP_URL, filename))
        _utils.download(GET_PIP_URL, filename)

    # python get-pip.py
    argv = [python, "-u", filename]
    res = _utils.run_cmd(argv, env=env)
    ec, _, _ = res
    if ec != 0:
        # get-pip.py was maybe not properly downloaded: remove it to
        # download it again next time
        os.unlink(filename)
    return res


def upgrade_pip(
    python=sys.executable,
    *,
    info=None,
    installer=False,
    **kwargs,
):
    """Upgrade pip on the given Python to the latest version."""
    if not python:
        python = getattr(info, "executable", None) or sys.executable

    # pip 6 is the first version supporting environment markers
    reqs = [f"pip>={MIN_PIP}"]
    res = install_requirements(*reqs, python=python, upgrade=True, **kwargs)
    ec, _, _ = res
    if ec != 0:
        return res

    if installer:
        # Upgrade installer dependencies (setuptools, ...)
        res = ensure_installer(python, upgrade=True, **kwargs)
    return res


def ensure_installer(python=sys.executable, **kwargs):
    reqs = [
        f"setuptools>={OLD_SETUPTOOLS}",
        # install wheel so pip can cache binary wheel packages locally,
        # and install prebuilt wheel packages from PyPI.
        "wheel",
    ]
    return install_requirements(*reqs, python=python, **kwargs)


def install_requirements(reqs, *extra, upgrade=True, **kwargs):
    """Install the given packages from PyPI."""
    args = []
    if upgrade:
        args.append("-U")  # --upgrade
    for reqs in [reqs, *extra]:
        if os.path.isfile(reqs) and reqs.endswith(".txt"):
            args.append("-r")  # --requirement
        args.append(reqs)
    return run_pip("install", *args, **kwargs)


def install_editable(projectroot, **kwargs):
    """Install the given project as an "editable" install."""
    return run_pip("install", "-e", projectroot, **kwargs)
