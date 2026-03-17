# Helpers for working with venvs.

import os
import os.path
import sys
import types

from . import _pip, _pythoninfo, _utils


class VenvCreationFailedError(Exception):
    def __init__(self, root, exitcode, already_existed):
        super().__init__(f"venv creation failed ({root})")
        self.root = root
        self.exitcode = exitcode
        self.already_existed = already_existed


class VenvPipInstallFailedError(Exception):
    def __init__(self, root, exitcode, msg=None):
        super().__init__(msg or f"failed to install pip in venv {root}")
        self.root = root
        self.exitcode = exitcode


class RequirementsInstallationFailedError(Exception):
    pass


def read_venv_config(root=None):
    """Return the config for the given venv, from its pyvenv.cfg file."""
    if not root:
        if sys.prefix == sys.base_prefix:
            raise Exception("current Python is not a venv")
        root = sys.prefix
    cfgfile = os.path.join(root, "pyvenv.cfg")
    with open(cfgfile, encoding="utf-8") as infile:
        text = infile.read()
    return parse_venv_config(text, root)


def parse_venv_config(lines, root=None):
    if isinstance(lines, str):
        lines = lines.splitlines()
    else:
        lines = (line.rstrip(os.linesep) for line in lines)

    cfg = types.SimpleNamespace(
        home=None,
        version=None,
        system_site_packages=None,
        prompt=None,
        executable=None,
        command=None,
    )
    fields = set(vars(cfg))
    for line in lines:
        # We do not validate the lines.
        name, sep, value = line.partition("=")
        if not sep:
            continue
        # We do not check for duplicate names.
        name = name.strip().lower()
        if name == "include-system-site-packages":
            name = "system_site_packages"
        if name not in fields:
            # XXX Preserve this anyway?
            continue
        value = value.lstrip()
        if name == "system_site_packages":
            value = value == "true"
        setattr(cfg, name, value)
    return cfg


def resolve_venv_python(root):
    python_exe = "python"
    if sys.executable.endswith(".exe"):
        python_exe += ".exe"
    if os.name == "nt":
        return os.path.join(root, "Scripts", python_exe)
    else:
        return os.path.join(root, "bin", python_exe)


def get_venv_root(name=None, venvsdir="venv", *, python=sys.executable):
    """Return the venv root to use for the given name (or given python)."""
    if not name:
        from .run import get_run_id

        runid = get_run_id(python)
        name = runid.name
    return os.path.abspath(
        os.path.join(venvsdir or ".", name),
    )


def venv_exists(root):
    venv_python = resolve_venv_python(root)
    return os.path.exists(venv_python)


def create_venv(
    root,
    python=sys.executable,
    *,
    env=None,
    downloaddir=None,
    withpip=True,
    cleanonfail=True,
):
    """Create a new venv at the given root, optionally installing pip."""
    already_existed = os.path.exists(root)
    if withpip:
        args = ["-m", "venv", root]
    else:
        args = ["-m", "venv", "--without-pip", root]
    ec, _, _ = _utils.run_python(*args, python=python, env=env)
    if ec != 0:
        if cleanonfail and not already_existed:
            _utils.safe_rmtree(root)
        raise VenvCreationFailedError(root, ec, already_existed)
    return resolve_venv_python(root)


class VirtualEnvironment:
    _env = None

    @classmethod
    def create(cls, root=None, python=sys.executable, **kwargs):
        if not python:
            python = sys.executable
        if isinstance(python, str):
            try:
                info = _pythoninfo.get_info(python)
            except FileNotFoundError:
                info = None
        else:
            info = python
        if not root:
            root = get_venv_root(python=info)

        print("Creating the virtual environment %s" % root)
        if venv_exists(root):
            raise Exception(f"virtual environment {root} already exists")

        try:
            venv_python = create_venv(root, info or python, **kwargs)
        except BaseException:
            _utils.safe_rmtree(root)
            raise  # re-raise
        if not info:
            info = _pythoninfo.get_info(python)
        self = cls(root, base=info)
        self._python = venv_python
        return self

    @classmethod
    def ensure(cls, root, python=None, **kwargs):
        if venv_exists(root):
            return cls(root)
        else:
            return cls.create(root, python, **kwargs)

    def __init__(self, root, *, base=None):
        assert os.path.exists(resolve_venv_python(root)), root
        self.root = root
        if base:
            self._base = base

    @property
    def python(self):
        try:
            return self._python
        except AttributeError:
            if not getattr(self, "_info", None):
                return resolve_venv_python(self.root)
            self._python = self.info.sys.executable
            return self._python

    @property
    def info(self):
        try:
            return self._info
        except AttributeError:
            try:
                python = self._python
            except AttributeError:
                python = resolve_venv_python(self.root)
            self._info = _pythoninfo.get_info(python)
            return self._info

    @property
    def base(self):
        try:
            return self._base
        except AttributeError:
            base_exe = self.info.sys._base_executable
            if not base_exe and base_exe != self.info.sys.executable:
                # XXX Use read_venv_config().
                raise NotImplementedError
                base_exe = ...
            self._base = _pythoninfo.get_info(base_exe)
            return self._base

    def ensure_pip(self, downloaddir=None, *, installer=True, upgrade=True):
        if not upgrade and _pip.is_pip_installed(self.python, env=self._env):
            return
        ec, _, _ = _pip.install_pip(
            self.python,
            info=self.info,
            downloaddir=downloaddir or self.root,
            env=self._env,
            upgrade=upgrade,
        )
        if ec != 0:
            raise VenvPipInstallFailedError(self.root, ec)
        elif not _pip.is_pip_installed(self.python, env=self._env):
            raise VenvPipInstallFailedError(self.root, 0, "pip doesn't work")

        if installer:
            # Upgrade installer dependencies (setuptools, ...)
            ec, _, _ = _pip.ensure_installer(
                self.python,
                env=self._env,
                upgrade=True,
            )
            if ec != 0:
                raise RequirementsInstallationFailedError("wheel")

    def upgrade_pip(self, *, installer=True):
        ec, _, _ = _pip.upgrade_pip(
            self.python,
            info=self.info,
            env=self._env,
            installer=installer,
        )
        if ec != 0:
            raise RequirementsInstallationFailedError("pip")

    def ensure_reqs(self, *reqs, upgrade=True):
        print("Installing requirements into the virtual environment %s" % self.root)
        ec, _, _ = _pip.install_requirements(
            *reqs,
            python=self.python,
            env=self._env,
            upgrade=upgrade,
        )
        if ec:
            raise RequirementsInstallationFailedError(reqs)
