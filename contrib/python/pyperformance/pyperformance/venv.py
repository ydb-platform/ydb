import os
import os.path
import sys

import pyperformance

from . import _pip, _utils, _venv

REQUIREMENTS_FILE = os.path.join(
    os.path.dirname(__file__), "requirements", "requirements.txt"
)
PYPERF_OPTIONAL = ["psutil"]


class Requirements(object):
    @classmethod
    def from_file(cls, filename):
        self = cls()
        self._add_from_file(filename)
        return self

    @classmethod
    def from_benchmarks(cls, benchmarks):
        self = cls()
        for bench in benchmarks or ():
            filename = bench.requirements_lockfile
            self._add_from_file(filename)
        return self

    def __init__(self):
        # if pip or setuptools is updated:
        # .github/workflows/main.yml should be updated as well

        # requirements
        self.specs = []

    def __len__(self):
        return len(self.specs)

    def __iter__(self):
        for spec in self.specs:
            yield spec

    def _add_from_file(self, filename):
        if not os.path.exists(filename):
            return
        for line in _utils.iter_clean_lines(filename):
            fullpath = os.path.join(os.path.dirname(filename), line.strip())
            if os.path.isfile(fullpath):
                self._add(fullpath)
            else:
                self._add(line)

    def _add(self, line):
        self.specs.append(line)

    def get(self, name):
        for req in self.specs:
            if _pip.get_pkg_name(req) == name:
                return req
        return None


# This is used by the hg_startup benchmark.
def get_venv_program(program):
    bin_path = os.path.dirname(sys.executable)
    bin_path = os.path.realpath(bin_path)

    if not os.path.isabs(bin_path):
        print("ERROR: Python executable path is not absolute: %s" % sys.executable)
        sys.exit(1)

    if not os.path.exists(os.path.join(bin_path, "activate")):
        print(
            "ERROR: Unable to get the virtual environment of "
            "the Python executable %s" % sys.executable
        )
        sys.exit(1)

    if os.name == "nt":
        path = os.path.join(bin_path, program)
    else:
        path = os.path.join(bin_path, program)

    if not os.path.exists(path):
        print(
            "ERROR: Unable to get the program %r "
            "from the virtual environment %r" % (program, bin_path)
        )
        sys.exit(1)

    return path


NECESSARY_ENV_VARS = {
    "nt": [
        "ALLUSERSPROFILE",
        "APPDATA",
        "COMPUTERNAME",
        "ComSpec",
        "CommonProgramFiles",
        "CommonProgramFiles(x86)",
        "CommonProgramW6432",
        "HOMEDRIVE",
        "HOMEPATH",
        "LOCALAPPDATA",
        "NUMBER_OF_PROCESSORS",
        "OS",
        "PATHEXT",
        "PROCESSOR_ARCHITECTURE",
        "PROCESSOR_IDENTIFIER",
        "PROCESSOR_LEVEL",
        "PROCESSOR_REVISION",
        "Path",
        "ProgramData",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramW6432",
        "SystemDrive",
        "SystemRoot",
        "TEMP",
        "TMP",
        "USERDNSDOMAIN",
        "USERDOMAIN",
        "USERDOMAIN_ROAMINGPROFILE",
        "USERNAME",
        "USERPROFILE",
        "windir",
    ],
}
NECESSARY_ENV_VARS_DEFAULT = [
    "HOME",
    "PATH",
]


def _get_envvars(inherit=None, osname=None):
    # Restrict the env we use.
    try:
        necessary = NECESSARY_ENV_VARS[osname or os.name]
    except KeyError:
        necessary = NECESSARY_ENV_VARS_DEFAULT
    copy_env = list(necessary)
    if inherit:
        copy_env.extend(inherit)

    env = {}
    for name in copy_env:
        if name in os.environ:
            env[name] = os.environ[name]
    return env


class VenvForBenchmarks(_venv.VirtualEnvironment):
    @classmethod
    def create(
        cls,
        root=None,
        python=None,
        *,
        inherit_environ=None,
        upgrade=False,
    ):
        env = _get_envvars(inherit_environ)
        self = super().create(root, python, env=env, withpip=False)
        self.inherit_environ = inherit_environ

        try:
            self.ensure_pip(upgrade=upgrade)
        except BaseException:
            _utils.safe_rmtree(self.root)
            raise

        # Display the pip version
        _pip.run_pip("--version", python=self.python, env=self._env)

        return self

    @classmethod
    def ensure(
        cls, root, python=None, *, inherit_environ=None, upgrade=False, **kwargs
    ):
        exists = _venv.venv_exists(root)
        if upgrade == "oncreate":
            upgrade = not exists
        elif upgrade == "onexists":
            upgrade = exists
        elif isinstance(upgrade, str):
            raise NotImplementedError(upgrade)

        if exists:
            self = super().ensure(root)
            self.inherit_environ = inherit_environ
            if upgrade:
                self.upgrade_pip()
            else:
                self.ensure_pip(upgrade=False)
            return self
        else:
            return cls.create(
                root, python, inherit_environ=inherit_environ, upgrade=upgrade, **kwargs
            )

    def __init__(self, root, *, base=None, inherit_environ=None):
        super().__init__(root, base=base)
        self.inherit_environ = inherit_environ or None

    @property
    def _env(self):
        # Restrict the env we use.
        return _get_envvars(self.inherit_environ)

    def install_pyperformance(self):
        print("installing pyperformance in the venv at %s" % self.root)
        # Install pyperformance inside the virtual environment.
        if pyperformance.is_dev():
            basereqs = Requirements.from_file(REQUIREMENTS_FILE)
            self.ensure_reqs(basereqs)
            if basereqs.get("pyperf"):
                self._install_pyperf_optional_dependencies()

            root_dir = os.path.dirname(pyperformance.PKG_ROOT)
            ec, _, _ = _pip.install_editable(
                root_dir,
                python=self.info,
                env=self._env,
            )
            if ec != 0:
                raise _venv.RequirementsInstallationFailedError(root_dir)
        else:
            version = pyperformance.__version__
            self.ensure_reqs([f"pyperformance=={version}"])
            self._install_pyperf_optional_dependencies()

    def _install_pyperf_optional_dependencies(self):
        for req in PYPERF_OPTIONAL:
            try:
                self.ensure_reqs([req])
            except _venv.RequirementsInstallationFailedError:
                print("WARNING: failed to install %s" % req)
                pass

    def ensure_reqs(self, requirements=None):
        # parse requirements
        bench = None
        if requirements is None:
            requirements = Requirements()
        elif hasattr(requirements, "requirements_lockfile"):
            bench = requirements
            requirements = Requirements.from_benchmarks([bench])

        # Every benchmark must depend on pyperf.
        if bench is not None and not requirements.get("pyperf"):
            basereqs = Requirements.from_file(REQUIREMENTS_FILE)
            pyperf_req = basereqs.get("pyperf")
            if not pyperf_req:
                raise NotImplementedError
            requirements.specs.append(pyperf_req)
            # XXX what about psutil?

        if not requirements:
            print("(nothing to install)")
        else:
            # install requirements
            super().ensure_reqs(
                *requirements,
                upgrade=False,
            )

            if bench is not None:
                self._install_pyperf_optional_dependencies()

        # Dump the package list and their versions: pip freeze
        _pip.run_pip("freeze", python=self.python, env=self._env)

        return requirements
