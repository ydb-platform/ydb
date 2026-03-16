__all__ = [
    "Benchmark",
    "BenchmarkSpec",
    "check_name",
    "parse_benchmark",
]


import os
import os.path
import sys
from collections import namedtuple

import pyperf
from packaging.specifiers import SpecifierSet

from . import _benchmark_metadata, _utils


def check_name(name):
    _utils.check_name("_" + name)


def parse_benchmark(entry, *, fail=True):
    name = entry
    version = None
    origin = None
    metafile = None

    if not f"_{name}".isidentifier():
        if not fail:
            return None
        raise ValueError(f"unsupported benchmark name in {entry!r}")

    bench = BenchmarkSpec(name, version, origin)
    return bench, metafile


class BenchmarkSpec(namedtuple("BenchmarkSpec", "name version origin")):
    __slots__ = ()

    @classmethod
    def from_raw(cls, raw):
        if isinstance(raw, BenchmarkSpec):
            return raw, None
        elif isinstance(raw, str):
            return parse_benchmark(raw)
        else:
            raise ValueError(f"unsupported raw spec {raw!r}")

    def __new__(cls, name, version=None, origin=None):
        self = super().__new__(cls, name, version or None, origin or None)
        return self


class Benchmark:
    _metadata = None

    def __init__(self, spec, metafile):
        spec, _metafile = BenchmarkSpec.from_raw(spec)
        if not metafile:
            if not _metafile:
                raise ValueError(f"missing metafile for {spec!r}")
            metafile = _metafile

        self.spec = spec
        self.metafile = metafile

    def __repr__(self):
        return f"{type(self).__name__}(spec={self.spec!r}, metafile={self.metafile!r})"

    def __hash__(self):
        return hash(self.spec)

    def __eq__(self, other):
        try:
            other_spec = other.spec
        except AttributeError:
            return NotImplemented
        return self.spec == other_spec

    def __gt__(self, other):
        try:
            other_spec = other.spec
        except AttributeError:
            return NotImplemented
        return self.spec > other_spec

    # __getattr__() gets weird when AttributeError comes out of
    # properties so we spell out all the aliased attributes.

    @property
    def name(self):
        return self.spec.name

    @property
    def version(self):
        version = self.spec.version
        if version is None:
            version = self._get_metadata_value("version", None)
        return version

    @property
    def origin(self):
        return self.spec.origin

    def _get_rootdir(self):
        try:
            return self._rootdir
        except AttributeError:
            script = self.runscript
            self._rootdir = os.path.dirname(script) if script else None
            return self._rootdir

    def _init_metadata(self):
        # assert self._metadata is None
        defaults = {
            "name": self.spec.name,
            "version": self.spec.version,
        }
        self._metadata, _ = _benchmark_metadata.load_metadata(
            self.metafile,
            defaults,
        )

    def _get_metadata_value(self, key, default):
        try:
            return self._metadata[key]
        except TypeError:
            if self._metadata is not None:
                raise  # re-raise
            self._init_metadata()
        except KeyError:
            pass
        return self._metadata.setdefault(key, default)

    @property
    def tags(self):
        return self._get_metadata_value("tags", [])

    @property
    def datadir(self):
        return self._get_metadata_value("datadir", None)

    @property
    def requirements_lockfile(self):
        try:
            return self._lockfile
        except AttributeError:
            lockfile = self._get_metadata_value("requirements_lockfile", None)
            if not lockfile:
                rootdir = self._get_rootdir()
                if rootdir:
                    lockfile = os.path.join(rootdir, "requirements.txt")
            self._lockfile = lockfile
            return self._lockfile

    @property
    def runscript(self):
        return self._get_metadata_value("runscript", None)

    @property
    def extra_opts(self):
        return self._get_metadata_value("extra_opts", ())

    @property
    def python(self):
        return SpecifierSet(self._get_metadata_value("python", ""))

    # Other metadata keys:
    # * base
    # * dependencies
    # * requirements

    def run(
        self,
        python,
        runid=None,
        pyperf_opts=None,
        *,
        venv=None,
        verbose=False,
    ):
        if venv and python == sys.executable:
            python = venv.python

        if not runid:
            from .run import get_run_id

            runid = get_run_id(python, self)

        runscript = self.runscript
        bench = _run_perf_script(
            python,
            runscript,
            runid,
            extra_opts=self.extra_opts,
            pyperf_opts=pyperf_opts,
            verbose=verbose,
        )

        return bench


#######################################
# internal implementation


def _run_perf_script(
    python,
    runscript,
    runid,
    *,
    extra_opts=None,
    pyperf_opts=None,
    verbose=False,
):
    if not runscript:
        raise ValueError("missing runscript")
    if not isinstance(runscript, str):
        raise TypeError(f"runscript must be a string, got {runscript!r}")

    with _utils.temporary_file() as tmp:
        opts = [
            *(extra_opts or ()),
            *(pyperf_opts or ()),
            "--output",
            tmp,
        ]
        if pyperf_opts and "--copy-env" in pyperf_opts:
            argv, env = _prep_cmd(python, runscript, opts, runid, lambda name: None)
        else:
            opts, inherit_envvar = _resolve_restricted_opts(opts)
            argv, env = _prep_cmd(python, runscript, opts, runid, inherit_envvar)
        hide_stderr = not verbose
        ec, _, stderr = _utils.run_cmd(
            argv,
            env=env,
            capture="stderr" if hide_stderr else None,
        )
        if ec != 0:
            if hide_stderr:
                sys.stderr.flush()
                sys.stderr.write(stderr)
                sys.stderr.flush()
            # pyperf returns exit code 124 if the benchmark execution times out
            if ec == 124:
                raise TimeoutError("Benchmark timed out")
            else:
                raise RuntimeError("Benchmark died")
        return pyperf.BenchmarkSuite.load(tmp)


def _prep_cmd(python, script, opts, runid, on_set_envvar=None):
    # Populate the environment variables.
    env = dict(os.environ)

    def set_envvar(name, value):
        env[name] = value
        if on_set_envvar is not None:
            on_set_envvar(name)

    # on_set_envvar() may update "opts" so all calls to set_envvar()
    # must happen before building argv.
    set_envvar("PYPERFORMANCE_RUNID", str(runid))

    # Build argv.
    argv = [
        python,
        "-u",
        script,
        *(opts or ()),
    ]

    return argv, env


def _resolve_restricted_opts(opts):
    # Deal with --inherit-environ.
    FLAG = "--inherit-environ"
    resolved = []
    idx = None
    for i, opt in enumerate(opts):
        if opt.startswith(FLAG + "="):
            idx = i + 1
            resolved.append(FLAG)
            resolved.append(opt.partition("=")[-1])
            resolved.extend(opts[idx:])
            break
        elif opt == FLAG:
            idx = i + 1
            resolved.append(FLAG)
            resolved.append(opts[idx])
            resolved.extend(opts[idx + 1 :])
            break
        else:
            resolved.append(opt)
    else:
        resolved.extend(["--inherit-environ", ""])
        idx = len(resolved) - 1
    inherited = set(resolved[idx].replace(",", " ").split())

    def inherit_env_var(name):
        inherited.add(name)
        resolved[idx] = ",".join(inherited)

    return resolved, inherit_env_var


def _insert_on_PYTHONPATH(entry, env):
    PYTHONPATH = env.get("PYTHONPATH", "").split(os.pathsep)
    PYTHONPATH.insert(0, entry)
    env["PYTHONPATH"] = os.pathsep.join(PYTHONPATH)
