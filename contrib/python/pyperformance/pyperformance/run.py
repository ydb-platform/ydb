import hashlib
import json
import os
import sys
import time
import traceback
from collections import namedtuple

import pyperformance

from . import _python, _pythoninfo, _utils, _venv
from .venv import REQUIREMENTS_FILE, VenvForBenchmarks


class BenchmarkException(Exception):
    pass


class RunID(namedtuple("RunID", "python compat bench timestamp")):
    def __new__(cls, python, compat, bench, timestamp):
        self = super().__new__(
            cls,
            python,
            compat,
            bench or None,
            int(timestamp) if timestamp else None,
        )
        return self

    def __str__(self):
        if not self.timestamp:
            return self.name
        return f"{self.name}-{self.timestamp}"

    @property
    def name(self):
        try:
            return self._name
        except AttributeError:
            name = f"{self.python}-compat-{self.compat}"
            if self.bench:
                name = f"{name}-bm-{self.bench.name}"
            self._name = name
            return self._name


def get_run_id(python, bench=None):
    py_id = _python.get_id(python, prefix=True)
    compat_id = get_compatibility_id(bench)
    ts = time.time()
    return RunID(py_id, compat_id, bench, ts)


def get_loops_from_file(filename):
    with open(filename) as fd:
        data = json.load(fd)

    loops = {}
    for benchmark in data["benchmarks"]:
        metadata = benchmark.get("metadata", data["metadata"])
        name = metadata["name"]
        if name.endswith("_none"):
            name = name[: -len("_none")]
        if "loops" in metadata:
            loops[name] = metadata["loops"]

    return loops


def run_benchmarks(should_run, python, options):
    if options.same_loops is not None:
        loops = get_loops_from_file(options.same_loops)
    else:
        loops = {}

    to_run = sorted(should_run)

    info = _pythoninfo.get_info(python)
    runid = get_run_id(info)

    unique = getattr(options, "unique_venvs", False)
    if not unique:
        common = VenvForBenchmarks.ensure(
            _venv.get_venv_root(runid.name, python=info),
            info,
            upgrade="oncreate",
            inherit_environ=options.inherit_environ,
        )

    benchmarks = {}
    venvs = set()
    for i, bench in enumerate(to_run):
        bench_runid = runid._replace(bench=bench)
        assert bench_runid.name, (bench, bench_runid)
        name = bench_runid.name
        venv_root = _venv.get_venv_root(name, python=info)
        print()
        print("=" * 50)
        print(f"({i + 1:>2}/{len(to_run)}) creating venv for benchmark ({bench.name})")
        print()
        if not unique:
            print("(trying common venv first)")
            # Try the common venv first.
            try:
                common.ensure_reqs(bench)
            except _venv.RequirementsInstallationFailedError:
                print("(falling back to unique venv)")
            else:
                benchmarks[bench] = (common, bench_runid)
                continue
        try:
            venv = VenvForBenchmarks.ensure(
                venv_root,
                info,
                upgrade="oncreate",
                inherit_environ=options.inherit_environ,
            )
            # XXX Do not override when there is a requirements collision.
            venv.ensure_reqs(bench)
        except _venv.RequirementsInstallationFailedError:
            print("(benchmark will be skipped)")
            print()
            venv = None
        venvs.add(venv_root)
        benchmarks[bench] = (venv, bench_runid)
    print()

    suite = None
    run_count = str(len(to_run))
    errors = []

    base_pyperf_opts = get_pyperf_opts(options)

    import pyperf

    for index, bench in enumerate(to_run):
        name = bench.name
        print("[%s/%s] %s..." % (str(index + 1).rjust(len(run_count)), run_count, name))
        sys.stdout.flush()

        def add_bench(dest_suite, obj):
            if isinstance(obj, pyperf.BenchmarkSuite):
                results = obj
            else:
                results = (obj,)

            version = pyperformance.__version__
            for res in results:
                res.update_metadata(
                    {"performance_version": version, "tags": bench.tags}
                )

                if dest_suite is not None:
                    dest_suite.add_benchmark(res)
                else:
                    dest_suite = pyperf.BenchmarkSuite([res])

            return dest_suite

        if name in loops:
            pyperf_opts = [*base_pyperf_opts, f"--loops={loops[name]}"]
        else:
            pyperf_opts = base_pyperf_opts

        bench_venv, bench_runid = benchmarks.get(bench)
        if bench_venv is None:
            print("ERROR: Benchmark %s failed: could not install requirements" % name)
            errors.append((name, "Install requirements error"))
            continue
        try:
            result = bench.run(
                bench_venv.python,
                bench_runid,
                pyperf_opts,
                venv=bench_venv,
                verbose=options.verbose,
            )
        except TimeoutError as exc:
            print("ERROR: Benchmark %s timed out" % name)
            errors.append((name, exc))
        except RuntimeError as exc:
            print("ERROR: Benchmark %s failed: %s" % (name, exc))
            traceback.print_exc()
            errors.append((name, exc))
        except Exception as exc:
            print("ERROR: Benchmark %s failed: %s" % (name, exc))
            traceback.print_exc()
            errors.append((name, exc))
        else:
            suite = add_bench(suite, result)

    print()

    return (suite, errors)


# Utility functions


def get_compatibility_id(bench=None):
    # XXX Do not include the pyperformance reqs if a benchmark was provided?
    reqs = sorted(_utils.iter_clean_lines(REQUIREMENTS_FILE))
    if bench:
        lockfile = bench.requirements_lockfile
        if lockfile and os.path.exists(lockfile):
            reqs += sorted(_utils.iter_clean_lines(lockfile))

    data = [
        # XXX Favor pyperf.__version__ instead?
        pyperformance.__version__,
        "\n".join(reqs),
    ]

    h = hashlib.sha256()
    for value in data:
        h.update(value.encode("utf-8"))
    compat_id = h.hexdigest()
    # XXX Return the whole string?
    compat_id = compat_id[:12]

    return compat_id


def get_pyperf_opts(options):
    opts = []

    if options.debug_single_value:
        opts.append("--debug-single-value")
    elif options.rigorous:
        opts.append("--rigorous")
    elif options.fast:
        opts.append("--fast")

    if options.verbose:
        opts.append("--verbose")

    if options.affinity:
        opts.append("--affinity=%s" % options.affinity)
    if options.track_memory:
        opts.append("--track-memory")
    if options.inherit_environ:
        opts.append("--inherit-environ=%s" % ",".join(options.inherit_environ))
    if options.min_time:
        opts.append("--min-time=%s" % options.min_time)
    if options.timeout:
        opts.append("--timeout=%s" % options.timeout)
    if options.hook:
        for hook in options.hook:
            opts.append("--hook=%s" % hook)
    # --warmups=0 is a valid option, so check for `not None` here
    if options.warmups is not None:
        opts.append("--warmups=%s" % options.warmups)

    return opts
