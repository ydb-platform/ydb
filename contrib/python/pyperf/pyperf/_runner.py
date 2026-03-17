import functools
import os
import sys
import time

import pyperf
from pyperf._cli import (format_benchmark, format_checks,
                         multiline_output, display_title, format_result_value,
                         catch_broken_pipe_error)
from pyperf._cpu_utils import (format_cpu_list, parse_cpu_list,
                               get_isolated_cpus, set_cpu_affinity,
                               set_highest_priority)
from pyperf._formatter import format_timedelta
from pyperf._hooks import get_hook_names
from pyperf._utils import (MS_WINDOWS, abs_executable,
                           WritePipe, get_python_names,
                           merge_profile_stats)
from pyperf._system import OS_LINUX
from pyperf._worker import WorkerProcessTask


def strictly_positive(value):
    value = int(value)
    if value <= 0:
        raise ValueError("value must be > 0")
    return value


def positive_or_nul(value):
    if '^' in value:
        x, _, y = value.partition('^')
        x = int(x)
        y = int(y)
        value = x ** y
    else:
        value = int(value)
    if value < 0:
        raise ValueError("value must be >= 0")
    return value


def comma_separated(values):
    values = [value.strip() for value in values.split(',')]
    return list(filter(None, values))


def parse_python_names(names):
    parts = names.split(':')
    if len(parts) != 2:
        raise ValueError("syntax is REF_NAME:CHANGED_NAME")
    return parts


def profiling_wrapper(func):
    """
    Wrap a function to collect profiling.
    """
    import cProfile
    profiler = cProfile.Profile()

    def profiling_func(*args):
        return profiler.runcall(func, *args)

    return profiler, profiling_func


class CLIError(Exception):
    pass


class Runner:
    _created = set()

    # Default parameters are chosen to have approximately a run of 0.5 second
    # and so a total duration of 5 seconds by default
    def __init__(self, values=None, processes=None,
                 loops=0, min_time=0.1, metadata=None,
                 show_name=True,
                 program_args=None, add_cmdline_args=None,
                 _argparser=None, warmups=1):

        # Watchdog: ensure that only once instance of Runner (or a Runner
        # subclass) is created per process to prevent bad surprises
        cls = self.__class__
        key = id(cls)
        if key in cls._created:
            raise RuntimeError("only one %s instance must be created "
                               "per process: use the same instance to run "
                               "all benchmarks" % cls.__name__)
        cls._created.add(key)

        # Use lazy import to limit imports on 'import pyperf'
        import argparse

        has_jit = pyperf.python_has_jit()
        if not values:
            if has_jit:
                # Since PyPy JIT has less processes:
                # run more values per process
                values = 10
            else:
                values = 3
        if not processes:
            if has_jit:
                # Use less processes than non-JIT, because JIT requires more
                # warmups and so each worker is slower
                processes = 6
            else:
                processes = 20

        if metadata is not None:
            self.metadata = metadata
        else:
            self.metadata = {}

        # Worker task identifier: count how many times _worker() was called,
        # see the --worker-task command line option
        self._worker_task = 0

        # Set used to check that benchmark names are unique
        self._bench_names = set()

        # result of argparser.parse_args()
        self.args = None

        # callback used to prepare command line arguments to spawn a worker
        # child process. The callback is called with prepare(runner.args, cmd).
        # args must be modified in-place.
        self._add_cmdline_args = add_cmdline_args

        # Command list arguments to call the program: (sys.argv[0],) by
        # default.
        #
        # For example, "python3 -m pyperf timeit" sets program_args to
        # ('-m', 'pyperf', 'timeit').
        if program_args:
            self._program_args = program_args
        else:
            self._program_args = (sys.argv[0],)
        self._show_name = show_name

        if _argparser is not None:
            parser = _argparser
        else:
            parser = argparse.ArgumentParser()
        parser.description = 'Benchmark'
        parser.add_argument('--rigorous', action="store_true",
                            help='Spend longer running tests '
                                 'to get more accurate results')
        parser.add_argument('--fast', action="store_true",
                            help='Get rough answers quickly')
        parser.add_argument("--debug-single-value", action="store_true",
                            help="Debug mode, only compute a single value")
        parser.add_argument('-p', '--processes',
                            type=strictly_positive, default=processes,
                            help='number of processes used to run benchmarks '
                                 '(default: %s)' % processes)
        parser.add_argument('-n', '--values', dest="values",
                            type=strictly_positive, default=values,
                            help='number of values per process (default: %s)'
                                 % values)
        parser.add_argument('-w', '--warmups',
                            type=positive_or_nul, default=warmups,
                            help='number of skipped values per run used '
                                 'to warmup the benchmark')
        parser.add_argument('-l', '--loops',
                            type=positive_or_nul, default=loops,
                            help='number of loops per value, 0 means '
                                 'automatic calibration (default: %s)'
                            % loops)
        parser.add_argument('-v', '--verbose', action="store_true",
                            help='enable verbose mode')
        parser.add_argument('-q', '--quiet', action="store_true",
                            help='enable quiet mode')
        parser.add_argument('--pipe', type=int, metavar="FD",
                            help='Write benchmarks encoded as JSON '
                                 'into the pipe FD')
        parser.add_argument('-o', '--output', metavar='FILENAME',
                            help='write results encoded to JSON into FILENAME')
        parser.add_argument('--append', metavar='FILENAME',
                            help='append results encoded to JSON into FILENAME')
        parser.add_argument('--min-time', type=float, default=min_time,
                            help='Minimum duration in seconds of a single '
                                 'value, used to calibrate the number of '
                                 'loops (default: %s)'
                            % format_timedelta(min_time))
        parser.add_argument('--timeout',
                            help='Specify a timeout in seconds for a single '
                                 'benchmark execution (default: disabled)',
                            type=strictly_positive)
        parser.add_argument('--worker', action='store_true',
                            help='Worker process, run the benchmark.')
        parser.add_argument('--worker-task', type=positive_or_nul, metavar='TASK_ID',
                            help='Identifier of the worker task: '
                                 'only execute the benchmark function TASK_ID')
        parser.add_argument('--calibrate-loops', action="store_true",
                            help="calibrate the number of loops")
        parser.add_argument('--recalibrate-loops', action="store_true",
                            help="recalibrate the the number of loops")
        parser.add_argument('--calibrate-warmups', action="store_true",
                            help="calibrate the number of warmups")
        parser.add_argument('--recalibrate-warmups', action="store_true",
                            help="recalibrate the number of warmups")
        parser.add_argument('-d', '--dump', action="store_true",
                            help='display benchmark run results')
        parser.add_argument('--metadata', '-m', action="store_true",
                            help='show metadata')
        parser.add_argument('--hist', '-g', action="store_true",
                            help='display an histogram of values')
        parser.add_argument('--stats', '-t', action="store_true",
                            help='display statistics (min, max, ...)')
        parser.add_argument("--affinity", metavar="CPU_LIST", default=None,
                            help='Specify CPU affinity for worker processes. '
                                 'This way, benchmarks can be forced to run '
                                 'on a given set of CPUs to minimize run to '
                                 'run variation. By default, worker processes '
                                 'are pinned to isolate CPUs if isolated CPUs '
                                 'are found.')
        parser.add_argument("--inherit-environ", metavar='VARS',
                            type=comma_separated,
                            help='Comma-separated list of environment '
                                 'variables inherited by worker child '
                                 'processes.')
        parser.add_argument("--copy-env",
                            dest="copy_env", action="store_true", default=False,
                            help="Copy all environment variables")
        parser.add_argument("--no-locale",
                            dest="locale", action="store_false", default=True,
                            help="Don't copy locale environment variables "
                                 "like LANG or LC_CTYPE.")
        parser.add_argument("--python", default=sys.executable,
                            help='Python executable '
                                 '(default: use running Python, '
                                 'sys.executable)')
        parser.add_argument("--compare-to", metavar="REF_PYTHON",
                            help='Run benchmark on the Python executable REF_PYTHON, '
                                 'run benchmark on Python executable PYTHON, '
                                 'and then compare REF_PYTHON result to PYTHON result')
        parser.add_argument("--python-names", metavar="REF_NAME:CHANGED_NAMED",
                            type=parse_python_names,
                            help='option used with --compare-to to name '
                                 'PYTHON as CHANGED_NAME '
                                 'and REF_PYTHON as REF_NAME in results')

        parser.add_argument('--profile',
                            type=str,
                            help='Collect profile data using cProfile '
                                 'and output to the given file.')

        hook_names = list(get_hook_names())
        parser.add_argument(
            '--hook', action="append", choices=hook_names,
            metavar=f"{', '.join(x for x in hook_names if not x.startswith('_'))}",
            help='Use the given pyperf hooks'
        )

        memory = parser.add_mutually_exclusive_group()
        memory.add_argument('--tracemalloc', action="store_true",
                            help='Trace memory allocations using tracemalloc')
        memory.add_argument('--track-memory', action="store_true",
                            help='Track memory usage using a thread')

        self.argparser = parser

    def _multiline_output(self):
        return self.args.verbose or multiline_output(self.args)

    def _only_in_worker(self, option):
        if not self.args.worker:
            raise CLIError("option %s requires --worker" % option)

    def _process_args_impl(self):
        args = self.args

        if args.pipe:
            args.quiet = True
            args.verbose = False
        elif args.quiet:
            args.verbose = False

        has_jit = pyperf.python_has_jit()
        if args.warmups is None and not args.worker and not has_jit:
            args.warmups = 1

        nprocess = self.argparser.get_default('processes')
        nvalues = self.argparser.get_default('values')
        if args.rigorous:
            args.processes = nprocess * 2
            # args.values = nvalues * 5 // 3
        elif args.fast:
            # use at least 3 processes to benchmark 3 different (randomized)
            # hash functions
            args.processes = max(nprocess // 2, 3)
            args.values = max(nvalues * 2 // 3, 2)
        elif args.debug_single_value:
            args.processes = 1
            args.warmups = 0
            args.values = 1
            args.loops = 1
            args.min_time = 1e-9

        # calibration
        if args.calibrate_loops:
            self._only_in_worker("--calibrate-loops")
            if args.loops:
                raise CLIError("--loops=N is incompatible with "
                               "--calibrate-loops")
        elif args.recalibrate_loops:
            self._only_in_worker("--recalibrate-loops")
            if args.loops < 1:
                raise CLIError("--recalibrate-loops requires --loops=N")
        elif args.calibrate_warmups:
            self._only_in_worker("--calibrate-warmups")
            if args.loops < 1:
                raise CLIError("--calibrate-warmups requires --loops=N")
        elif args.recalibrate_warmups:
            self._only_in_worker("--recalibrate-warmups")
            if args.loops < 1 or args.warmups is None:
                raise CLIError("--recalibrate-warmups requires "
                               "--loops=N and --warmups=N")
        else:
            if args.worker and args.loops < 1:
                raise CLIError("--worker requires --loops=N "
                               "or --calibrate-loops")
            if args.worker and args.warmups is None:
                raise CLIError("--worker requires --warmups=N "
                               "or --calibrate-warmups")

            if args.values < 1:
                raise CLIError("--values must be >= 1")

        filename = args.output
        if filename and os.path.exists(filename):
            raise CLIError("The JSON file %r already exists" % filename)

        if args.worker_task:
            self._only_in_worker("--worker-task")

        if args.tracemalloc:
            if getattr(args, 'action', None) == 'command':
                raise CLIError('--tracemalloc cannot be used with pyperf command')
            try:
                import tracemalloc   # noqa
            except ImportError as exc:
                raise CLIError("fail to import tracemalloc: %s" % exc)

        if args.track_memory:
            if MS_WINDOWS:
                from pyperf._win_memory import check_tracking_memory
            elif OS_LINUX:
                from pyperf._linux_memory import check_tracking_memory
            else:
                from pyperf._psutil_memory import check_tracking_memory
            err_msg = check_tracking_memory()
            if err_msg:
                raise CLIError("unable to track the memory usage "
                               "(--track-memory): %s" % err_msg)

        args.python = abs_executable(args.python)
        if args.compare_to:
            args.compare_to = abs_executable(args.compare_to)

        if args.compare_to:
            for option in ('output', 'append'):
                if getattr(args, option):
                    raise CLIError("--%s option is incompatible "
                                   "with --compare-to option" % option)

    def _process_args(self):
        try:
            self._process_args_impl()
        except CLIError as exc:
            print("ERROR: %s" % str(exc))
            sys.exit(1)

    def _set_args(self, args):
        if self.args is not None:
            raise RuntimeError("arguments already parsed")

        self.args = args
        self._process_args()

    def parse_args(self, args=None):
        if self.args is not None and args is None:
            return self.args

        args = self.argparser.parse_args(args)
        self._set_args(args)
        return args

    def _cpu_affinity(self):
        cpus = self.args.affinity
        if not cpus:
            # --affinity option is not set: detect isolated CPUs
            isolated = True
            cpus = get_isolated_cpus()
            if not cpus:
                # no isolated CPUs or unable to get the isolated CPUs
                return
        else:
            isolated = False
            cpus = parse_cpu_list(cpus)

        if set_cpu_affinity(cpus):
            if self.args.verbose:
                if isolated:
                    text = ("Pin process to isolated CPUs: %s"
                            % format_cpu_list(cpus))
                else:
                    text = ("Pin process to CPUs: %s"
                            % format_cpu_list(cpus))
                print(text)

            if isolated:
                self.args.affinity = format_cpu_list(cpus)
        else:
            if not isolated:
                print("ERROR: CPU affinity not available.", file=sys.stderr)
                print("Install psutil dependency and check "
                      "psutil.Process.cpu_affinity is available on your OS.")
                sys.exit(1)
            elif not self.args.quiet:
                print("WARNING: unable to pin worker processes to "
                      "isolated CPUs, CPU affinity not available")
                print("Install psutil dependency and check "
                      "psutil.Process.cpu_affinity is available on your OS.")

    def _process_priority(self):
        if not MS_WINDOWS:
            return
        if not set_highest_priority():
            print("WARNING: unable to increase process priority")

    def _worker(self, task):
        self._cpu_affinity()
        self._process_priority()
        run = task.create_run()
        bench = pyperf.Benchmark((run,))
        self._display_result(bench, checks=False)
        return bench

    def _check_worker_task(self):
        args = self.parse_args()

        if args.worker_task is None:
            return True

        if args.worker_task != self._worker_task:
            # Skip the benchmark if it's not the expected worker task
            self._worker_task += 1
            return False

        return True

    def _main(self, task):
        if task.name in self._bench_names:
            raise ValueError("duplicated benchmark name: %r" % task.name)
        self._bench_names.add(task.name)

        args = self.parse_args()
        try:
            if args.worker:
                bench = self._worker(task)
            elif args.compare_to:
                self._compare_to()
                bench = None
            else:
                bench = self._manager()
        except KeyboardInterrupt:
            what = "Benchmark worker" if args.worker else "Benchmark"
            print("%s interrupted: exit" % what, file=sys.stderr)
            sys.exit(1)

        self._worker_task += 1
        return bench

    @staticmethod
    def _no_keyword_argument(kwargs):
        if not kwargs:
            return

        args = ', '.join(map(repr, sorted(kwargs)))
        raise TypeError('unexpected keyword argument %s' % args)

    def bench_time_func(self, name, time_func, *args, **kwargs):
        inner_loops = kwargs.pop('inner_loops', None)
        metadata = kwargs.pop('metadata', None)
        self._no_keyword_argument(kwargs)

        if not self._check_worker_task():
            return None

        if self.args.profile:
            profiler, time_func = profiling_wrapper(time_func)

        def task_func(_, loops):
            return time_func(loops, *args)

        task = WorkerProcessTask(self, name, task_func, metadata)

        task.inner_loops = inner_loops
        result = self._main(task)

        if self.args.profile:
            merge_profile_stats(profiler, self.args.profile)

        return result

    def bench_func(self, name, func, *args, **kwargs):
        """"Benchmark func(*args)."""

        inner_loops = kwargs.pop('inner_loops', None)
        metadata = kwargs.pop('metadata', None)
        self._no_keyword_argument(kwargs)

        if not self._check_worker_task():
            return None

        if args:
            func = functools.partial(func, *args)

        if self.args.profile:
            profiler, func = profiling_wrapper(func)

        def task_func(_, loops):
            # use fast local variables
            local_timer = time.perf_counter
            local_func = func
            if loops != 1:
                range_it = range(loops)

                t0 = local_timer()
                for _ in range_it:
                    local_func()
                dt = local_timer() - t0
            else:
                t0 = local_timer()
                local_func()
                dt = local_timer() - t0

            return dt

        task = WorkerProcessTask(self, name, task_func, metadata)
        task.inner_loops = inner_loops
        result = self._main(task)

        if self.args.profile:
            merge_profile_stats(profiler, self.args.profile)

        return result

    def bench_async_func(self, name, func, *args, **kwargs):
        """Benchmark await func(*args)"""

        inner_loops = kwargs.pop('inner_loops', None)
        metadata = kwargs.pop('metadata', None)
        loop_factory = kwargs.pop('loop_factory', None)
        self._no_keyword_argument(kwargs)

        if not self._check_worker_task():
            return None

        if args:
            func = functools.partial(func, *args)

        if self.args.profile:
            profiler, func = profiling_wrapper(func)

        def task_func(_, loops):
            if loops != 1:
                async def main():
                    # use fast local variables
                    local_timer = time.perf_counter
                    local_func = func
                    range_it = range(loops)

                    t0 = local_timer()
                    for _ in range_it:
                        await local_func()
                    dt = local_timer() - t0
                    return dt
            else:
                async def main():
                    # use fast local variables
                    local_timer = time.perf_counter
                    local_func = func

                    t0 = local_timer()
                    await local_func()
                    dt = local_timer() - t0
                    return dt

            import asyncio
            # using the lower level loop API instead of asyncio.run because
            # asyncio.run gained the `loop_factory` arg only in Python 3.12.
            # we can go back to asyncio.run when Python 3.12 is the oldest
            # supported version for pyperf.
            if loop_factory is None:
                loop = asyncio.new_event_loop()
            else:
                loop = loop_factory()
            asyncio.set_event_loop(loop)
            try:
                dt = loop.run_until_complete(main())
            finally:
                asyncio.set_event_loop(None)
                loop.close()

            return dt

        task = WorkerProcessTask(self, name, task_func, metadata)
        task.inner_loops = inner_loops
        result = self._main(task)

        if self.args.profile:
            merge_profile_stats(profiler, self.args.profile)

        return result

    def timeit(self, name, stmt=None, setup="pass", teardown="pass",
               inner_loops=None, duplicate=None, metadata=None, globals=None):

        if not self._check_worker_task():
            return None

        if stmt is None:
            # timeit(stmt) behaves as timeit(stmt, stmt)
            stmt = name

        # Use lazy import to limit imports on 'import pyperf'
        from pyperf._timeit import bench_timeit
        return bench_timeit(self, name, stmt,
                            setup=setup,
                            teardown=teardown,
                            inner_loops=inner_loops,
                            duplicate=duplicate,
                            func_metadata=metadata,
                            globals=globals)

    def _display_result(self, bench, checks=True):
        args = self.args

        # Display the average +- stdev
        if self.args.quiet:
            checks = False

        if args.pipe is not None:
            wpipe = WritePipe.from_subprocess(args.pipe)

            with wpipe.open_text() as wfile:
                with catch_broken_pipe_error(wfile):
                    bench.dump(wfile)
        else:
            lines = format_benchmark(bench,
                                     checks=checks,
                                     metadata=args.metadata,
                                     dump=args.dump,
                                     stats=args.stats,
                                     hist=args.hist,
                                     show_name=self._show_name)
            for line in lines:
                print(line)

            sys.stdout.flush()

        if args.append:
            pyperf.add_runs(args.append, bench)

        if args.output:
            if self._worker_task >= 1:
                pyperf.add_runs(args.output, bench)
            else:
                bench.dump(args.output)

    def _manager(self):
        # Use lazy import to limit imports on 'import pyperf'
        from pyperf._manager import Manager

        if self.args.verbose and self._worker_task > 0:
            print()
        bench = Manager(self).create_bench()
        if not self.args.quiet:
            print()
        self._display_result(bench)
        return bench

    def _compare_to(self):
        # Use lazy import to limit imports on 'import pyperf'
        from pyperf._compare import timeit_compare_benchs
        from pyperf._manager import Manager

        args = self.args
        python_ref = args.compare_to
        python_changed = args.python

        multiline = self._multiline_output()
        if args.python_names:
            name_ref, name_changed = args.python_names
        else:
            name_ref, name_changed = get_python_names(python_ref, python_changed)

        benchs = []
        for python, name in ((python_ref, name_ref), (python_changed, name_changed)):
            if self._worker_task > 0:
                print()

            if multiline:
                display_title('Benchmark %s' % name)
            elif not args.quiet:
                print(name, end=': ')

            bench = Manager(self, python=python).create_bench()
            benchs.append(bench)

            if multiline:
                self._display_result(bench)
            elif not args.quiet:
                print(' ' + format_result_value(bench))

            if multiline:
                print()
            elif not args.quiet:
                warnings = format_checks(bench)
                if warnings:
                    print()
                    for line in warnings:
                        print(line)
                    print()

        if multiline:
            display_title('Compare')
        elif not args.quiet:
            print()
        timeit_compare_benchs(name_ref, benchs[0], name_changed, benchs[1], args)

    def bench_command(self, name, command):
        if not self._check_worker_task():
            return None

        if self.args.profile:
            command.extend(["--profile", self.args.profile])

        if self.args.hook:
            for hook in self.args.hook:
                command.extend(["--hook", hook])

        # Use lazy import to limit imports on 'import pyperf'
        from pyperf._command import BenchCommandTask
        task = BenchCommandTask(self, name, command)
        return self._main(task)
