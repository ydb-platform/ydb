import sys
import subprocess

from pyperf._bench import _load_suite_from_pipe
from pyperf._cli import format_run
from pyperf._formatter import format_number
from pyperf._utils import MS_WINDOWS, create_environ, create_pipe, popen_killer


EXIT_TIMEOUT = 60

# Limit to 5 calibration processes
# (10 if calibration is needed for loops and warmups)
MAX_CALIBRATION = 5


class Manager:
    """
    Manager process which spawns worker processes to:
    - calibrate warmups
    - calibrate loops
    - compute values

    It uses a state machine with next_run attribute and the choose_next_run()
    method.
    """
    def __init__(self, runner, python=None):
        self.runner = runner
        self.args = runner.args
        if python:
            self.python = python
        else:
            self.python = self.args.python
        self.bench = None
        self.need_nprocess = self.args.processes
        self.nprocess = 0
        self.next_run = 'loops'
        self.calibrate_loops = int(not self.args.loops)
        self.calibrate_warmups = int(self.args.warmups is None)

    def worker_cmd(self, calibrate_loops, calibrate_warmups, wpipe):
        args = self.args

        cmd = [self.python]
        cmd.extend(self.runner._program_args)
        cmd.extend(('--worker', '--pipe', str(wpipe),
                    '--worker-task=%s' % self.runner._worker_task,
                    '--values', str(args.values),
                    '--min-time', str(args.min_time)))
        if calibrate_loops == 1:
            cmd.append('--calibrate-loops')
        else:
            cmd.extend(('--loops', str(args.loops)))
            if calibrate_loops > 1:
                cmd.append('--recalibrate-loops')
        if calibrate_warmups == 1:
            cmd.append('--calibrate-warmups')
        else:
            cmd.extend(('--warmups', str(args.warmups)))
            if calibrate_warmups > 1:
                cmd.append('--recalibrate-warmups')
        if args.verbose:
            cmd.append('-' + 'v' * args.verbose)
        if args.affinity:
            cmd.append('--affinity=%s' % args.affinity)
        if args.tracemalloc:
            cmd.append('--tracemalloc')
        if args.track_memory:
            cmd.append('--track-memory')

        if args.profile:
            cmd.extend(['--profile', args.profile])

        if args.timeout:
            cmd.extend(['--timeout', str(args.timeout)])

        if args.hook:
            for hook in args.hook:
                cmd.extend(['--hook', hook])

        if self.runner._add_cmdline_args:
            self.runner._add_cmdline_args(cmd, args)

        return cmd

    def spawn_worker(self, calibrate_loops, calibrate_warmups):
        env = create_environ(self.args.inherit_environ,
                             self.args.locale,
                             self.args.copy_env)

        rpipe, wpipe = create_pipe()
        with rpipe:
            with wpipe:
                warg = wpipe.to_subprocess()
                cmd = self.worker_cmd(calibrate_loops,
                                      calibrate_warmups, warg)

                kw = {}
                if MS_WINDOWS:
                    # Set close_fds to False to call CreateProcess() with
                    # bInheritHandles=True. For pass_handles, see
                    # http://bugs.python.org/issue19764
                    kw['close_fds'] = False
                else:
                    kw['pass_fds'] = [wpipe.fd]

                proc = subprocess.Popen(cmd, env=env, **kw)

            with popen_killer(proc):
                try:
                    bench_json = rpipe.read_text(timeout=self.args.timeout)
                    exitcode = proc.wait(timeout=EXIT_TIMEOUT)
                except TimeoutError as exc:
                    print(exc)
                    sys.exit(124)

        if exitcode:
            raise RuntimeError("%s failed with exit code %s"
                               % (cmd[0], exitcode))

        return _load_suite_from_pipe(bench_json)

    def create_suite(self):
        # decide which kind of run must be computed
        if self.next_run == 'loops' and not self.calibrate_loops:
            self.next_run = 'warmups'
        if self.next_run == 'warmups' and not self.calibrate_warmups:
            self.next_run = 'values'

        # compute the run
        if self.next_run == 'loops':
            suite = self.spawn_worker(self.calibrate_loops, 0)
        elif self.next_run == 'warmups':
            suite = self.spawn_worker(0, self.calibrate_warmups)
        else:
            suite = self.spawn_worker(0, 0)
        if suite is None:
            raise RuntimeError("pyperf worker process didn't produce JSON result")
        return suite

    def create_worker_bench(self):
        suite = self.create_suite()

        # get the run
        benchmarks = suite._benchmarks
        if len(benchmarks) != 1:
            raise ValueError("worker produced %s benchmarks instead of 1"
                             % len(benchmarks))
        worker_bench = benchmarks[0]
        if len(worker_bench._runs) != 1:
            raise ValueError("worker produced %s runs, only 1 run expected"
                             % len(worker_bench._runs))
        run = worker_bench._runs[0]

        # save the run into bench
        if self.bench is not None:
            self.bench.add_runs(worker_bench)
        else:
            self.bench = worker_bench
        if not worker_bench._only_calibration():
            self.nprocess += 1

        return (worker_bench, run)

    def display_run(self, bench, run):
        if self.args.verbose:
            for line in format_run(bench, len(self.bench._runs), run):
                print(line)
            sys.stdout.flush()
        elif not self.args.quiet:
            print(".", end='')
            sys.stdout.flush()

    def calibration_done(self):
        if self.args.verbose:
            print("Calibration: %s, %s"
                  % (format_number(self.args.warmups, 'warmup'),
                     format_number(self.args.loops, 'loop')))
        self.calibrate_loops = 0
        self.calibrate_warmups = 0

    def handle_calibration(self, run):
        args = self.args

        if run._is_calibration_loops() or run._is_recalibration_loops():
            old_loops = args.loops
            args.loops = run._get_calibration_loops()
            self.calibrate_loops += 1

            if args.loops == old_loops and args.warmups is not None:
                self.calibration_done()
            elif args.loops != old_loops and self.calibrate_warmups > 1:
                # number of loops increased and warmup already
                # calibrated: need to restart the warmup calibration
                # (less warmup may be needed with more loops)
                self.calibrate_warmups = 1

            if self.calibrate_loops and not self.calibrate_warmups:
                self.calibration_done()

            if self.calibrate_loops > MAX_CALIBRATION:
                print("ERROR: calibration failed, the number of loops "
                      "is not stable after %s calibrations"
                      % (self.calibrate_loops - 1))
                sys.exit(1)

        elif run._is_calibration_warmups() or run._is_recalibration_warmups():
            old_warmups = args.warmups
            args.warmups = run._get_calibration_warmups()
            self.calibrate_warmups += 1

            if old_warmups is not None and args.warmups <= old_warmups:
                # loops calibrated with old_warmups > warmups, no
                # need to recalibrate
                self.calibration_done()
            elif args.warmups == old_warmups:
                # the number of warmup is stable: the calibration is done
                self.calibration_done()

            if self.calibrate_warmups and not self.calibrate_loops:
                self.calibration_done()

            if self.calibrate_warmups > MAX_CALIBRATION:
                print("ERROR: calibration failed, the number of warmups "
                      "is not stable after %s calibrations"
                      % (self.calibrate_warmups - 1))
                sys.exit(1)

    def choose_next_run(self):
        if self.next_run == 'loops':
            self.next_run = 'warmups'
        elif self.next_run == 'warmups':
            self.next_run = 'loops'
        # else: keep action 'values'

    def create_bench(self):
        old_warmups = self.args.warmups
        old_loops = self.args.loops
        if self.args.warmups is None:
            self.args.warmups = 1

        while self.nprocess < self.need_nprocess:
            worker_bench, run = self.create_worker_bench()
            self.display_run(worker_bench, run)
            self.handle_calibration(run)
            self.choose_next_run()

        # restore the old value of warmups and loops, to recalibrate
        # the next benchmark function if needed
        self.args.warmups = old_warmups
        self.args.loops = old_loops
        return self.bench
