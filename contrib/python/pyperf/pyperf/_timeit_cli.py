"""
"pyperf timeit" microbenchmark command based on the Python timeit module.
"""
from pyperf._runner import Runner
from pyperf._timeit import bench_timeit


DEFAULT_NAME = 'timeit'


def add_cmdline_args(cmd, args):
    cmd.extend(('--name', args.name))
    if args.inner_loops:
        cmd.extend(('--inner-loops', str(args.inner_loops)))
    for setup in args.setup:
        cmd.extend(("--setup", setup))
    for teardown in args.teardown:
        cmd.extend(('--teardown', teardown))
    if args.duplicate:
        cmd.extend(('--duplicate', str(args.duplicate)))
    cmd.extend(args.stmt)


class TimeitRunner(Runner):
    def __init__(self, *args, **kw):
        if 'program_args' not in kw:
            kw['program_args'] = ('-m', 'pyperf', 'timeit')
        kw['add_cmdline_args'] = add_cmdline_args
        Runner.__init__(self, *args, **kw)

        def parse_name(name):
            return name.strip()

        cmd = self.argparser
        cmd.add_argument('--name', type=parse_name,
                         help='Benchmark name (default: %r)' % DEFAULT_NAME)
        cmd.add_argument('-s', '--setup', action='append', default=[],
                         help='setup statements')
        cmd.add_argument('--teardown', action='append', default=[],
                         help='teardown statements')
        cmd.add_argument('--inner-loops',
                         type=int,
                         help='Number of inner loops per value. For example, '
                              'the number of times that the code is copied '
                              'manually multiple times to reduce the overhead '
                              'of the outer loop.')
        cmd.add_argument('--duplicate', type=int,
                         help='duplicate statements to reduce the overhead of '
                              'the outer loop and multiply inner_loops '
                              'by DUPLICATE')
        cmd.add_argument('stmt', nargs='+', help='executed statements')

    def _process_args(self):
        Runner._process_args(self)
        args = self.args

        self._show_name = bool(args.name)
        if not args.name:
            args.name = DEFAULT_NAME


def main(runner):
    args = runner.args
    bench_timeit(runner, args.name, args.stmt, args.setup, args.teardown,
                 args.inner_loops, args.duplicate)
