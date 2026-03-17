import argparse
import collections
import functools
import os.path
import sys

import pyperf
from pyperf._metadata import _common_metadata
from pyperf._cli import (format_metadata, empty_line,
                         format_checks, format_histogram, format_title,
                         format_benchmark, display_title, format_result,
                         catch_broken_pipe_error)
from pyperf._formatter import format_timedelta, format_seconds, format_datetime
from pyperf._cpu_utils import parse_cpu_list
from pyperf._timeit_cli import TimeitRunner
from pyperf._utils import parse_run_list


def add_cmdline_args(cmd, args):
    cmd.extend(('--name', args.name))
    cmd.append(args.program)
    if args.program_args:
        cmd.extend(args.program_args)


class CommandRunner(pyperf.Runner):
    def __init__(self, cmd):
        def parse_name(name):
            return name.strip()

        pyperf.Runner.__init__(self,
                               _argparser=cmd,
                               add_cmdline_args=add_cmdline_args)
        self._program_args = ('-m', 'pyperf', 'command')

        cmd.add_argument('--name', type=parse_name, default='command',
                         help='Benchmark name (default: command)')
        cmd.add_argument('program',
                         help='Program path')
        cmd.add_argument('program_args', nargs=argparse.REMAINDER,
                         help='Program arguments')


def create_parser():
    parser = argparse.ArgumentParser(description='Display benchmark results.',
                                     prog='-m pyperf')
    subparsers = parser.add_subparsers(dest='action')

    def input_filenames(cmd, name=True):
        if name:
            cmd.add_argument('-b', '--benchmark', metavar='NAME',
                             dest='benchmarks', action='append',
                             help='only display the benchmark called NAME')
        cmd.add_argument('filenames', metavar='file.json',
                         type=str, nargs='+',
                         help='Benchmark file')

    def display_options(cmd):
        cmd.add_argument('-q', '--quiet',
                         action="store_true", help='enable quiet mode')
        input_filenames(cmd)

    def parse_affinity(value):
        try:
            cpus = parse_cpu_list(value)
        except ValueError:
            cpus = None
        if not cpus:
            raise argparse.ArgumentTypeError('invalid CPU list: %r' % value)
        return cpus

    def cpu_affinity(cmd):
        cmd.add_argument("--affinity", metavar="CPU_LIST", default=None,
                         type=parse_affinity,
                         help='Specify CPU affinity. '
                              'By default, use isolated CPUs.')

    # show
    cmd = subparsers.add_parser('show', help='Display a benchmark')
    cmd.add_argument('-m', '--metadata', dest='metadata', action="store_true",
                     help="Show metadata.")
    cmd.add_argument('-g', '--hist', action="store_true",
                     help='display an histogram of values')
    cmd.add_argument('-t', '--stats', action="store_true",
                     help='display statistics (min, max, ...)')
    cmd.add_argument('-d', '--dump', action="store_true",
                     help='display benchmark run results')
    display_options(cmd)

    # hist
    cmd = subparsers.add_parser('hist', help='Render an histogram')
    cmd.add_argument('--extend', action="store_true",
                     help="Extend the histogram to fit the terminal")
    cmd.add_argument('-n', '--bins', type=int, default=None,
                     help='Number of histogram bars (default: 25, or less '
                          'depeding on the terminal size)')
    display_options(cmd)

    # compare_to
    cmd = subparsers.add_parser('compare_to', help='Compare benchmarks')
    cmd.add_argument('-q', '--quiet', action="store_true",
                     help='enable quiet mode')
    cmd.add_argument('-v', '--verbose', action="store_true",
                     help='enable verbose mode')
    cmd.add_argument('-G', '--group-by-speed', action="store_true",
                     help='group slower/faster/same speed')
    cmd.add_argument('--min-speed', type=float,
                     help='Absolute minimum of speed in percent to '
                          'consider that a benchmark is significant '
                          '(default: 0%%)')
    cmd.add_argument('--table', action="store_true",
                     help='Render a table')
    cmd.add_argument("--table-format", type=str, default="rest",
                     choices=["rest", "md"],
                     help="Format of table rendering")
    input_filenames(cmd)

    # stats
    cmd = subparsers.add_parser('stats', help='Compute statistics')
    display_options(cmd)

    # metadata
    cmd = subparsers.add_parser('metadata', help='Display metadata')
    display_options(cmd)

    # check
    cmd = subparsers.add_parser('check',
                                help='Check if a benchmark seems stable')
    display_options(cmd)

    # collect_metadata
    cmd = subparsers.add_parser('collect_metadata')
    cpu_affinity(cmd)
    cmd.add_argument('-o', '--output', metavar='FILENAME',
                     help='Save metadata as JSON into FILENAME')

    # timeit
    cmd = subparsers.add_parser('timeit', help='Quick Python microbenchmark')
    timeit_runner = TimeitRunner(_argparser=cmd)

    # system
    cmd = subparsers.add_parser('system', help='System setup for benchmarks')
    cpu_affinity(cmd)
    cmd.add_argument("system_action", nargs="?",
                     choices=('show', 'tune', 'reset'),
                     default='show')

    # convert
    cmd = subparsers.add_parser('convert', help='Modify benchmarks')
    cmd.add_argument(
        'input_filename', help='Filename of the input benchmark suite')
    output = cmd.add_mutually_exclusive_group(required=True)
    output.add_argument('-o', '--output', metavar='OUTPUT_FILENAME',
                        dest='output_filename',
                        help='Filename where the output benchmark suite '
                             'is written')
    output.add_argument('--stdout', action='store_true',
                        help='Write benchmark encoded to JSON into stdout')
    cmd.add_argument('--include-benchmark', metavar='NAME',
                     dest='include_benchmarks', action='append',
                     help='Only keep benchmark called NAME')
    cmd.add_argument('--exclude-benchmark', metavar='NAME',
                     dest='exclude_benchmarks', action='append',
                     help='Remove the benchmark called NAMED')
    cmd.add_argument('--include-runs', help='Only keep benchmark runs RUNS')
    cmd.add_argument('--exclude-runs', help='Remove specified benchmark runs')
    cmd.add_argument('--indent', action='store_true',
                     help='Indent JSON (rather using compact JSON)')
    cmd.add_argument('--remove-warmups', action='store_true',
                     help='Remove warmup values')
    cmd.add_argument('--add', metavar='FILE',
                     help='Add benchmark runs of benchmark FILE')
    cmd.add_argument('--extract-metadata', metavar='NAME',
                     help='Use metadata NAME as the new run values')
    cmd.add_argument('--remove-all-metadata', action="store_true",
                     help='Remove all benchmarks metadata, but keep '
                          'the benchmarks name')
    cmd.add_argument('--update-metadata', metavar='METADATA',
                     help='Update metadata: METADATA is a comma-separated '
                          'list of KEY=VALUE')

    # dump
    cmd = subparsers.add_parser('dump', help='Dump the runs')
    cmd.add_argument('-v', '--verbose', action='store_true',
                     help='enable verbose mode')
    cmd.add_argument('--raw', action='store_true',
                     help='display raw values')
    display_options(cmd)

    # slowest
    cmd = subparsers.add_parser('slowest',
                                help='List benchmarks which took most '
                                     'of the time')
    cmd.add_argument('-n', type=int, default=5,
                     help='Number of slow benchmarks to display (default: 5)')
    input_filenames(cmd, name=False)

    # command
    cmd = subparsers.add_parser('command',
                                help='Benchmark a command')
    command_runner = CommandRunner(cmd)

    return parser, timeit_runner, command_runner


DataItem = collections.namedtuple('DataItem',
                                  'suite filename benchmark '
                                  'name title is_last')
GroupItem = collections.namedtuple('GroupItem', 'benchmark title filename')
GroupItem2 = collections.namedtuple('GroupItem2', 'name benchmarks is_last')
IterSuite = collections.namedtuple('IterSuite', 'filename suite')


def format_filename_noop(filename):
    return filename


def format_filename_func(suites):
    filenames = [suite.filename for suite in suites]

    # FIXME: reuse get_python_names()
    base_filenames = set(map(os.path.basename, filenames))
    if len(base_filenames) != len(filenames):
        # FIXME: try harder: try to get differente names by keeping only
        # the parent directory?
        return format_filename_noop

    def strip_extension(filename):
        name = os.path.splitext(filename)[0]
        if name.endswith('.json'):
            # replace "bench.json.gz" with "bench"
            name = name[:-5]
        return name

    next_filenames = set(map(strip_extension, base_filenames))
    if len(next_filenames) != len(base_filenames):
        return os.path.basename

    def format_filename(filename):
        filename = os.path.basename(filename)
        filename = strip_extension(filename)
        return filename

    return format_filename


class Benchmarks:
    def __init__(self):
        self.suites = []

    def load_benchmark_suite(self, filename):
        suite = pyperf.BenchmarkSuite.load(filename)
        self.suites.append(suite)

    def load_benchmark_suites(self, filenames):
        for filename in filenames:
            self.load_benchmark_suite(filename)

    def has_same_unique_benchmark(self):
        "True if all suites have one benchmark with the same name"
        if any(len(suite) > 1 for suite in self.suites):
            return False
        names = self.suites[0].get_benchmark_names()
        return all(suite.get_benchmark_names() == names
                   for suite in self.suites[1:])

    def include_benchmarks(self, names):
        for suite in self.suites:
            try:
                suite._convert_include_benchmark(names)
            except KeyError:
                fatal_missing_benchmarks(suite, names)

    def get_nsuite(self):
        return len(self.suites)

    def __len__(self):
        return sum(len(suite) for suite in self.suites)

    def iter_suites(self):
        format_filename = format_filename_func(self.suites)
        for suite in self.suites:
            filename = format_filename(suite.filename)
            yield IterSuite(filename, suite)

    def __iter__(self):
        format_filename = format_filename_func(self.suites)

        show_name = (len(self) > 1)
        show_filename = (self.get_nsuite() > 1)

        for suite_index, suite in enumerate(self.suites):
            filename = format_filename(suite.filename)
            last_suite = (suite_index == (len(self.suites) - 1))

            benchmarks = suite.get_benchmarks()
            for bench_index, benchmark in enumerate(benchmarks):
                name = benchmark.get_name()
                # FIXME: remove title, move logic to the caller?
                if show_name:
                    title = name
                    if show_filename:
                        title = "%s:%s" % (filename, title)
                else:
                    title = None
                last_benchmark = (bench_index == (len(benchmarks) - 1))
                is_last = (last_suite and last_benchmark)

                yield DataItem(suite, filename, benchmark, name, title, is_last)

    def _group_by_name_names(self):
        names = set(self.suites[0].get_benchmark_names())
        for suite in self.suites[1:]:
            names &= set(suite.get_benchmark_names())
        # Keep original name order
        return [name for name in self.suites[0].get_benchmark_names()
                if name in names]

    def group_by_name(self):
        format_filename = format_filename_func(self.suites)

        show_filename = (self.get_nsuite() > 1)

        names = self._group_by_name_names()
        show_name = (len(names) > 1)

        groups = []
        for index, name in enumerate(names):
            benchmarks = []
            for suite in self.suites:
                benchmark = suite.get_benchmark(name)
                filename = format_filename(suite.filename)
                if show_name:
                    if not show_filename:
                        title = name
                    else:
                        # name is displayed in the group title
                        title = filename
                else:
                    title = None
                benchmarks.append(GroupItem(benchmark, title, filename))

            is_last = (index == (len(names) - 1))
            group = GroupItem2(name, benchmarks, is_last)
            groups.append(group)

        return groups

    def group_by_name_ignored(self):
        names = set(self._group_by_name_names())
        for suite in self.suites:
            ignored = []
            for bench in suite:
                if bench.get_name() not in names:
                    ignored.append(bench)
            if ignored:
                yield (suite, ignored)


def load_benchmarks(args):
    data = Benchmarks()
    data.load_benchmark_suites(args.filenames)
    if getattr(args, 'benchmarks', None):
        data.include_benchmarks(args.benchmarks)
    return data


def _display_common_metadata(metadatas, lines):
    if len(metadatas) < 2:
        return

    for metadata in metadatas:
        # don't display name as metadata, it's already displayed
        metadata.pop('name', None)

    common_metadata = _common_metadata(metadatas)
    if common_metadata:
        format_title('Common metadata', lines=lines)
        empty_line(lines)

        format_metadata(common_metadata, lines=lines)

    for key in common_metadata:
        for metadata in metadatas:
            metadata.pop(key, None)


def cmd_compare_to(args):
    from pyperf._compare import compare_suites, CompareError

    data = load_benchmarks(args)
    if data.get_nsuite() < 2:
        print("ERROR: need at least two benchmark files")
        sys.exit(1)

    if args.group_by_speed and data.get_nsuite() != 2:
        print("ERROR: --by-speed only works on two benchmark files",
              file=sys.stderr)
        sys.exit(1)

    try:
        compare_suites(data, args)
    except CompareError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


def cmd_collect_metadata(args):
    from pyperf._collect_metadata import cmd_collect_metadata as func
    func(args)


def display_benchmarks(args, show_metadata=False, hist=False, stats=False,
                       dump=False, result=False, checks=False,
                       display_runs_args=None, only_checks=False):
    data = load_benchmarks(args)

    output = []

    if show_metadata:
        metadatas = [item.benchmark.get_metadata() for item in data]
        _display_common_metadata(metadatas, lines=output)

    if hist or stats or dump or show_metadata or (not result):
        use_title = True
    else:
        use_title = False
        if checks:
            for index, item in enumerate(data):
                warnings = format_checks(item.benchmark)
                if warnings:
                    use_title = True
                    break

    if use_title:
        show_filename = (data.get_nsuite() > 1)
        show_name = not data.has_same_unique_benchmark()
        if not show_filename and stats:
            show_filename = (len(data) > 1)

        suite = None
        for index, item in enumerate(data):
            lines = []

            if show_metadata:
                metadata = metadatas[index]
                if metadata:
                    empty_line(lines)
                    lines.append("Metadata:")
                    format_metadata(metadata, lines=lines)

            bench_lines = format_benchmark(item.benchmark,
                                           hist=hist,
                                           stats=stats,
                                           dump=dump,
                                           checks=checks,
                                           result=result,
                                           display_runs_args=display_runs_args,
                                           only_checks=only_checks)

            if bench_lines:
                empty_line(lines)
                lines.extend(bench_lines)

            if lines:
                bench_lines = lines
                lines = []

                if show_filename and item.suite is not suite:
                    suite = item.suite
                    format_title(item.filename, 1, lines=lines)

                    if stats and len(suite) > 1:
                        empty_line(lines)

                        duration = suite.get_total_duration()
                        lines.append("Number of benchmarks: %s" % len(suite))
                        lines.append("Total duration: %s" % format_seconds(duration))
                        dates = suite.get_dates()
                        if dates:
                            start, end = dates
                            lines.append("Start date: %s" % format_datetime(start, microsecond=False))
                            lines.append("End date: %s" % format_datetime(end, microsecond=False))

                if show_name:
                    format_title(item.name, 2, lines=lines)

                empty_line(lines)
                lines.extend(bench_lines)

            if lines:
                empty_line(output)
                output.extend(lines)

        contains_warning = False
        for line in output:
            if line.startswith("WARNING:"):
                contains_warning = True
            print(line)

        if not contains_warning and only_checks:
            if len(data) == 1:
                print("The benchmark seems to be stable")
            else:
                print("All benchmarks seem to be stable")
    else:
        for line in output:
            print(line)

        show_filename = (data.get_nsuite() > 1)

        suite = None
        for item in data:
            if show_filename and item.suite is not suite:
                if suite is not None:
                    print()

                suite = item.suite
                display_title(item.filename, 1)

            line = format_result(item.benchmark)
            if item.title:
                line = '%s: %s' % (item.name, line)
            print(line)


def cmd_show(args):
    display_benchmarks(args,
                       show_metadata=args.metadata,
                       hist=args.hist,
                       stats=args.stats,
                       dump=args.dump,
                       checks=not args.quiet,
                       result=True)


def cmd_metadata(args):
    display_benchmarks(args, show_metadata=True, checks=not args.quiet)


def cmd_check(args):
    display_benchmarks(args, checks=True, only_checks=True)


def cmd_dump(args):
    display_runs_args = {'quiet': args.quiet,
                         'verbose': args.verbose,
                         'raw': args.raw}
    display_benchmarks(args,
                       dump=True,
                       display_runs_args=display_runs_args,
                       checks=not args.quiet)


def cmd_timeit(args, timeit_runner):
    import pyperf._timeit_cli as timeit_cli
    timeit_runner._set_args(args)
    timeit_cli.main(timeit_runner)


def cmd_stats(args):
    display_benchmarks(args, stats=True, checks=not args.quiet)


def cmd_hist(args):
    checks = not args.quiet
    data = load_benchmarks(args)

    ignored = list(data.group_by_name_ignored())

    groups = data.group_by_name()
    show_filename = (data.get_nsuite() > 1)
    show_group_name = (len(groups) > 1)

    for name, benchmarks, is_last in groups:
        if show_group_name:
            display_title(name)

        benchmarks = [(benchmark, filename if show_filename else None)
                      for benchmark, title, filename in benchmarks]

        for line in format_histogram(benchmarks, bins=args.bins,
                                     extend=args.extend,
                                     checks=checks):
            print(line)

        if not (is_last or ignored):
            print()

    for suite, ignored in ignored:
        for bench in ignored:
            name = bench.get_name()
            print("[ %s ]" % name)
            for line in format_histogram([name], bins=args.bins,
                                         extend=args.extend,
                                         checks=checks):
                print(line)


def fatal_missing_benchmarks(suite, names):
    print("ERROR: The benchmark suite %s doesn't contain "
          "with benchmark name in %r"
          % (suite.filename, names),
          file=sys.stderr)
    sys.exit(1)


def fatal_no_more_benchmark(suite):
    print("ERROR: After modification, the benchmark suite %s has no "
          "more benchmark!"
          % suite.filename,
          file=sys.stderr)
    sys.exit(1)


def cmd_convert(args):
    suite = pyperf.BenchmarkSuite.load(args.input_filename)

    if args.add:
        suite2 = pyperf.BenchmarkSuite.load(args.add)
        for bench in suite2.get_benchmarks():
            suite._add_benchmark_runs(bench)

    if args.include_benchmarks:
        names = args.include_benchmarks
        try:
            suite._convert_include_benchmark(names)
        except KeyError:
            fatal_missing_benchmarks(suite, names)

    elif args.exclude_benchmarks:
        names = args.exclude_benchmarks
        try:
            suite._convert_exclude_benchmark(names)
        except ValueError:
            fatal_no_more_benchmark(suite)

    if args.include_runs or args.exclude_runs:
        if args.include_runs:
            runs = args.include_runs
            include = True
        else:
            runs = args.exclude_runs
            include = False
        try:
            only_runs = parse_run_list(runs)
        except ValueError as exc:
            print("ERROR: %s (runs: %r)" % (exc, runs), file=sys.stderr)
            sys.exit(1)
        for benchmark in suite:
            try:
                benchmark._filter_runs(include, only_runs)
            except ValueError:
                print("ERROR: Benchmark %r has no more run"
                      % benchmark.get_name(),
                      file=sys.stderr)
                sys.exit(1)

    if args.remove_warmups:
        for benchmark in suite:
            benchmark._remove_warmups()

    if args.update_metadata:
        items = [item.strip()
                 for item in args.update_metadata.split(',')]

        metadata = {}
        for item in items:
            if not item:
                continue
            key, _, value = item.partition('=')
            metadata[key] = value

        for benchmark in suite:
            benchmark.update_metadata(metadata)

    if args.extract_metadata:
        name = args.extract_metadata
        for benchmark in suite:
            try:
                benchmark._extract_metadata(name)
            except KeyError:
                print("ERROR: Benchmark %r has no metadata %r"
                      % (benchmark.get_name(), name),
                      file=sys.stderr)
                sys.exit(1)
            except TypeError:
                print("ERROR: Metadata %r of benchmark %r is not an integer"
                      % (name, benchmark.get_name()),
                      file=sys.stderr)
                sys.exit(1)

    if args.remove_all_metadata:
        for benchmark in suite:
            benchmark._remove_all_metadata()

    compact = not args.indent
    if args.output_filename:
        suite.dump(args.output_filename, compact=compact)
    else:
        suite.dump(sys.stdout, compact=compact)


def cmd_slowest(args):
    data = load_benchmarks(args)
    nslowest = args.n

    use_title = (data.get_nsuite() > 1)
    for item in data.iter_suites():
        if use_title:
            display_title(item.filename, 1)

        benchs = []
        for bench in item.suite:
            duration = bench.get_total_duration()
            benchs.append((duration, bench))
        benchs.sort(key=lambda item: item[0], reverse=True)

        for index, item in enumerate(benchs[:nslowest], 1):
            duration, bench = item
            print("#%s: %s (%s)"
                  % (index, bench.get_name(), format_timedelta(duration)))


def cmd_system(args):
    from pyperf._system import System
    System().main(args.system_action, args)


def cmd_bench_command(runner, args):
    runner._set_args(args)
    name = args.name
    command = [args.program] + args.program_args
    runner.bench_command(name, command)


def main():
    parser, timeit_runner, command_runner = create_parser()
    args = parser.parse_args()
    action = args.action

    dispatch = {
        'show': functools.partial(cmd_show, args),
        'compare_to': functools.partial(cmd_compare_to, args),
        'hist': functools.partial(cmd_hist, args),
        'stats': functools.partial(cmd_stats, args),
        'metadata': functools.partial(cmd_metadata, args),
        'check': functools.partial(cmd_check, args),
        'collect_metadata': functools.partial(cmd_collect_metadata, args),
        'timeit': functools.partial(cmd_timeit, args, timeit_runner),
        'convert': functools.partial(cmd_convert, args),
        'dump': functools.partial(cmd_dump, args),
        'slowest': functools.partial(cmd_slowest, args),
        'system': functools.partial(cmd_system, args),
        'command': functools.partial(cmd_bench_command, command_runner, args),
    }

    with catch_broken_pipe_error():
        try:
            func = dispatch[action]
        except KeyError:
            parser.print_usage()
            sys.exit(1)
        else:
            func()


if __name__ == "__main__":
    main()
