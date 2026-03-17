import contextlib
import errno
import os.path
import sys

from pyperf._formatter import (format_seconds, format_number,
                               format_datetime)
from pyperf._metadata import format_metadata as _format_metadata


def empty_line(lines):
    if lines:
        lines.append('')


def format_title(title, level=1, lines=None):
    if lines is None:
        lines = []

    empty_line(lines)

    lines.append(title)
    if level == 1:
        char = '='
    else:
        char = '-'
    lines.append(char * len(title))
    return lines


def display_title(title, level=1):
    for line in format_title(title, level):
        print(line)
    print()


def format_metadata(metadata, prefix='- ', lines=None):
    if lines is None:
        lines = []
    for name, value in sorted(metadata.items()):
        value = _format_metadata(name, value)
        lines.append("%s%s: %s" % (prefix, name, value))
    return lines


def _format_values_diff(bench, values, raw, total_loops):
    values_str = [bench.format_value(value) for value in values]
    mean = bench.mean()
    max_delta = mean * 0.05
    for index, value in enumerate(values):
        if raw:
            value = float(value) / total_loops
        delta = float(value) - mean
        if abs(delta) > max_delta:
            values_str[index] += ' (%+.0f%%)' % (delta * 100 / mean)
    return values_str


def format_run(bench, run_index, run, common_metadata=None, raw=False,
               verbose=0, lines=None):
    if lines is None:
        lines = []

    inner_loops = run.get_inner_loops()

    if run._is_calibration():
        if run._is_calibration_warmups():
            warmups = run._get_calibration_warmups()
            action = 'calibrate the number of warmups: %s' % format_number(warmups)
        elif run._is_recalibration_warmups():
            warmups = run._get_calibration_warmups()
            action = 'recalibrate the number of warmups: %s' % format_number(warmups)
        elif run._is_recalibration_loops():
            loops = run._get_calibration_loops()
            action = 'recalibrate the number of loops: %s' % format_number(loops)
        else:
            loops = run._get_calibration_loops()
            action = 'calibrate the number of loops: %s' % format_number(loops)
        lines.append("Run %s: %s" % (run_index, action))
        if raw:
            name = 'raw calibrate'
        else:
            name = 'calibrate'
        unit = bench.get_unit()
        format_value = bench.format_value
        for index, warmup in enumerate(run.warmups, 1):
            loops, value = warmup
            raw_value = value * (loops * inner_loops)
            if raw:
                text = ("%s (loops: %s)"
                        % (format_value(raw_value),
                           format_number(loops)))
            # when using --track-memory, displaying value * loops doesn't make
            # sense, so only display raw value if the unit is seconds
            elif unit == 'second':
                text = ("%s (loops: %s, raw: %s)"
                        % (format_value(value),
                           format_number(loops),
                           format_value(raw_value)))
            else:
                text = ("%s (loops: %s)"
                        % (format_value(value),
                           format_number(loops)))
            lines.append("- %s %s: %s"
                         % (name, index, text))
    else:
        show_warmup = (verbose >= 0)

        total_loops = run.get_total_loops()

        values = run.values
        if raw:
            warmups = [bench.format_value(value * (loops * inner_loops))
                       for loops, value in run.warmups]
            values = [value * total_loops for value in values]
        else:
            warmups = run.warmups
            if warmups:
                warmups = [value for loops, value in warmups]
                warmups = _format_values_diff(bench, warmups, raw, total_loops)
        values = _format_values_diff(bench, values, raw, total_loops)

        if verbose >= 0:
            loops = run.get_loops()
            lines.append("Run %s: %s, %s, %s"
                         % (run_index,
                            format_number(len(warmups), 'warmup'),
                            format_number(len(values), 'value'),
                            format_number(loops, 'loop')))
        else:
            lines.append("Run %s:" % run_index)

        if warmups and show_warmup:
            if raw:
                name = 'raw warmup'
            else:
                name = 'warmup'
            for index, warmup in enumerate(warmups, 1):
                lines.append('- %s %s: %s' % (name, index, warmup))

        if raw:
            name = 'raw value'
        else:
            name = 'value'
        for index, value in enumerate(values, 1):
            lines.append('- %s %s: %s' % (name, index, value))

    if verbose > 0:
        metadata = run.get_metadata()
        if metadata:
            lines.append('- Metadata:')
            for name, value in sorted(metadata.items()):
                if common_metadata and name in common_metadata:
                    continue
                value = _format_metadata(name, value)
                lines.append('  %s: %s' % (name, value))

    return lines


def _format_runs(bench, quiet=False, verbose=False, raw=False, lines=None):
    runs = bench.get_runs()
    if quiet:
        verbose = -1
    elif verbose:
        verbose = 1
    else:
        verbose = 0

    if lines is None:
        lines = []
    if verbose > 0:
        empty_line(lines)

        # FIXME: display metadata in format_benchmark()
        common_metadata = bench.get_metadata()
        lines.append("Metadata:")
        format_metadata(common_metadata, prefix='  ', lines=lines)
    else:
        common_metadata = None

    empty_line_written = False
    for run_index, run in enumerate(runs, 1):
        if quiet and run._is_calibration():
            continue
        if not empty_line_written:
            empty_line_written = True
            empty_line(lines)
        format_run(bench, run_index, run,
                   common_metadata=common_metadata,
                   verbose=verbose, raw=raw, lines=lines)

    return lines


PERCENTILE_NAMES = {0: 'minimum', 25: 'Q1', 50: 'median', 75: 'Q3', 100: 'maximum'}


def format_stats(bench, lines):
    fmt = bench.format_value
    values = bench.get_values()

    nrun = bench.get_nrun()
    nvalue = len(values)

    empty_line(lines)

    # Total duration
    duration = bench.get_total_duration()
    if duration:
        lines.append("Total duration: %s" % format_seconds(duration))

    # Start/End dates
    dates = bench.get_dates()
    if dates:
        start, end = dates
        lines.append("Start date: %s" % format_datetime(start, microsecond=False))
        lines.append("End date: %s" % format_datetime(end, microsecond=False))

    # Raw value minimize/maximum
    raw_values = bench._get_raw_values()
    lines.append("Raw value minimum: %s" % bench.format_value(min(raw_values)))
    lines.append("Raw value maximum: %s" % bench.format_value(max(raw_values)))
    lines.append('')

    # Number of values
    ncalibration_runs = sum(run._is_calibration() for run in bench._runs)
    lines.append("Number of calibration run: %s"
                 % format_number(ncalibration_runs))
    lines.append("Number of run with values: %s"
                 % (format_number(nrun - ncalibration_runs)))
    lines.append("Total number of run: %s" % format_number(nrun))
    lines.append('')

    # Number of values
    nwarmup = bench._get_nwarmup()
    text = format_number(nwarmup)
    if isinstance(nwarmup, float):
        text += ' (average)'
    lines.append('Number of warmup per run: %s' % text)

    nvalue_per_run = bench._get_nvalue_per_run()
    text = format_number(nvalue_per_run)
    if isinstance(nvalue_per_run, float):
        text += ' (average)'
    lines.append('Number of value per run: %s' % text)

    # Loop iterations per value
    loops = bench.get_loops()
    inner_loops = bench.get_inner_loops()
    total_loops = loops * inner_loops
    if isinstance(total_loops, int):
        text = format_number(total_loops)
    else:
        text = "%s (average)" % total_loops

    if not (isinstance(inner_loops, int) and inner_loops == 1):
        if isinstance(loops, int):
            loops = format_number(loops, 'outer-loop')
        else:
            loops = '%.1f outer-loops (average)'

        if isinstance(inner_loops, int):
            inner_loops = format_number(inner_loops, 'inner-loop')
        else:
            inner_loops = "%.1f inner-loops (average)" % inner_loops

        text = '%s (%s x %s)' % (text, loops, inner_loops)

    lines.append("Loop iterations per value: %s" % text)
    lines.append("Total number of values: %s" % format_number(nvalue))
    lines.append('')

    # Minimum
    table = [("Minimum", bench.format_value(min(values)))]

    # Median +- MAD
    median = bench.median()
    if len(values) > 2:
        median_abs_dev = bench.median_abs_dev()
        table.append(("Median +- MAD",
                      "%s +- %s"
                      % bench.format_values((median, median_abs_dev))))
    else:
        table.append(("Mean", bench.format_value(median)))

    # Mean +- std dev
    mean = bench.mean()
    if len(values) > 2:
        stdev = bench.stdev()
        table.append(("Mean +- std dev",
                      "%s +- %s" % bench.format_values((mean, stdev))))
    else:
        table.append(("Mean", bench.format_value(mean)))

    table.append(("Maximum", bench.format_value(max(values))))

    # Render table
    width = max(len(row[0]) + 1 for row in table)
    for key, value in table:
        key = (key + ':').ljust(width)
        lines.append("%s %s" % (key, value))
    lines.append('')

    def format_limit(mean, value):
        return ("%s (%+.0f%% of the mean)"
                % (fmt(value), (value - mean) * 100.0 / mean))

    # Percentiles
    for p in (0, 5, 25, 50, 75, 95, 100):
        text = format_limit(mean, bench.percentile(p))
        text = "%3sth percentile: %s" % (p, text)
        name = PERCENTILE_NAMES.get(p)
        if name:
            text = '%s -- %s' % (text, name)
        lines.append(text)
    lines.append('')

    # Outliers
    q1 = bench.percentile(25)
    q3 = bench.percentile(75)
    iqr = q3 - q1
    outlier_min = (q1 - 1.5 * iqr)
    outlier_max = (q3 + 1.5 * iqr)
    noutlier = sum(not (outlier_min <= value <= outlier_max)
                   for value in values)
    bounds = bench.format_values((outlier_min, outlier_max))
    lines.append('Number of outlier (out of %s..%s): %s'
                 % (bounds[0], bounds[1], format_number(noutlier)))

    return lines


def format_histogram(benchmarks, bins=20, extend=False, lines=None,
                     checks=False):
    import collections
    import shutil

    if hasattr(shutil, 'get_terminal_size'):
        columns, nline = shutil.get_terminal_size()
    else:
        columns, nline = (80, 25)

    if not bins:
        bins = max(nline - 3, 3)
        if not extend:
            bins = min(bins, 25)

    all_values = []
    for bench, title in benchmarks:
        all_values.extend(bench.get_values())
    all_min = min(all_values)
    all_max = max(all_values)
    value_k = float(all_max - all_min) / bins
    if not value_k:
        value_k = 1.0

    def value_bucket(value):
        # round towards zero (ROUND_DOWN)
        return int(value / value_k)

    bucket_min = value_bucket(all_min)
    bucket_max = value_bucket(all_max)
    if lines is None:
        lines = []

    for item in benchmarks:
        empty_line(lines)

        bench, title = item
        if title:
            lines.append("[ %s ]" % title)

        values = bench.get_values()

        buckets = [value_bucket(value) for value in values]
        counter = collections.Counter(buckets)
        count_max = max(counter.values())
        count_width = len(str(count_max))

        value_width = max([len(bench.format_value(bucket * value_k))
                           for bucket in range(bucket_min, bucket_max + 1)])
        line = ': %s #' % count_max
        width = columns - (value_width + len(line))
        if not extend:
            width = min(width, 79)
        width = max(width, 3)
        line_k = float(width) / max(counter.values())
        for bucket in range(bucket_min, bucket_max + 1):
            count = counter.get(bucket, 0)
            linelen = int(round(count * line_k))
            text = bench.format_value(bucket * value_k)
            line = ('#' * linelen) or '|'
            lines.append("{:>{}}: {:>{}} {}".format(text, value_width,
                                                    count, count_width, line))

        if checks:
            format_checks(bench, lines=lines)

    return lines


def format_checks(bench, lines=None, check_too_many_processes=False):
    if lines is None:
        lines = []

    if bench._only_calibration():
        # Benchmark only contains calibration runs
        return lines

    values = bench.get_values()
    mean = bench.mean()
    warnings = []
    warn = warnings.append
    required_nprocesses = None

    # Display a warning if the standard deviation is greater than 10%
    # of the mean
    if len(values) >= 2:
        stdev = bench.stdev()
        percent = stdev * 100.0 / mean
        if percent >= 10.0:
            warn("the standard deviation (%s) is %.0f%% of the mean (%s)"
                 % (bench.format_value(stdev), percent, bench.format_value(mean)))
        else:
            # display a warning if the number of samples isn't enough to get a stable result
            required_nprocesses = bench.required_nprocesses()
            if (
                required_nprocesses is not None and
                required_nprocesses > len(bench._runs)
            ):
                warn("Not enough samples to get a stable result (95% certainly of less than 1% variation)")

    # Minimum and maximum, detect obvious outliers
    for minimum, value in (
        ('minimum', min(values)),
        ('maximum', max(values)),
    ):
        percent = (value - mean) * 100.0 / mean
        if abs(percent) >= 50:
            if percent >= 0:
                text = "%.0f%% greater" % (percent)
            else:
                text = "%.0f%% smaller" % (-percent)
            warn("the %s (%s) is %s than the mean (%s)"
                 % (minimum, bench.format_value(value), text, bench.format_value(mean)))

    # Check that the shortest raw value took at least 1 ms
    if bench.get_unit() == 'second':
        shortest = min(bench._get_raw_values())
        if shortest < 1e-3:
            warn("the shortest raw value is only %s"
                 % bench.format_value(shortest))

    if warnings:
        empty_line(lines)
        lines.append("WARNING: the benchmark result may be unstable")
        for msg in warnings:
            lines.append("* %s" % msg)
        empty_line(lines)
        lines.append("Try to rerun the benchmark with more runs, values "
                     "and/or loops.")
        lines.append("Run '%s -m pyperf system tune' command to reduce "
                     "the system jitter."
                     % os.path.basename(sys.executable))
        lines.append("Use pyperf stats, pyperf dump and pyperf hist to analyze results.")
        lines.append("Use --quiet option to hide these warnings.")

    if check_too_many_processes:
        if required_nprocesses is None:
            required_nprocesses = bench.required_nprocesses()
        if (
            required_nprocesses is not None and
            required_nprocesses < len(bench._runs) * 0.75
        ):
            lines.append("Benchmark was run more times than necessary to get a stable result.")
            lines.append(
                "Consider passing processes=%d to the Runner constructor to save time." %
                required_nprocesses
            )

    # Warn if nohz_full+intel_pstate combo if found in cpu_config metadata
    for run in bench._runs:
        cpu_config = run._metadata.get('cpu_config')
        if not cpu_config:
            continue
        if 'nohz_full' in cpu_config and 'intel_pstate' in cpu_config:
            empty_line(lines)
            warn("WARNING: nohz_full is enabled on CPUs which use the "
                 "intel_pstate driver, whereas intel_pstate is incompatible "
                 "with nohz_full")
            warn("CPU config: %s" % cpu_config)
            warn("See https://bugzilla.redhat.com/show_bug.cgi?id=1378529")
            break

    return lines


def _format_result_value(bench):
    mean = bench.mean()
    if bench.get_nvalue() >= 2:
        args = bench.format_values((mean, bench.stdev()))
        return '%s +- %s' % args
    else:
        return bench.format_value(mean)


def format_result_value(bench):
    loops = None
    warmups = None
    for run in bench._runs:
        if run._is_calibration_warmups():
            warmups = run._get_calibration_warmups()
        elif run._is_recalibration_warmups():
            warmups = run._get_calibration_warmups()
        elif run._is_recalibration_loops():
            loops = run._get_calibration_loops()
        elif run._is_calibration_warmups():
            loops = run._get_calibration_loops()
        elif run._is_calibration_loops():
            loops = run._get_calibration_loops()
        else:
            loops = None
            warmups = None
            break

    info = []
    if loops is not None:
        info.append(format_number(loops, 'loop'))
    if warmups is not None:
        info.append(format_number(warmups, 'warmup'))
    if info:
        return '<calibration: %s>' % ', '.join(info)

    return _format_result_value(bench)


def format_result(bench):
    loops = None
    warmups = None
    for run in bench._runs:
        if run._is_calibration_warmups():
            warmups = run._get_calibration_warmups()
        elif run._is_recalibration_warmups():
            warmups = run._get_calibration_warmups()
        elif run._is_recalibration_loops():
            loops = run._get_calibration_loops()
        elif run._is_calibration_warmups():
            loops = run._get_calibration_loops()
        elif run._is_calibration_loops():
            loops = run._get_calibration_loops()
        else:
            loops = None
            warmups = None
            break

    info = []
    if loops is not None:
        info.append(format_number(loops, 'loop'))
    if warmups is not None:
        info.append(format_number(warmups, 'warmup'))
    if info:
        return 'Calibration: %s' % ', '.join(info)

    text = _format_result_value(bench)
    if bench.get_nvalue() >= 2:
        return 'Mean +- std dev: %s' % text
    else:
        return text


def format_benchmark(bench, checks=True, metadata=False,
                     dump=False, stats=False, hist=False, show_name=False,
                     result=True, display_runs_args=None, only_checks=False):
    lines = []

    if metadata:
        lines.append("Metadata:")
        format_metadata(bench.get_metadata(), lines=lines)

    if dump:
        if display_runs_args is None:
            display_runs_args = {}
        _format_runs(bench, lines=lines, **display_runs_args)

    if hist:
        format_histogram([(bench, None)], lines=lines)

    if stats:
        format_stats(bench, lines=lines)

    if checks:
        format_checks(bench, lines=lines, check_too_many_processes=only_checks)

    if result:
        empty_line(lines)

        text = format_result(bench)
        if show_name:
            name = bench.get_name()
            text = "%s: %s" % (name, text)
        lines.append(text)

    return lines


# FIXME: remove this function?
def multiline_output(args):
    return (args.hist or args.stats or args.dump or args.metadata)


@contextlib.contextmanager
def catch_broken_pipe_error(file=None):
    if file is None:
        files = [sys.stdout, sys.stderr]
    else:
        files = [file]

    try:
        for file in files:
            file.flush()

        yield

        # Flush files to be able to catch a broken pipe error if the pipe
        # was closed by the consumer
        for file in files:
            file.flush()
    except OSError as exc:
        if exc.errno != errno.EPIPE:
            raise
        # got a broken pipe error: ignore it

        # explicitly close files to prevent broken pipe error on implicit
        # close at exit which would log the error:
        # "Exception ignored in: ... BrokenPipeError: ..."
        for file in files:
            try:
                file.close()
            except OSError:
                pass
