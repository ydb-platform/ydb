import csv
import math
import os.path
import statistics

import pyperf

NO_VERSION = "<not set>"


class VersionMismatchError(Exception):
    def __init__(self, version1, version2):
        super().__init__(
            f"Performance versions are different ({version1} != {version2})",
        )
        self.version1 = version1
        self.version2 = version2


def format_result(bench):
    mean = bench.mean()
    if bench.get_nvalue() >= 2:
        args = bench.format_values((mean, bench.stdev()))
        return "Mean +- std dev: %s +- %s" % args
    else:
        return bench.format_value(mean)


# A table of 95% confidence intervals for a two-tailed t distribution, as a
# function of the degrees of freedom. For larger degrees of freedom, we
# approximate. While this may look less elegant than simply calculating the
# critical value, those calculations suck. Look at
# http://www.math.unb.ca/~knight/utility/t-table.htm if you need more values.
_T_DIST_95_CONF_LEVELS = [
    0,
    12.706,
    4.303,
    3.182,
    2.776,
    2.571,
    2.447,
    2.365,
    2.306,
    2.262,
    2.228,
    2.201,
    2.179,
    2.160,
    2.145,
    2.131,
    2.120,
    2.110,
    2.101,
    2.093,
    2.086,
    2.080,
    2.074,
    2.069,
    2.064,
    2.060,
    2.056,
    2.052,
    2.048,
    2.045,
    2.042,
]


def tdist95conf_level(df):
    """Approximate the 95% confidence interval for Student's T distribution.

    Given the degrees of freedom, returns an approximation to the 95%
    confidence interval for the Student's T distribution.

    Args:
        df: An integer, the number of degrees of freedom.

    Returns:
        A float.
    """
    df = int(round(df))
    highest_table_df = len(_T_DIST_95_CONF_LEVELS)
    if df >= 200:
        return 1.960
    if df >= 100:
        return 1.984
    if df >= 80:
        return 1.990
    if df >= 60:
        return 2.000
    if df >= 50:
        return 2.009
    if df >= 40:
        return 2.021
    if df >= highest_table_df:
        return _T_DIST_95_CONF_LEVELS[highest_table_df - 1]
    return _T_DIST_95_CONF_LEVELS[df]


def pooled_sample_variance(sample1, sample2):
    """Find the pooled sample variance for two samples.

    Args:
        sample1: one sample.
        sample2: the other sample.

    Returns:
        Pooled sample variance, as a float.
    """
    deg_freedom = len(sample1) + len(sample2) - 2
    mean1 = statistics.mean(sample1)
    squares1 = ((x - mean1) ** 2 for x in sample1)
    mean2 = statistics.mean(sample2)
    squares2 = ((x - mean2) ** 2 for x in sample2)

    return (math.fsum(squares1) + math.fsum(squares2)) / float(deg_freedom)


def tscore(sample1, sample2):
    """Calculate a t-test score for the difference between two samples.

    Args:
        sample1: one sample.
        sample2: the other sample.

    Returns:
        The t-test score, as a float.
    """
    if len(sample1) != len(sample2):
        raise ValueError("different number of values")
    error = pooled_sample_variance(sample1, sample2) / len(sample1)
    diff = statistics.mean(sample1) - statistics.mean(sample2)
    return diff / math.sqrt(error * 2)


def is_significant(sample1, sample2):
    """Determine whether two samples differ significantly.

    This uses a Student's two-sample, two-tailed t-test with alpha=0.95.

    Args:
        sample1: one sample.
        sample2: the other sample.

    Returns:
        (significant, t_score) where significant is a bool indicating whether
        the two samples differ significantly; t_score is the score from the
        two-sample T test.
    """
    deg_freedom = len(sample1) + len(sample2) - 2
    critical_value = tdist95conf_level(deg_freedom)
    t_score = tscore(sample1, sample2)
    return (abs(t_score) >= critical_value, t_score)


def significant_msg(base, changed):
    if base.get_nvalue() < 2 or changed.get_nvalue() < 2:
        return "(benchmark only contains a single value)"

    avg_base = base.mean()
    avg_changed = changed.mean()

    msg = "Not significant"
    significant = False

    # Due to inherent measurement imprecisions, variations of less than 1%
    # are automatically considered insignificant. This helps present
    # a clear picture to the user.
    if abs(avg_base - avg_changed) > (avg_base + avg_changed) * 0.01:
        base_times = base.get_values()
        changed_times = changed.get_values()

        significant, t_score = is_significant(base_times, changed_times)
        if significant:
            msg = "Significant (t=%.2f)" % t_score

    return msg


def format_table(base_label, changed_label, results):
    table = [("Benchmark", base_label, changed_label, "Change", "Significance")]

    for bench_name, result in results:
        format_value = result.base.format_value
        avg_base = result.base.mean()
        avg_changed = result.changed.mean()
        delta_avg = quantity_delta(result.base, result.changed)
        msg = significant_msg(result.base, result.changed)
        table.append(
            (
                bench_name,
                # Limit the precision for conciseness in the table.
                format_value(avg_base),
                format_value(avg_changed),
                delta_avg,
                msg,
            )
        )

    # Columns with None values are skipped
    skipped_cols = set()
    col_widths = [0] * len(table[0])
    for row in table:
        for col, val in enumerate(row):
            if val is None:
                skipped_cols.add(col)
                continue
            col_widths[col] = max(col_widths[col], len(val))

    outside_line = "+"
    header_sep_line = "+"
    for col, width in enumerate(col_widths):
        if col in skipped_cols:
            continue
        width += 2  # Compensate for the left and right padding spaces.
        outside_line += "-" * width + "+"
        header_sep_line += "=" * width + "+"

    output = [outside_line]
    for row_i, row in enumerate(table):
        output_row = []
        for col_i, val in enumerate(row):
            if col_i in skipped_cols:
                continue
            output_row.append("| " + val.ljust(col_widths[col_i]) + " ")
        output.append("".join(output_row) + "|")
        if row_i > 0:
            output.append(outside_line)

    output.insert(2, "".join(header_sep_line))
    return "\n".join(output)


class BenchmarkResult(object):
    """An object representing data from a succesful benchmark run."""

    def __init__(self, base, changed):
        name = base.get_name()
        name2 = changed.get_name()
        if name2 != name:
            raise ValueError("not the same benchmark: %s != %s" % (name, name2))

        if base.get_nvalue() != changed.get_nvalue():
            raise RuntimeError("base and changed don't have the same number of values")

        self.base = base
        self.changed = changed

    def __str__(self):
        if self.base.get_nvalue() > 1:
            values = (
                self.base.mean(),
                self.base.stdev(),
                self.changed.mean(),
                self.changed.stdev(),
            )
            text = "%s +- %s -> %s +- %s" % self.base.format_values(values)

            msg = significant_msg(self.base, self.changed)
            delta_avg = quantity_delta(self.base, self.changed)
            return "Mean +- std dev: %s: %s\n%s" % (text, delta_avg, msg)
        else:
            format_value = self.base.format_value
            base = self.base.mean()
            changed = self.changed.mean()
            delta_avg = quantity_delta(self.base, self.changed)
            return "%s -> %s: %s" % (
                format_value(base),
                format_value(changed),
                delta_avg,
            )


def quantity_delta(base, changed):
    old = base.mean()
    new = changed.mean()
    is_time = base.get_unit() == "second"

    if old == 0 or new == 0:
        return "incomparable (one result was zero)"
    if new > old:
        if is_time:
            return "%.2fx slower" % (new / old)
        else:
            return "%.2fx larger" % (new / old)
    elif new < old:
        if is_time:
            return "%.2fx faster" % (old / new)
        else:
            return "%.2fx smaller" % (old / new)
    else:
        return "no change"


def display_suite_metadata(suite, title=None):
    metadata = suite.get_metadata()
    empty = True
    for key, fmt in (
        ("performance_version", "Performance version: %s"),
        ("python_version", "Python version: %s"),
        ("platform", "Report on %s"),
        ("cpu_count", "Number of logical CPUs: %s"),
    ):
        if key not in metadata:
            continue

        empty = False
        if title:
            print(title)
            print("=" * len(title))
            print()
            title = None

        text = fmt % metadata[key]
        print(text)

    dates = suite.get_dates()
    if dates:
        print("Start date: %s" % dates[0].isoformat(" "))
        print("End date: %s" % dates[1].isoformat(" "))
        empty = False

    if not empty:
        print()


def display_benchmark_suite(suite):
    display_suite_metadata(suite)

    for bench in suite.get_benchmarks():
        print("### %s ###" % bench.get_name())
        print(format_result(bench))
        print()


def get_labels(filename1, filename2):
    # Find a short label to identify two filenames:
    # the two labels must be different
    name1 = os.path.basename(filename1)
    name2 = os.path.basename(filename2)
    if name1 != name2:
        return (name1, name2)

    return (filename1, filename2)


def compare_results(options):
    base_label, changed_label = get_labels(
        options.baseline_filename, options.changed_filename
    )

    base_suite = pyperf.BenchmarkSuite.load(options.baseline_filename)
    changed_suite = pyperf.BenchmarkSuite.load(options.changed_filename)

    results = []
    common = set(base_suite.get_benchmark_names()) & set(
        changed_suite.get_benchmark_names()
    )
    for name in sorted(common):
        base_bench = base_suite.get_benchmark(name)
        changed_bench = changed_suite.get_benchmark(name)
        result = BenchmarkResult(base_bench, changed_bench)
        results.append(result)

    hidden = []
    shown = []
    for result in results:
        name = result.base.get_name()

        significant = significant_msg(result.base, result.changed)
        if significant or options.verbose:
            shown.append((name, result))
        else:
            hidden.append((name, result))

    display_suite_metadata(base_suite, title=base_label)
    display_suite_metadata(changed_suite, title=changed_label)

    if options.output_style == "normal":
        for index, item in enumerate(shown):
            if index:
                print()
            name, result = item
            print("###", name, "###")
            print(result)

    elif options.output_style == "table":
        if shown:
            print(format_table(base_label, changed_label, shown))
    else:
        raise ValueError("Invalid output_style: %r" % options.output_style)

    if hidden:
        print()
        print("The following not significant results are hidden, use -v to show them:")
        print(", ".join(name for (name, result) in hidden) + ".")

    only_base = set(base_suite.get_benchmark_names()) - common
    if only_base:
        print()
        print(
            "Skipped %s benchmarks only in %s: %s"
            % (len(only_base), base_label, ", ".join(sorted(only_base)))
        )

    only_changed = set(changed_suite.get_benchmark_names()) - common
    if only_changed:
        print()
        print(
            "Skipped %s benchmarks only in %s: %s"
            % (len(only_changed), changed_label, ", ".join(sorted(only_changed)))
        )

    version1 = base_suite.get_metadata().get("performance_version", NO_VERSION)
    version2 = changed_suite.get_metadata().get("performance_version", NO_VERSION)
    if version1 != version2 or (version1 == version2 == NO_VERSION):
        raise VersionMismatchError(version1, version2)

    return results


def format_csv(value):
    abs_value = abs(value)
    # keep at least 3 significant digits, but also try to avoid too many zeros
    if abs_value >= 1.0:
        return "%.2f" % value
    elif abs_value >= 1e-3:
        return "%.5f" % value
    elif abs_value >= 1e-6:
        return "%.8f" % value
    else:
        return "%.11f" % value


def write_csv(results, filename):
    with open(filename, "w", newline="", encoding="ascii") as fp:
        writer = csv.writer(fp)
        writer.writerow(["Benchmark", "Base", "Changed"])
        for result in results:
            name = result.base.get_name()
            base = result.base.mean()
            changed = result.changed.mean()
            row = [name, format_csv(base), format_csv(changed)]
            writer.writerow(row)
