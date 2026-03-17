from pyperf._cli import display_title, format_result_value
from pyperf._utils import is_significant, geometric_mean


def is_significant_benchs(bench1, bench2):
    values1 = bench1.get_values()
    values2 = bench2.get_values()

    if len(values1) == 1 and len(values2) == 1:
        # FIXME: is it ok to consider that comparison between two values
        # is significant?
        return (True, None)

    try:
        significant, t_score = is_significant(values1, values2)
        return (significant, t_score)
    except Exception:
        # FIXME: fix the root bug, don't work around it
        return (True, None)


class CompareData:
    def __init__(self, name, benchmark):
        self.name = name
        self.benchmark = benchmark

    def __repr__(self):
        return '<CompareData name=%r value#=%s>' % (self.name, self.benchmark.get_nvalue())


def compute_normalized_mean(bench, ref):
    ref_avg = ref.mean()
    bench_avg = bench.mean()
    # Note: means cannot be zero, it's a warranty of pyperf API
    return bench_avg / ref_avg


def format_normalized_mean(norm_mean):
    if norm_mean == 1.0:
        return "no change"
    elif norm_mean < 1.0:
        return "%.2fx faster" % (1.0 / norm_mean)
    else:
        return "%.2fx slower" % norm_mean


def format_geometric_mean(norm_means):
    geo_mean = geometric_mean(norm_means)
    return format_normalized_mean(geo_mean)


def get_tags_for_result(result):
    return result.ref.benchmark.get_metadata().get("tags", [])


class CompareResult:
    def __init__(self, ref, changed, min_speed=None):
        # CompareData object
        self.ref = ref
        # CompareData object
        self.changed = changed
        self._min_speed = min_speed
        self._significant = None
        self._t_score = None
        self._norm_mean = None

    def __repr__(self):
        return '<CompareResult ref=%r changed=%r>' % (self.ref, self.changed)

    def _set_significant(self):
        bench1 = self.ref.benchmark
        bench2 = self.changed.benchmark
        self._significant, self._t_score = is_significant_benchs(bench1, bench2)

        if self._min_speed is not None:
            norm_mean = self.norm_mean
            if norm_mean < 1.0:
                # faster uses the inverse
                norm_mean = 1.0 / norm_mean
            if (norm_mean - 1.0) * 100 < self._min_speed:
                self._significant = False

    @property
    def significant(self):
        if self._significant is None:
            self._set_significant()
        return self._significant

    @property
    def t_score(self):
        if self._significant is None:
            self._set_significant()
        return self._t_score

    def _compute_norm_mean(self):
        ref = self.ref.benchmark
        bench = self.changed.benchmark
        self._norm_mean = compute_normalized_mean(bench, ref)

    # mean normalized to the reference benchmark mean
    @property
    def norm_mean(self):
        if self._norm_mean is None:
            self._compute_norm_mean()
        return self._norm_mean

    def oneliner(self, verbose=True, show_name=True, check_significant=True):
        if check_significant and not self.significant:
            return "Not significant!"

        ref_text = format_result_value(self.ref.benchmark)
        chg_text = format_result_value(self.changed.benchmark)
        if verbose:
            if show_name:
                ref_text = "[%s] %s" % (self.ref.name, ref_text)
                chg_text = "[%s] %s" % (self.changed.name, chg_text)
            if (self.ref.benchmark.get_nvalue() > 1
               or self.changed.benchmark.get_nvalue() > 1):
                text = "Mean +- std dev: %s -> %s" % (ref_text, chg_text)
            else:
                text = "%s -> %s" % (ref_text, chg_text)
        else:
            text = "%s -> %s" % (ref_text, chg_text)

        text = "%s: %s" % (text, format_normalized_mean(self.norm_mean))
        return text

    def format(self, verbose=True, show_name=True):
        text = self.oneliner(show_name=show_name, check_significant=False)
        lines = [text]

        # significant?
        if self.t_score is None:
            lines.append("ERROR when testing if values are significant")

        if self.significant:
            if verbose:
                if self.t_score is not None:
                    lines.append("Significant (t=%.2f)" % self.t_score)
                else:
                    lines.append("Significant")
        else:
            lines.append("Not significant!")
        return lines


class CompareResults(list):
    # list of CompareResult objects
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<CompareResult %r>' % (list(self),)


class ReSTTable:
    def __init__(self, headers, rows):
        self.headers = headers
        self.rows = rows
        self.widths = [len(header) for header in self.headers]
        for row in self.rows:
            for column, cell in enumerate(row):
                self.widths[column] = max(self.widths[column], len(cell))

    def _render_line(self, char='-'):
        parts = ['']
        for width in self.widths:
            parts.append(char * (width + 2))
        parts.append('')
        return '+'.join(parts)

    def _render_row(self, row):
        parts = ['']
        for width, cell in zip(self.widths, row):
            parts.append(' %s ' % cell.ljust(width))
        parts.append('')
        return '|'.join(parts)

    def render(self, write_line):
        write_line(self._render_line('-'))
        write_line(self._render_row(self.headers))
        write_line(self._render_line('='))
        for row in self.rows:
            write_line(self._render_row(row))
            write_line(self._render_line('-'))


class MarkDownTable:
    def __init__(self, headers, rows):
        self.headers = headers
        self.rows = rows
        self.widths = [len(header) for header in self.headers]
        for row in self.rows:
            for column, cell in enumerate(row):
                self.widths[column] = max(self.widths[column], len(cell))

    def _render_line(self, char='-'):
        parts = ['']
        for idx, width in enumerate(self.widths):
            if idx == 0:
                parts.append(char * (width + 2))
            else:
                parts.append(f':{char * width}:')
        parts.append('')
        return '|'.join(parts)

    def _render_row(self, row):
        parts = ['']
        for width, cell in zip(self.widths, row):
            parts.append(" %s " % cell.ljust(width))
        parts.append('')
        return '|'.join(parts)

    def render(self, write_line):
        write_line(self._render_row(self.headers))
        write_line(self._render_line('-'))
        for row in self.rows:
            write_line(self._render_row(row))


class CompareError(Exception):
    pass


class CompareSuites:
    def __init__(self, benchmarks, args):
        self.benchmarks = benchmarks

        self.table = args.table
        self.table_format = args.table_format
        self.min_speed = args.min_speed
        self.group_by_speed = args.group_by_speed
        self.verbose = args.verbose
        self.quiet = args.quiet

        grouped_by_name = self.benchmarks.group_by_name()
        if not grouped_by_name:
            raise CompareError("Benchmark suites have no benchmark in common")

        # List of CompareResults
        self.all_results = []
        for item in grouped_by_name:
            cmp_benchmarks = item.benchmarks
            results = self.compare_benchmarks(item.name, cmp_benchmarks)
            self.all_results.append(results)

        self.show_name = (len(grouped_by_name) > 1)

        self.tags = set()
        for results in self.all_results:
            for result in results:
                self.tags.update(get_tags_for_result(result))
        self.tags = sorted(list(self.tags))

    def compare_benchmarks(self, name, benchmarks):
        min_speed = self.min_speed

        results = CompareResults(name)

        ref_item = benchmarks[0]
        ref = CompareData(ref_item.filename, ref_item.benchmark)

        for item in benchmarks[1:]:
            changed = CompareData(item.filename, item.benchmark)
            result = CompareResult(ref, changed, min_speed)
            results.append(result)

        return results

    @staticmethod
    def display_not_significant(not_significant):
        print("Benchmark hidden because not significant (%s): %s"
              % (len(not_significant), ', '.join(not_significant)))

    def compare_suites_table(self, all_results):
        if self.group_by_speed:
            def sort_key(results):
                result = results[0]
                return result.norm_mean

            self.all_results.sort(key=sort_key)

        headers = ['Benchmark', self.all_results[0][0].ref.name]
        for item in self.all_results[0]:
            headers.append(item.changed.name)

        all_norm_means = [[] for _ in range(len(headers[2:]))]

        rows = []
        not_significant = []
        for results in all_results:
            row = [results.name]

            ref_bench = results[0].ref.benchmark
            text = ref_bench.format_value(ref_bench.mean())
            row.append(text)

            significants = []
            for index, result in enumerate(results):
                bench = result.changed.benchmark
                significant = result.significant
                if significant:
                    text = format_normalized_mean(result.norm_mean)
                    if not self.quiet:
                        text = "%s: %s" % (bench.format_value(bench.mean()), text)
                else:
                    text = "not significant"
                significants.append(significant)
                all_norm_means[index].append(result.norm_mean)
                row.append(text)

            if any(significants):
                rows.append(row)
            else:
                not_significant.append(results.name)

        # only compute the geometric mean if there is at least two benchmarks
        # and if at least one is signicant.
        if len(all_norm_means[0]) > 1 and rows:
            row = ['Geometric mean', '(ref)']
            for norm_means in all_norm_means:
                row.append(format_geometric_mean(norm_means))
            rows.append(row)

        if rows:
            if self.table_format == 'rest':
                table = ReSTTable(headers, rows)
            else:
                table = MarkDownTable(headers, rows)
            table.render(print)

        if not_significant:
            if rows:
                print()
            self.display_not_significant(not_significant)

    def compare_suites_by_speed(self, all_results):
        not_significant = []
        slower = []
        faster = []
        same = []
        for results in all_results:
            result = results[0]
            if not result.significant:
                not_significant.append(results.name)
                continue

            item = (results.name, result)
            norm_mean = result.norm_mean
            if norm_mean == 1.0:
                same.append(item)
            elif norm_mean < 1.0:
                faster.append(item)
            else:
                slower.append(item)

        def sort_key(item):
            return item[1].norm_mean

        slower.sort(key=sort_key, reverse=True)
        faster.sort(key=sort_key)

        empty_line = False
        for title, results, sort_reverse in (
            ('Slower', slower, True),
            ('Faster', faster, False),
            ('Same speed', same, False),
        ):
            if not results:
                continue

            if empty_line:
                print()
            print("%s (%s):" % (title, len(results)))
            for name, result in results:
                text = result.oneliner(verbose=False)
                print("- %s: %s" % (name, text))
            empty_line = True

        if not self.quiet and not_significant:
            if empty_line:
                print()
            self.display_not_significant(not_significant)

    def compare_suites_list(self, all_results):
        not_significant = []
        empty_line = False
        last_index = (len(self.all_results) - 1)

        for index, results in enumerate(all_results):
            significant = any(result.significant for result in results)
            lines = []
            for result in results:
                lines.extend(result.format(self.verbose))

            if not (significant or self.verbose):
                not_significant.append(results.name)
                continue

            if len(lines) != 1:
                if self.show_name:
                    display_title(results.name)
                for line in lines:
                    print(line)
                if index != last_index:
                    print()
            else:
                text = lines[0]
                if self.show_name:
                    text = '%s: %s' % (results.name, text)
                print(text)
            empty_line = True

        if not self.quiet and not_significant:
            if empty_line:
                print()
            self.display_not_significant(not_significant)

    def list_ignored(self):
        for suite, hidden in self.benchmarks.group_by_name_ignored():
            if not hidden:
                continue
            hidden_names = [bench.get_name() for bench in hidden]
            print("Ignored benchmarks (%s) of %s: %s"
                  % (len(hidden), suite.filename, ', '.join(sorted(hidden_names))))

    def compare_geometric_mean(self, all_results):
        # use a list since two filenames can be identical,
        # even if results are different
        all_norm_means = []
        for item in all_results[0]:
            all_norm_means.append((item.changed.name, []))

        for results in all_results:
            for index, result in enumerate(results):
                all_norm_means[index][1].append(result.norm_mean)

        if len(all_norm_means[0][1]) < 2:
            # only compute the geometric mean when there is at least two benchmarks
            return

        print()
        if len(all_norm_means) > 1:
            display_title('Geometric mean')
            for name, norm_means in all_norm_means:
                geo_mean = format_geometric_mean(norm_means)
                print(f'{name}: {geo_mean}')
        else:
            geo_mean = format_geometric_mean(all_norm_means[0][1])
            print(f'Geometric mean: {geo_mean}')

    def compare_suites(self, results):
        if self.table:
            self.compare_suites_table(results)
        else:
            if self.group_by_speed:
                self.compare_suites_by_speed(results)
            else:
                self.compare_suites_list(results)

            self.compare_geometric_mean(results)

    def compare(self):
        if len(self.tags):
            for tag in self.tags:
                display_title(f"Benchmarks with tag '{tag}':")
                all_results = [
                    results for results in self.all_results
                    if tag is None or tag in get_tags_for_result(results[0])
                ]
                self.compare_suites(all_results)
                print()
            display_title("All benchmarks:")
        self.compare_suites(self.all_results)

        if not self.quiet:
            self.list_ignored()


def compare_suites(benchmarks, args):
    CompareSuites(benchmarks, args).compare()


def timeit_compare_benchs(name1, bench1, name2, bench2, args):
    data1 = CompareData(name1, bench1)
    data2 = CompareData(name2, bench2)
    compare = CompareResult(data1, data2)
    if not args.quiet:
        lines = compare.format(verbose=args.verbose)
        for line in lines:
            print(line)
    else:
        line = compare.oneliner()
        print(line)
