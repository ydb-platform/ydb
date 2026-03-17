import math
import unittest

from prometheus_client.core import (
    CollectorRegistry, CounterMetricFamily, GaugeMetricFamily,
    HistogramMetricFamily, Metric, Sample, SummaryMetricFamily,
)
from prometheus_client.exposition import generate_latest
from prometheus_client.openmetrics.exposition import ALLOWUTF8
from prometheus_client.parser import text_string_to_metric_families


class TestParse(unittest.TestCase):
    def assertEqualMetrics(self, first, second, msg=None):
        super().assertEqual(first, second, msg)

        # Test that samples are actually named tuples of type Sample.
        for a, b in zip(first, second):
            for sa, sb in zip(a.samples, b.samples):
                assert sa.name == sb.name

    def test_simple_counter(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a 1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "help", value=1)], list(families))

    def test_utf8_counter(self):
        families = text_string_to_metric_families("""# TYPE "a.b" counter
# HELP "a.b" help
{"a.b"} 1
""")
        self.assertEqualMetrics([CounterMetricFamily("a.b", "help", value=1)], list(families))

    def test_complex_name_counter(self):
        families = text_string_to_metric_families("""# TYPE "my.counter{} # = \\" \\n" counter
# HELP "my.counter{} # = \\" \\n" help
{"my.counter{} # = \\" \\n", "awful. }}{{ # HELP EOF name"="\\n yikes } \\" value"} 1
""")
        metric = CounterMetricFamily("my.counter{} # = \" \n", "help", labels={'awful. }}{{ # HELP EOF name': '\n yikes } " value'})
        metric.add_sample("my.counter{} # = \" \n_total", {'awful. }}{{ # HELP EOF name': '\n yikes } " value'}, 1)
        self.assertEqual([metric], list(families))

    def test_simple_gauge(self):
        families = text_string_to_metric_families("""# TYPE a gauge
# HELP a help
a 1
""")
        self.assertEqualMetrics([GaugeMetricFamily("a", "help", value=1)], list(families))

    def test_simple_summary(self):
        families = text_string_to_metric_families("""# TYPE a summary
# HELP a help
a_count 1
a_sum 2
""")
        summary = SummaryMetricFamily("a", "help", count_value=1, sum_value=2)
        self.assertEqualMetrics([summary], list(families))

    def test_summary_quantiles(self):
        families = text_string_to_metric_families("""# TYPE a summary
# HELP a help
a_count 1
a_sum 2
a{quantile="0.5"} 0.7
""")
        # The Python client doesn't support quantiles, but we
        # still need to be able to parse them.
        metric_family = SummaryMetricFamily("a", "help", count_value=1, sum_value=2)
        metric_family.add_sample("a", {"quantile": "0.5"}, 0.7)
        self.assertEqualMetrics([metric_family], list(families))

    def test_simple_histogram(self):
        families = text_string_to_metric_families("""# TYPE a histogram
# HELP a help
a_bucket{le="1"} 0
a_bucket{le="+Inf"} 3
a_count 3
a_sum 2
""")
        self.assertEqualMetrics([HistogramMetricFamily("a", "help", sum_value=2, buckets=[("1", 0.0), ("+Inf", 3.0)])],
                                list(families))

    def test_no_metadata(self):
        families = text_string_to_metric_families("""a 1
""")
        metric_family = Metric("a", "", "untyped")
        metric_family.add_sample("a", {}, 1)
        self.assertEqualMetrics([metric_family], list(families))

    def test_untyped(self):
        # https://github.com/prometheus/client_python/issues/79
        families = text_string_to_metric_families("""# HELP redis_connected_clients Redis connected clients
# TYPE redis_connected_clients untyped
redis_connected_clients{instance="rough-snowflake-web",port="6380"} 10.0
redis_connected_clients{instance="rough-snowflake-web",port="6381"} 12.0
""")
        m = Metric("redis_connected_clients", "Redis connected clients", "untyped")
        m.samples = [
            Sample("redis_connected_clients", {"instance": "rough-snowflake-web", "port": "6380"}, 10),
            Sample("redis_connected_clients", {"instance": "rough-snowflake-web", "port": "6381"}, 12),
        ]
        self.assertEqualMetrics([m], list(families))

    def test_type_help_switched(self):
        families = text_string_to_metric_families("""# HELP a help
# TYPE a counter
a 1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "help", value=1)], list(families))

    def test_blank_lines_and_comments(self):
        families = text_string_to_metric_families("""
# TYPE a counter
# FOO a
# BAR b
# HELP a help

a 1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "help", value=1)], list(families))

    def test_comments_parts_are_not_validated_against_legacy_metric_name(self):
        # https://github.com/prometheus/client_python/issues/1108
        families = text_string_to_metric_families("""
# A simple. comment line where third token cannot be matched against METRIC_NAME_RE under validation.py
# 3565 12345/4436467 another random comment line where third token cannot be matched against METRIC_NAME_RE under validation.py
""")
        self.assertEqualMetrics([], list(families))

    def test_extra_whitespace(self):
        families = text_string_to_metric_families("""# TYPE  a  counter
# HELP  a  help
a  1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "help", value=1)], list(families))

    def test_tabs(self):
        families = text_string_to_metric_families("""#\tTYPE\ta\tcounter
#\tHELP\ta\thelp
a\t1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "help", value=1)], list(families))

    def test_labels_with_curly_braces(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{foo="bar", bar="b{a}z"} 1
""")
        metric_family = CounterMetricFamily("a", "help", labels=["foo", "bar"])
        metric_family.add_metric(["bar", "b{a}z"], 1)
        self.assertEqualMetrics([metric_family], list(families))

    def test_empty_help(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a
a 1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "", value=1)], list(families))

    def test_labels_and_infinite(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{foo="bar"} +Inf
a{foo="baz"} -Inf
""")
        metric_family = CounterMetricFamily("a", "help", labels=["foo"])
        metric_family.add_metric(["bar"], float('inf'))
        metric_family.add_metric(["baz"], float('-inf'))
        self.assertEqualMetrics([metric_family], list(families))

    def test_spaces(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{ foo = "bar" } 1
a\t\t{\t\tfoo\t\t=\t\t"baz"\t\t}\t\t2
a   {    foo   =  "buz"   }    3
a\t {  \t foo\t = "biz"\t  } \t 4
a \t{\t foo   = "boz"\t}\t 5
a{foo="bez"}6
""")
        metric_family = CounterMetricFamily("a", "help", labels=["foo"])
        metric_family.add_metric(["bar"], 1)
        metric_family.add_metric(["baz"], 2)
        metric_family.add_metric(["buz"], 3)
        metric_family.add_metric(["biz"], 4)
        metric_family.add_metric(["boz"], 5)
        metric_family.add_metric(["bez"], 6)
        self.assertEqualMetrics([metric_family], list(families))

    def test_commas(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{foo="bar",} 1
a{foo="baz",  } 1
# TYPE b counter
# HELP b help
b{,} 2
# TYPE c counter
# HELP c help
c{  ,} 3
# TYPE d counter
# HELP d help
d{,  } 4
""")
        a = CounterMetricFamily("a", "help", labels=["foo"])
        a.add_metric(["bar"], 1)
        a.add_metric(["baz"], 1)
        b = CounterMetricFamily("b", "help", value=2)
        c = CounterMetricFamily("c", "help", value=3)
        d = CounterMetricFamily("d", "help", value=4)
        self.assertEqualMetrics([a, b, c, d], list(families))

    def test_multiple_trailing_commas(self):
        text = """# TYPE a counter
# HELP a help
a{foo="bar",, } 1
"""
        self.assertRaises(ValueError,
                          lambda: list(text_string_to_metric_families(text)))

    def test_empty_brackets(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{} 1
""")
        self.assertEqualMetrics([CounterMetricFamily("a", "help", value=1)], list(families))

    def test_nan(self):
        families = text_string_to_metric_families("""a NaN
""")
        # Can't use a simple comparison as nan != nan.
        self.assertTrue(math.isnan(list(families)[0].samples[0][2]))

    def test_empty_label(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{foo="bar"} 1
a{foo=""} 2
""")
        metric_family = CounterMetricFamily("a", "help", labels=["foo"])
        metric_family.add_metric(["bar"], 1)
        metric_family.add_metric([""], 2)
        self.assertEqualMetrics([metric_family], list(families))

    def test_label_escaping(self):
        for escaped_val, unescaped_val in [
            ('foo', 'foo'),
            ('\\foo', '\\foo'),
            ('\\\\foo', '\\foo'),
            ('foo\\\\', 'foo\\'),
            ('\\\\', '\\'),
            ('\\n', '\n'),
            ('\\\\n', '\\n'),
            ('\\\\\\n', '\\\n'),
            ('\\"', '"'),
            ('\\\\\\"', '\\"')]:
            families = list(text_string_to_metric_families("""
# TYPE a counter
# HELP a help
a{foo="%s",bar="baz"} 1
""" % escaped_val))
            metric_family = CounterMetricFamily(
                "a", "help", labels=["foo", "bar"])
            metric_family.add_metric([unescaped_val, "baz"], 1)
            self.assertEqualMetrics([metric_family], list(families))

    def test_help_escaping(self):
        for escaped_val, unescaped_val in [
            ('foo', 'foo'),
            ('\\foo', '\\foo'),
            ('\\\\foo', '\\foo'),
            ('foo\\', 'foo\\'),
            ('foo\\\\', 'foo\\'),
            ('\\n', '\n'),
            ('\\\\n', '\\n'),
            ('\\\\\\n', '\\\n'),
            ('\\"', '\\"'),
            ('\\\\"', '\\"'),
            ('\\\\\\"', '\\\\"')]:
            families = list(text_string_to_metric_families("""
# TYPE a counter
# HELP a %s
a{foo="bar"} 1
""" % escaped_val))
            metric_family = CounterMetricFamily("a", unescaped_val, labels=["foo"])
            metric_family.add_metric(["bar"], 1)
            self.assertEqualMetrics([metric_family], list(families))

    def test_escaping(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a he\\n\\\\l\\tp
a{foo="b\\"a\\nr"} 1
a{foo="b\\\\a\\z"} 2
""")
        metric_family = CounterMetricFamily("a", "he\n\\l\\tp", labels=["foo"])
        metric_family.add_metric(["b\"a\nr"], 1)
        metric_family.add_metric(["b\\a\\z"], 2)
        self.assertEqualMetrics([metric_family], list(families))

    def test_timestamps(self):
        families = text_string_to_metric_families("""# TYPE a counter
# HELP a help
a{foo="bar"} 1\t000
# TYPE b counter
# HELP b help
b 2  1234567890
b 88   1234566000   
""")
        a = CounterMetricFamily("a", "help", labels=["foo"])
        a.add_metric(["bar"], 1, timestamp=0)
        b = CounterMetricFamily("b", "help")
        b.add_metric([], 2, timestamp=1234567.89)
        b.add_metric([], 88, timestamp=1234566)
        self.assertEqualMetrics([a, b], list(families))

    def test_roundtrip(self):
        text = """# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.013300656000000001
go_gc_duration_seconds{quantile="0.25"} 0.013638736
go_gc_duration_seconds{quantile="0.5"} 0.013759906
go_gc_duration_seconds{quantile="0.75"} 0.013962066
go_gc_duration_seconds{quantile="1"} 0.021383540000000003
go_gc_duration_seconds_sum 56.12904785
go_gc_duration_seconds_count 7476.0
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 166.0
# HELP prometheus_local_storage_indexing_batch_duration_milliseconds Quantiles for batch indexing duration in milliseconds.
# TYPE prometheus_local_storage_indexing_batch_duration_milliseconds summary
prometheus_local_storage_indexing_batch_duration_milliseconds{quantile="0.5"} NaN
prometheus_local_storage_indexing_batch_duration_milliseconds{quantile="0.9"} NaN
prometheus_local_storage_indexing_batch_duration_milliseconds{quantile="0.99"} NaN
prometheus_local_storage_indexing_batch_duration_milliseconds_sum 871.5665949999999
prometheus_local_storage_indexing_batch_duration_milliseconds_count 229.0
# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 29323.4
# HELP process_virtual_memory_bytes Virtual memory size in bytes.
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 2.478268416e+09
# HELP prometheus_build_info A metric with a constant '1' value labeled by version, revision, and branch from which Prometheus was built.
# TYPE prometheus_build_info gauge
prometheus_build_info{branch="HEAD",revision="ef176e5",version="0.16.0rc1"} 1.0
# HELP prometheus_local_storage_chunk_ops_total The total number of chunk operations by their type.
# TYPE prometheus_local_storage_chunk_ops_total counter
prometheus_local_storage_chunk_ops_total{type="clone"} 28.0
prometheus_local_storage_chunk_ops_total{type="create"} 997844.0
prometheus_local_storage_chunk_ops_total{type="drop"} 1.345758e+06
prometheus_local_storage_chunk_ops_total{type="load"} 1641.0
prometheus_local_storage_chunk_ops_total{type="persist"} 981408.0
prometheus_local_storage_chunk_ops_total{type="pin"} 32662.0
prometheus_local_storage_chunk_ops_total{type="transcode"} 980180.0
prometheus_local_storage_chunk_ops_total{type="unpin"} 32662.0
# HELP "my.utf8.metric.#{}=" A fancy metric with dots.
# TYPE "my.utf8.metric.#{}=" summary
{"my.utf8.metric.#{}=",quantile="0"} 0.013300656000000001
{"my.utf8.metric.#{}=",quantile="0.25"} 0.013638736
{"my.utf8.metric.#{}=",quantile="0.5"} 0.013759906
{"my.utf8.metric.#{}=",quantile="0.75"} 0.013962066
{"my.utf8.metric.#{}=",quantile="1"} 0.021383540000000003
{"my.utf8.metric.#{}=_sum"} 56.12904785
{"my.utf8.metric.#{}=_count"} 7476.0
"""
        families = list(text_string_to_metric_families(text))

        class TextCollector:
            def collect(self):
                return families

        registry = CollectorRegistry()
        registry.register(TextCollector())
        self.assertEqual(text.encode('utf-8'), generate_latest(registry, ALLOWUTF8))


def _test_benchmark_text_string_to_metric_families(benchmark):
    text = """# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.013300656000000001
go_gc_duration_seconds{quantile="0.25"} 0.013638736
go_gc_duration_seconds{quantile="0.5"} 0.013759906
go_gc_duration_seconds{quantile="0.75"} 0.013962066
go_gc_duration_seconds{quantile="1"} 0.021383540000000003
go_gc_duration_seconds_sum 56.12904785
go_gc_duration_seconds_count 7476.0
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 166.0
# HELP prometheus_local_storage_indexing_batch_duration_milliseconds Quantiles for batch indexing duration in milliseconds.
# TYPE prometheus_local_storage_indexing_batch_duration_milliseconds summary
prometheus_local_storage_indexing_batch_duration_milliseconds{quantile="0.5"} NaN
prometheus_local_storage_indexing_batch_duration_milliseconds{quantile="0.9"} NaN
prometheus_local_storage_indexing_batch_duration_milliseconds{quantile="0.99"} NaN
prometheus_local_storage_indexing_batch_duration_milliseconds_sum 871.5665949999999
prometheus_local_storage_indexing_batch_duration_milliseconds_count 229.0
# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 29323.4
# HELP process_virtual_memory_bytes Virtual memory size in bytes.
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 2.478268416e+09
# HELP prometheus_build_info A metric with a constant '1' value labeled by version, revision, and branch from which Prometheus was built.
# TYPE prometheus_build_info gauge
prometheus_build_info{branch="HEAD",revision="ef176e5",version="0.16.0rc1"} 1.0
# HELP prometheus_local_storage_chunk_ops_total The total number of chunk operations by their type.
# TYPE prometheus_local_storage_chunk_ops_total counter
prometheus_local_storage_chunk_ops_total{type="clone"} 28.0
prometheus_local_storage_chunk_ops_total{type="create"} 997844.0
prometheus_local_storage_chunk_ops_total{type="drop"} 1.345758e+06
prometheus_local_storage_chunk_ops_total{type="load"} 1641.0
prometheus_local_storage_chunk_ops_total{type="persist"} 981408.0
prometheus_local_storage_chunk_ops_total{type="pin"} 32662.0
prometheus_local_storage_chunk_ops_total{type="transcode"} 980180.0
prometheus_local_storage_chunk_ops_total{type="unpin"} 32662.0
# TYPE hist histogram
# HELP hist help
hist_bucket{le="1"} 0
hist_bucket{le="+Inf"} 3
hist_count 3
hist_sum 2
"""

    @benchmark
    def _():
        # We need to convert the generator to a full list in order to
        # accurately measure the time to yield everything.
        return list(text_string_to_metric_families(text))


if __name__ == '__main__':
    unittest.main()
