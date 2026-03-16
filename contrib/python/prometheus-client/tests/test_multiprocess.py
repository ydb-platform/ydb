import glob
import os
import shutil
import tempfile
import unittest
import warnings

from prometheus_client import mmap_dict, values
from prometheus_client.core import (
    CollectorRegistry, Counter, Gauge, Histogram, Sample, Summary,
)
from prometheus_client.multiprocess import (
    mark_process_dead, MultiProcessCollector,
)
from prometheus_client.values import (
    get_value_class, MultiProcessValue, MutexValue,
)


@unittest.skipIf(True, "warnings disabled")
class TestMultiProcessDeprecation(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        os.environ.pop('prometheus_multiproc_dir', None)
        os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)
        values.ValueClass = MutexValue
        shutil.rmtree(self.tempdir)

    def test_deprecation_warning(self):
        os.environ['prometheus_multiproc_dir'] = self.tempdir
        with warnings.catch_warnings(record=True) as w:
            values.ValueClass = get_value_class()
            registry = CollectorRegistry()
            collector = MultiProcessCollector(registry)
            Counter('c', 'help', registry=None)

            assert os.environ['PROMETHEUS_MULTIPROC_DIR'] == self.tempdir
            assert len(w) == 1
            assert issubclass(w[-1].category, DeprecationWarning)
            assert "PROMETHEUS_MULTIPROC_DIR" in str(w[-1].message)

    def test_mark_process_dead_respects_lowercase(self):
        os.environ['prometheus_multiproc_dir'] = self.tempdir
        # Just test that this does not raise with a lowercase env var. The
        # logic is tested elsewhere.
        mark_process_dead(123)


class TestMultiProcess(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = self.tempdir
        values.ValueClass = MultiProcessValue(lambda: 123)
        self.registry = CollectorRegistry()
        self.collector = MultiProcessCollector(self.registry)

    @property
    def _value_class(self):
        return

    def tearDown(self):
        del os.environ['PROMETHEUS_MULTIPROC_DIR']
        shutil.rmtree(self.tempdir)
        values.ValueClass = MutexValue

    def test_counter_adds(self):
        c1 = Counter('c', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        c2 = Counter('c', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('c_total'))
        c1.inc(1)
        c2.inc(2)
        self.assertEqual(3, self.registry.get_sample_value('c_total'))

    def test_summary_adds(self):
        s1 = Summary('s', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        s2 = Summary('s', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('s_count'))
        self.assertEqual(0, self.registry.get_sample_value('s_sum'))
        s1.observe(1)
        s2.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('s_count'))
        self.assertEqual(3, self.registry.get_sample_value('s_sum'))

    def test_histogram_adds(self):
        h1 = Histogram('h', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        h2 = Histogram('h', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('h_count'))
        self.assertEqual(0, self.registry.get_sample_value('h_sum'))
        self.assertEqual(0, self.registry.get_sample_value('h_bucket', {'le': '5.0'}))
        h1.observe(1)
        h2.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('h_count'))
        self.assertEqual(3, self.registry.get_sample_value('h_sum'))
        self.assertEqual(2, self.registry.get_sample_value('h_bucket', {'le': '5.0'}))

    def test_gauge_all(self):
        g1 = Gauge('g', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '456'}))
        g1.set(1)
        g2.set(2)
        mark_process_dead(123)
        self.assertEqual(1, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g', {'pid': '456'}))

    def test_gauge_liveall(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='liveall')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='liveall')
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '456'}))
        g1.set(1)
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g', {'pid': '456'}))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(None, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g', {'pid': '456'}))

    def test_gauge_min(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='min')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='min')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g'))

    def test_gauge_livemin(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livemin')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livemin')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g'))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_gauge_max(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='max')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='max')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_gauge_livemax(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livemax')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livemax')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(2)
        g2.set(1)
        self.assertEqual(2, self.registry.get_sample_value('g'))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(1, self.registry.get_sample_value('g'))

    def test_gauge_sum(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='sum')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='sum')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(3, self.registry.get_sample_value('g'))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(3, self.registry.get_sample_value('g'))

    def test_gauge_livesum(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livesum')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livesum')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(3, self.registry.get_sample_value('g'))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_gauge_mostrecent(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='mostrecent')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='mostrecent')
        g2.set(2)
        g1.set(1)
        self.assertEqual(1, self.registry.get_sample_value('g'))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(1, self.registry.get_sample_value('g'))

    def test_gauge_livemostrecent(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livemostrecent')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livemostrecent')
        g2.set(2)
        g1.set(1)
        self.assertEqual(1, self.registry.get_sample_value('g'))
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_namespace_subsystem(self):
        c1 = Counter('c', 'help', registry=None, namespace='ns', subsystem='ss')
        c1.inc(1)
        self.assertEqual(1, self.registry.get_sample_value('ns_ss_c_total'))

    def test_counter_across_forks(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        c1 = Counter('c', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('c_total'))
        c1.inc(1)
        c1.inc(1)
        pid = 1
        c1.inc(1)
        self.assertEqual(3, self.registry.get_sample_value('c_total'))
        self.assertEqual(1, c1._value.get())

    def test_initialization_detects_pid_change(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)

        # can not inspect the files cache directly, as it's a closure, so we
        # check for the actual files themselves
        def files():
            fs = os.listdir(os.environ['PROMETHEUS_MULTIPROC_DIR'])
            fs.sort()
            return fs

        c1 = Counter('c1', 'c1', registry=None)
        self.assertEqual(files(), ['counter_0.db'])
        c2 = Counter('c2', 'c2', registry=None)
        self.assertEqual(files(), ['counter_0.db'])
        pid = 1
        c3 = Counter('c3', 'c3', registry=None)
        self.assertEqual(files(), ['counter_0.db', 'counter_1.db'])

    def test_collect(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        c = Counter('c', 'help', labelnames=labels.keys(), registry=None)
        g = Gauge('g', 'help', labelnames=labels.keys(), registry=None)
        h = Histogram('h', 'help', labelnames=labels.keys(), registry=None)

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(1)

        pid = 1

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(5)

        metrics = {m.name: m for m in self.collector.collect()}

        self.assertEqual(
            metrics['c'].samples, [Sample('c_total', labels, 2.0)]
        )
        metrics['g'].samples.sort(key=lambda x: x[1]['pid'])
        self.assertEqual(metrics['g'].samples, [
            Sample('g', add_label('pid', '0'), 1.0),
            Sample('g', add_label('pid', '1'), 1.0),
        ])

        expected_histogram = [
            Sample('h_sum', labels, 6.0),
            Sample('h_bucket', add_label('le', '0.005'), 0.0),
            Sample('h_bucket', add_label('le', '0.01'), 0.0),
            Sample('h_bucket', add_label('le', '0.025'), 0.0),
            Sample('h_bucket', add_label('le', '0.05'), 0.0),
            Sample('h_bucket', add_label('le', '0.075'), 0.0),
            Sample('h_bucket', add_label('le', '0.1'), 0.0),
            Sample('h_bucket', add_label('le', '0.25'), 0.0),
            Sample('h_bucket', add_label('le', '0.5'), 0.0),
            Sample('h_bucket', add_label('le', '0.75'), 0.0),
            Sample('h_bucket', add_label('le', '1.0'), 1.0),
            Sample('h_bucket', add_label('le', '2.5'), 1.0),
            Sample('h_bucket', add_label('le', '5.0'), 2.0),
            Sample('h_bucket', add_label('le', '7.5'), 2.0),
            Sample('h_bucket', add_label('le', '10.0'), 2.0),
            Sample('h_bucket', add_label('le', '+Inf'), 2.0),
            Sample('h_count', labels, 2.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    def test_collect_histogram_ordering(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        h = Histogram('h', 'help', labelnames=['view'], registry=None)

        h.labels(view='view1').observe(1)

        pid = 1

        h.labels(view='view1').observe(5)
        h.labels(view='view2').observe(1)

        metrics = {m.name: m for m in self.collector.collect()}

        expected_histogram = [
            Sample('h_sum', {'view': 'view1'}, 6.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.005'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.01'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.025'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.05'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.075'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.1'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.25'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.5'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.75'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '1.0'}, 1.0),
            Sample('h_bucket', {'view': 'view1', 'le': '2.5'}, 1.0),
            Sample('h_bucket', {'view': 'view1', 'le': '5.0'}, 2.0),
            Sample('h_bucket', {'view': 'view1', 'le': '7.5'}, 2.0),
            Sample('h_bucket', {'view': 'view1', 'le': '10.0'}, 2.0),
            Sample('h_bucket', {'view': 'view1', 'le': '+Inf'}, 2.0),
            Sample('h_count', {'view': 'view1'}, 2.0),
            Sample('h_sum', {'view': 'view2'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.005'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.01'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.025'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.05'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.075'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.1'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.25'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.5'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.75'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '1.0'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '2.5'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '5.0'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '7.5'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '10.0'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '+Inf'}, 1.0),
            Sample('h_count', {'view': 'view2'}, 1.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    def test_collect_preserves_help(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        c = Counter('c', 'c help', labelnames=labels.keys(), registry=None)
        g = Gauge('g', 'g help', labelnames=labels.keys(), registry=None)
        h = Histogram('h', 'h help', labelnames=labels.keys(), registry=None)

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(1)

        pid = 1

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(5)

        metrics = {m.name: m for m in self.collector.collect()}

        self.assertEqual(metrics['c'].documentation, 'c help')
        self.assertEqual(metrics['g'].documentation, 'g help')
        self.assertEqual(metrics['h'].documentation, 'h help')

    def test_merge_no_accumulate(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        h = Histogram('h', 'help', labelnames=labels.keys(), registry=None)
        h.labels(**labels).observe(1)
        pid = 1
        h.labels(**labels).observe(5)

        path = os.path.join(os.environ['PROMETHEUS_MULTIPROC_DIR'], '*.db')
        files = glob.glob(path)
        metrics = {
            m.name: m for m in self.collector.merge(files, accumulate=False)
        }

        expected_histogram = [
            Sample('h_sum', labels, 6.0),
            Sample('h_bucket', add_label('le', '0.005'), 0.0),
            Sample('h_bucket', add_label('le', '0.01'), 0.0),
            Sample('h_bucket', add_label('le', '0.025'), 0.0),
            Sample('h_bucket', add_label('le', '0.05'), 0.0),
            Sample('h_bucket', add_label('le', '0.075'), 0.0),
            Sample('h_bucket', add_label('le', '0.1'), 0.0),
            Sample('h_bucket', add_label('le', '0.25'), 0.0),
            Sample('h_bucket', add_label('le', '0.5'), 0.0),
            Sample('h_bucket', add_label('le', '0.75'), 0.0),
            Sample('h_bucket', add_label('le', '1.0'), 1.0),
            Sample('h_bucket', add_label('le', '2.5'), 0.0),
            Sample('h_bucket', add_label('le', '5.0'), 1.0),
            Sample('h_bucket', add_label('le', '7.5'), 0.0),
            Sample('h_bucket', add_label('le', '10.0'), 0.0),
            Sample('h_bucket', add_label('le', '+Inf'), 0.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    def test_missing_gauge_file_during_merge(self):
        # These files don't exist, just like if mark_process_dead(9999999) had been
        # called during self.collector.collect(), after the glob found it
        # but before the merge actually happened.
        # This should not raise and return no metrics
        self.assertFalse(self.collector.merge([
            os.path.join(self.tempdir, 'gauge_liveall_9999999.db'),
            os.path.join(self.tempdir, 'gauge_livesum_9999999.db'),
        ]))

    def test_remove_clear_warning(self):
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = self.tempdir
        with warnings.catch_warnings(record=True) as w:
            values.ValueClass = get_value_class()
            registry = CollectorRegistry()
            collector = MultiProcessCollector(registry)
            counter = Counter('c', 'help', labelnames=['label'], registry=None)
            counter.labels('label').inc()
            counter.remove('label')
            counter.clear()
            assert os.environ['PROMETHEUS_MULTIPROC_DIR'] == self.tempdir
            assert issubclass(w[0].category, UserWarning)
            assert "Removal of labels has not been implemented" in str(w[0].message)
            assert issubclass(w[-1].category, UserWarning)
            assert "Clearing labels has not been implemented" in str(w[-1].message)
    
    def test_child_name_is_built_once_with_namespace_subsystem_unit(self):
        """
        Repro for #1035:
        In multiprocess mode, child metrics must NOT rebuild the full name
        (namespace/subsystem/unit) a second time. The exported family name should
        be built once, and Counter samples should use "<family>_total".
        """
        from prometheus_client import Counter

        class CustomCounter(Counter):
            def __init__(
                self, 
                name, 
                documentation, 
                labelnames=(),
                namespace="mydefaultnamespace", 
                subsystem="mydefaultsubsystem",
                unit="", 
                registry=None, 
                _labelvalues=None
            ):
                # Intentionally provide non-empty defaults to trigger the bug path.
                super().__init__(
                    name=name, 
                    documentation=documentation,
                    labelnames=labelnames, 
                    namespace=namespace,
                    subsystem=subsystem, 
                    unit=unit, 
                    registry=registry,
                    _labelvalues=_labelvalues)

        # Create a Counter with explicit namespace/subsystem/unit
        c = CustomCounter(
            name='m',
            documentation='help',
            labelnames=('status', 'method'),
            namespace='ns',
            subsystem='ss',
            unit='seconds',   # avoid '_total_total' confusion
            registry=None,    # not registered in local registry in multiprocess mode
        )

        # Create two labeled children
        c.labels(status='200', method='GET').inc()
        c.labels(status='404', method='POST').inc()

        # Collect from the multiprocess collector initialized in setUp()
        metrics = {m.name: m for m in self.collector.collect()}

        # Family name should be built once (no '_total' in family name)
        expected_family = 'ns_ss_m_seconds'
        self.assertIn(expected_family, metrics, f"missing family {expected_family}")

        # Counter samples must use '<family>_total'
        mf = metrics[expected_family]
        sample_names = {s.name for s in mf.samples}
        self.assertTrue(
            all(name == expected_family + '_total' for name in sample_names),
            f"unexpected sample names: {sample_names}"
        )

        # Ensure no double-built prefix sneaks in (the original bug)
        bad_prefix = 'mydefaultnamespace_mydefaultsubsystem_'
        all_names = {mf.name, *sample_names}
        self.assertTrue(
            all(not n.startswith(bad_prefix) for n in all_names),
            f"found double-built name(s): {[n for n in all_names if n.startswith(bad_prefix)]}"
        )

    def test_child_preserves_parent_context_for_subclasses(self):
        """
        Ensure child metrics preserve parent's namespace/subsystem/unit information
        so that subclasses can correctly use these parameters in their logic.
        """
        class ContextAwareCounter(Counter):
            def __init__(self,
                         name,
                         documentation,
                         labelnames=(),
                         namespace="",
                         subsystem="",
                         unit="",
                         **kwargs):
                self.context = {
                    'namespace': namespace,
                    'subsystem': subsystem,
                    'unit': unit
                }
                super().__init__(name, documentation,
                                 labelnames=labelnames,
                                 namespace=namespace,
                                 subsystem=subsystem,
                                 unit=unit,
                                 **kwargs)

        parent = ContextAwareCounter('m', 'help',
                                     labelnames=['status'],
                                     namespace='prod',
                                     subsystem='api',
                                     unit='seconds',
                                     registry=None)

        child = parent.labels(status='200')

        # Verify that child retains parent's context
        self.assertEqual(child.context['namespace'], 'prod')
        self.assertEqual(child.context['subsystem'], 'api')
        self.assertEqual(child.context['unit'], 'seconds')


class TestMmapedDict(unittest.TestCase):
    def setUp(self):
        fd, self.tempfile = tempfile.mkstemp()
        os.close(fd)
        self.d = mmap_dict.MmapedDict(self.tempfile)

    def test_process_restart(self):
        self.d.write_value('abc', 123.0, 987.0)
        self.d.close()
        self.d = mmap_dict.MmapedDict(self.tempfile)
        self.assertEqual((123, 987.0), self.d.read_value('abc'))
        self.assertEqual([('abc', 123.0, 987.0)], list(self.d.read_all_values()))

    def test_expansion(self):
        key = 'a' * mmap_dict._INITIAL_MMAP_SIZE
        self.d.write_value(key, 123.0, 987.0)
        self.assertEqual([(key, 123.0, 987.0)], list(self.d.read_all_values()))

    def test_multi_expansion(self):
        key = 'a' * mmap_dict._INITIAL_MMAP_SIZE * 4
        self.d.write_value('abc', 42.0, 987.0)
        self.d.write_value(key, 123.0, 876.0)
        self.d.write_value('def', 17.0, 765.0)
        self.assertEqual(
            [('abc', 42.0, 987.0), (key, 123.0, 876.0), ('def', 17.0, 765.0)],
            list(self.d.read_all_values()))

    def test_corruption_detected(self):
        self.d.write_value('abc', 42.0, 987.0)
        # corrupt the written data
        self.d._m[8:16] = b'somejunk'
        with self.assertRaises(RuntimeError):
            list(self.d.read_all_values())

    def tearDown(self):
        os.unlink(self.tempfile)


class TestUnsetEnv(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        fp, self.tmpfl = tempfile.mkstemp()
        os.close(fp)

    def test_unset_syncdir_env(self):
        self.assertRaises(
            ValueError, MultiProcessCollector, self.registry)

    def test_file_syncpath(self):
        registry = CollectorRegistry()
        self.assertRaises(
            ValueError, MultiProcessCollector, registry, self.tmpfl)

    def tearDown(self):
        os.remove(self.tmpfl)
