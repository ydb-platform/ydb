import os
import unittest

from prometheus_client import CollectorRegistry, ProcessCollector


class TestProcessCollector(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        import yatest.common as yc
        self.test_proc = os.path.join(os.path.dirname(os.path.realpath(yc.source_path(__file__))), 'proc')

    def test_working(self):
        collector = ProcessCollector(proc=self.test_proc, pid=lambda: 26231, registry=self.registry)
        collector._ticks = 100
        collector._pagesize = 4096

        self.assertEqual(17.21, self.registry.get_sample_value('process_cpu_seconds_total'))
        self.assertEqual(56274944.0, self.registry.get_sample_value('process_virtual_memory_bytes'))
        self.assertEqual(8114176, self.registry.get_sample_value('process_resident_memory_bytes'))
        self.assertEqual(1418184099.75, self.registry.get_sample_value('process_start_time_seconds'))
        self.assertEqual(2048.0, self.registry.get_sample_value('process_max_fds'))
        self.assertEqual(5.0, self.registry.get_sample_value('process_open_fds'))
        self.assertEqual(None, self.registry.get_sample_value('process_fake_namespace'))

    def test_namespace(self):
        collector = ProcessCollector(proc=self.test_proc, pid=lambda: 26231, registry=self.registry, namespace='n')
        collector._ticks = 100
        collector._pagesize = 4096

        self.assertEqual(17.21, self.registry.get_sample_value('n_process_cpu_seconds_total'))
        self.assertEqual(56274944.0, self.registry.get_sample_value('n_process_virtual_memory_bytes'))
        self.assertEqual(8114176, self.registry.get_sample_value('n_process_resident_memory_bytes'))
        self.assertEqual(1418184099.75, self.registry.get_sample_value('n_process_start_time_seconds'))
        self.assertEqual(2048.0, self.registry.get_sample_value('n_process_max_fds'))
        self.assertEqual(5.0, self.registry.get_sample_value('n_process_open_fds'))
        self.assertEqual(None, self.registry.get_sample_value('process_cpu_seconds_total'))

    def test_working_584(self):
        collector = ProcessCollector(proc=self.test_proc, pid=lambda: "584\n", registry=self.registry)
        collector._ticks = 100
        collector._pagesize = 4096

        self.assertEqual(0.0, self.registry.get_sample_value('process_cpu_seconds_total'))
        self.assertEqual(10395648.0, self.registry.get_sample_value('process_virtual_memory_bytes'))
        self.assertEqual(634880, self.registry.get_sample_value('process_resident_memory_bytes'))
        self.assertEqual(1418291667.75, self.registry.get_sample_value('process_start_time_seconds'))
        self.assertEqual(None, self.registry.get_sample_value('process_max_fds'))
        self.assertEqual(None, self.registry.get_sample_value('process_open_fds'))

    def test_working_fake_pid(self):
        collector = ProcessCollector(proc=self.test_proc, pid=lambda: 123, registry=self.registry)
        collector._ticks = 100
        collector._pagesize = 4096

        self.assertEqual(None, self.registry.get_sample_value('process_cpu_seconds_total'))
        self.assertEqual(None, self.registry.get_sample_value('process_virtual_memory_bytes'))
        self.assertEqual(None, self.registry.get_sample_value('process_resident_memory_bytes'))
        self.assertEqual(None, self.registry.get_sample_value('process_start_time_seconds'))
        self.assertEqual(None, self.registry.get_sample_value('process_max_fds'))
        self.assertEqual(None, self.registry.get_sample_value('process_open_fds'))
        self.assertEqual(None, self.registry.get_sample_value('process_fake_namespace'))


if __name__ == '__main__':
    unittest.main()
