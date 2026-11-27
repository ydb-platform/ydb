import collections
import json
import logging
import os
import signal
import threading
import time
import traceback
import unittest

import yatest.common
import ydb
from ydb.resolver import DiscoveryEndpointsResolver
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.daemon import Daemon
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


logger = logging.getLogger(__name__)

WORKERS = 4
COMPUTE_NODES = 8
ACQUIRE_TIMEOUT = 10
QUERY_TIMEOUT = 60
DRAIN_TIMEOUT = ACQUIRE_TIMEOUT + QUERY_TIMEOUT + 10
TABLE_ROWS = {
    'customer': 150000,
    'lineitem': 6001215,
    'nation': 25,
    'orders': 1500000,
    'part': 200000,
    'partsupp': 800000,
    'region': 5,
    'supplier': 10000,
}


def wait_until(predicate, timeout, description, check=lambda: None):
    deadline = time.monotonic() + timeout
    while True:
        check()
        if predicate():
            return
        if time.monotonic() >= deadline:
            raise AssertionError('Timed out waiting for ' + description)
        time.sleep(0.1)


def execute(pool, query, on_execute=lambda session: None):
    with pool.checkout(timeout=ACQUIRE_TIMEOUT) as session:
        on_execute(session)
        settings = ydb.BaseRequestSettings().with_timeout(QUERY_TIMEOUT)
        # Consume the entire stream: a successful first part does not imply success.
        with session.execute(query, settings=settings) as stream:
            return [row for part in stream for row in part.rows]


def assert_successful_window(records, minimum):
    failures = [record for record in records if record['status'] != 'SUCCESS']
    assert not failures, 'Query failures in measurement window: ' + json.dumps(failures[:10])
    assert len(records) >= minimum, f'Only {len(records)} attempts completed, expected {minimum}'
    assert {record['worker'] for record in records} == set(range(WORKERS)), 'A worker made no progress'


class QueryLoad:
    """Continuous single-attempt load, with drained boundaries between phases."""

    def __init__(self, pool, query, output_path):
        self.pool = pool
        self.query = query
        self.condition = threading.Condition()
        self.stopping = threading.Event()
        self.phase = None
        self.active = {}
        self.records = []
        self.errors = []
        self.next_id = 0
        self.phase_started = 0
        self.journal = open(output_path, 'w')
        self.threads = [threading.Thread(target=self._worker, args=(i,), daemon=True) for i in range(WORKERS)]
        for thread in self.threads:
            thread.start()

    def _worker(self, worker):
        try:
            while not self.stopping.is_set():
                with self.condition:
                    self.condition.wait_for(lambda: self.phase is not None or self.stopping.is_set())
                    if self.stopping.is_set():
                        return
                    record = dict(
                        attempt=self.next_id, worker=worker, phase=self.phase,
                        submitted=time.monotonic(), started=None, node_id=None,
                        status='SUCCESS', stage='acquire', issues='',
                    )
                    self.next_id += 1
                    self.active[worker] = record

                def on_execute(session):
                    with self.condition:
                        record.update(started=time.monotonic(), node_id=session.node_id, stage='execute')

                try:
                    rows = execute(self.pool, self.query, on_execute)
                    if not rows:
                        raise AssertionError('Q15 returned no rows')
                except ydb.Error as exc:
                    record.update(status=type(exc).__name__, issues=str(exc))
                except BaseException:
                    record.update(status='WORKER_ERROR', issues=traceback.format_exc())
                    raise
                finally:
                    with self.condition:
                        record['finished'] = time.monotonic()
                        self.records.append(record)
                        del self.active[worker]
                        self.journal.write(json.dumps(record) + '\n')
                        self.journal.flush()
                        self.condition.notify_all()
                # Avoid a hot loop on immediate errors while a node is down.
                if record['status'] != 'SUCCESS':
                    self.stopping.wait(0.1)
        except BaseException:
            with self.condition:
                self.errors.append(traceback.format_exc())
                self.stopping.set()
                self.condition.notify_all()

    def check(self):
        with self.condition:
            assert not self.errors, '\n'.join(self.errors)

    def start_phase(self, phase):
        with self.condition:
            assert self.phase is None and not self.active, 'Previous phase has not drained'
            self.phase = phase
            self.phase_started = time.monotonic()
            self.condition.notify_all()
        logger.info('Starting phase %s', phase)

    def phase_records(self, phase):
        with self.condition:
            return [record for record in self.records if record['phase'] == phase]

    def drain(self):
        with self.condition:
            phase = self.phase
            self.phase = None
        wait_until(lambda: not self.active, DRAIN_TIMEOUT, 'outstanding queries to drain', self.check)
        records = self.phase_records(phase)
        elapsed = time.monotonic() - self.phase_started
        logger.info('Phase %s: attempts=%d, statuses=%s, completed QPS=%.2f',
                    phase, len(records), dict(collections.Counter(r['status'] for r in records)),
                    len(records) / elapsed)
        return records

    def streaks_reached(self, phase, count):
        streaks = [0] * WORKERS
        for record in self.phase_records(phase):
            worker = record['worker']
            streaks[worker] = streaks[worker] + 1 if record['status'] == 'SUCCESS' else 0
        return min(streaks) >= count

    def has_inflight_query(self):
        with self.condition:
            return any(record['started'] is not None for record in self.active.values())

    def measure(self, phase, duration, minimum, timeout):
        self.start_phase(phase)

        def complete():
            records = self.phase_records(phase)
            failures = [record for record in records if record['status'] != 'SUCCESS']
            assert not failures, json.dumps(failures[:10])
            return (time.monotonic() - self.phase_started >= duration and len(records) >= minimum
                    and {record['worker'] for record in records} == set(range(WORKERS)))

        wait_until(complete, timeout, phase + ' measurement', self.check)
        assert_successful_window(self.drain(), minimum)

    def stop(self):
        self.stopping.set()
        with self.condition:
            self.condition.notify_all()
        deadline = time.monotonic() + DRAIN_TIMEOUT
        for thread in self.threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in self.threads), 'Load workers did not stop'
        self.journal.close()
        self.check()


class TestTpchResilience(unittest.TestCase):
    def setUp(self):
        self.output = yatest.common.test_output_path(self._testMethodName)
        os.makedirs(self.output, exist_ok=True)
        self.outages = []
        self.load = None
        self.addCleanup(self.save_diagnostics)
        config = KikimrConfigGenerator(
            nodes=8, erasure=Erasure.BLOCK_4_2, domain_name='local',
            use_in_memory_pdisks=True,
        )
        # Tenant pools default to erasure=none, independently of static storage.
        for domain in config.yaml_config['domains_config']['domain']:
            for pool in domain['storage_pool_types']:
                pool['pool_config']['erasure_species'] = 'block-4-2'
        self.cluster = KiKiMR(configurator=config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        self.database = '/local/tpch_resilience'
        self.cluster.create_database(self.database, storage_pool_units_count={'hdd': 1})
        self.slots = self.cluster.register_and_start_slots(self.database, count=COMPUTE_NODES)
        self.endpoint = self.cluster.nodes[1].endpoint
        config = ydb.DriverConfig(self.endpoint, self.database, discovery_request_timeout=5)
        self.resolver = DiscoveryEndpointsResolver(config)
        self.wait_slots(self.slots, 180)
        self.driver = ydb.Driver(config)
        self.addCleanup(self.driver.stop)
        self.driver.wait(timeout=30, fail_fast=True)
        self.pool = ydb.QuerySessionPool(self.driver, size=WORKERS)
        self.addCleanup(self.pool.stop)

        self.run_cli('init', ['init', '--store=column', '--float-mode=double'], 120)
        self.run_cli('import', ['import', 'generator', '--scale=1'], 600)
        for table, expected in TABLE_ROWS.items():
            rows = execute(self.pool, f'SELECT COUNT(*) AS count FROM `{self.database}/tpch/{table}`')
            self.assertEqual(rows[0].count, expected, table)

        with open(yatest.common.source_path('ydb/library/benchmarks/queries/tpch/yql/q15.sql')) as source:
            query = source.read()
        with open(yatest.common.source_path('ydb/library/benchmarks/gen_queries/consts.yql')) as source:
            query = query.replace("{% include 'header.sql.jinja' %}", source.read())
        for table in TABLE_ROWS:
            query = query.replace('{{' + table + '}}', f'`{self.database}/tpch/{table}`')
        self.assertNotIn('{{', query)
        self.assertNotIn('{%', query)
        self.load = QueryLoad(self.pool, query, os.path.join(self.output, 'attempts.jsonl'))
        self.addCleanup(self.load.stop)

    def run_cli(self, name, args, timeout):
        command = [yatest.common.binary_path('ydb/apps/ydb/ydb'), '-e', 'grpc://' + self.endpoint,
                   '-d', self.database, 'workload', 'tpch', '-p', 'tpch'] + args
        with open(os.path.join(self.output, name + '.stdout'), 'w') as stdout:
            with open(os.path.join(self.output, name + '.stderr'), 'w') as stderr:
                yatest.common.execute(command, timeout=timeout, stdout=stdout, stderr=stderr)

    def slots_ready(self, slots):
        if not all(slot.is_alive() for slot in slots):
            return False
        result = self.resolver.resolve()
        ports = {endpoint.port for endpoint in result.endpoints} if result else set()
        return {slot.grpc_port for slot in slots}.issubset(ports)

    def wait_slots(self, slots, timeout):
        wait_until(lambda: self.slots_ready(slots), timeout, 'compute endpoints to be published',
                   self.load.check if self.load else lambda: None)

    def restart_slot(self, slot, mode):
        wait_until(self.load.has_inflight_query, QUERY_TIMEOUT, 'an in-flight Q15', self.load.check)
        outage = dict(slot=slot.node_id, port=slot.grpc_port, mode=mode, requested=time.monotonic())
        self.outages.append(outage)
        logger.info('Stopping compute slot %s (%s)', slot.node_id, mode)
        try:
            if mode == 'kill':
                # KiKiMRNode.kill() automatically restarts. The base operation reaps
                # the process and closes its logs but leaves it down for observation.
                slot.server.reset_clients()
                Daemon.kill(slot)
            else:
                slot.stop()
                self.assertNotEqual(slot.daemon.exit_code, -signal.SIGKILL,
                                    'Controlled stop fell back to SIGKILL')
            self.assertFalse(slot.is_alive())
            outage['down'] = time.monotonic()
            while time.monotonic() - outage['down'] < 15:
                self.load.check()
                self.assertFalse(slot.is_alive(), 'Slot restarted during the downtime interval')
                time.sleep(0.1)
        finally:
            outage['restart'] = time.monotonic()
            slot.start()
        self.wait_slots([slot], 120)
        outage['ready'] = time.monotonic()

    def run_scenario(self, mode):
        self.load.start_phase('warmup')
        wait_until(lambda: self.load.streaks_reached('warmup', 1), 180, 'Q15 warm-up', self.load.check)
        self.load.drain()
        self.load.measure('healthy', duration=30, minimum=20, timeout=120)
        self.load.start_phase('faults')
        for index in (0, 3, 7):
            self.restart_slot(self.slots[index], mode)
        records = self.load.drain()
        successes_while_down = [
            record for record in records if record['status'] == 'SUCCESS' and any(
                outage['down'] <= record['started'] <= record['finished'] <= outage['restart']
                for outage in self.outages
            )
        ]
        self.assertTrue(successes_while_down, 'No Q15 attempt started and completed while a node was down')

        # Keep the same driver and pool: replacing them would hide client recovery bugs.
        self.load.start_phase('recovery')
        for slot in self.slots:
            slot.start()
        wait_until(lambda: self.slots_ready(self.slots) and self.load.streaks_reached('recovery', 3),
                   180, 'all endpoints and three consecutive successes per worker', self.load.check)
        self.load.drain()
        self.load.measure('recovered', duration=60, minimum=40, timeout=180)

    def save_diagnostics(self):
        summary = {'outages': self.outages, 'phases': {}}
        if self.load:
            for phase in ('warmup', 'healthy', 'faults', 'recovery', 'recovered'):
                records = self.load.phase_records(phase)
                elapsed = max((r['finished'] for r in records), default=0) - min(
                    (r['submitted'] for r in records), default=0)
                summary['phases'][phase] = dict(
                    attempts=len(records), statuses=dict(collections.Counter(r['status'] for r in records)),
                    completed_qps=len(records) / elapsed if elapsed else 0,
                )
            summary['worker_errors'] = self.load.errors
        with open(os.path.join(self.output, 'summary.json'), 'w') as output:
            json.dump(summary, output, indent=2)
        logger.info('Resilience summary: %s', json.dumps(summary))

    def test_controlled_stop(self):
        self.run_scenario('stop')

    def test_kill(self):
        self.run_scenario('kill')
