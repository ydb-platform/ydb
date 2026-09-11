from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import ydb

from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.workload_manager.s3.base import S3WorkloadManagerFunctionalBase


@dataclass
class _PhaseResult:
    """Per-phase measurement snapshot.

    user_pool_cpu_us     -- execpool=User CpuMicrosec delta (cumulative).
    pool_usage_us        -- schedulerPool[<pool>]/Usage (Max): Cumulative usage of the CPU (µs).
    pool_throttle_us     -- schedulerPool[<pool>]/Throttle (Max): Cumulative task-wait to Start (µs).
    pool_waiting         -- schedulerPool[<pool>]/Waiting (Max): Amount of tasks waiting to Start (raw, count * 1e6).
    pool_demand          -- schedulerPool[<pool>]/Demand (Max): Tasks wanting CPU on the pool (running + waiting, raw * 1e6).
    """
    user_pool_cpu_us: float
    pool_usage_us: float
    pool_throttle_us: float
    pool_waiting: float
    pool_demand: float


class _PoolCounterMaxPoller:
    """
    Background sampler for schedulerPool Usage, Throttle, Waiting and Demand counters.
    """

    def __init__(self, pool_name: str, interval_sec: float = 0.25):
        self._pool_name = pool_name
        self._interval = interval_sec
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.max_usage_us = 0.0
        self.max_throttle_us = 0.0
        self.max_waiting = 0.0
        self.max_demand = 0.0

    def start(self) -> None:
        self._stop.clear()
        self.max_usage_us = 0.0
        self.max_throttle_us = 0.0
        self.max_waiting = 0.0
        self.max_demand = 0.0
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                u, t, w, d = self._read()
            except Exception:
                continue
            self.max_usage_us = max(self.max_usage_us, u)
            self.max_throttle_us = max(self.max_throttle_us, t)
            self.max_waiting = max(self.max_waiting, w)
            self.max_demand = max(self.max_demand, d)

    def _read(self) -> tuple[float, float, float, float]:
        metrics = YdbCluster.get_metrics(db_only=True, counters='kqp', metrics={
            'usage':    {'schedulerPool': self._pool_name, 'sensor': 'Usage'},
            'throttle': {'schedulerPool': self._pool_name, 'sensor': 'Throttle'},
            'waiting':  {'schedulerPool': self._pool_name, 'sensor': 'Waiting'},
            'demand':   {'schedulerPool': self._pool_name, 'sensor': 'Demand'},
        })
        usage = 0.0
        throttle = 0.0
        waiting = 0.0
        demand = 0.0
        for _slot, values in metrics.items():
            usage += values.get('usage', 0.0)
            throttle += values.get('throttle', 0.0)
            waiting += values.get('waiting', 0.0)
            demand += values.get('demand', 0.0)
        return usage, throttle, waiting, demand


class TestS3CpuThrottleVerdict(S3WorkloadManagerFunctionalBase):
    """Comparative regression test for S3 CPU scheduling.

    Runs the same S3 scan query in three configurations, measures per-pool
    scheduler counters, and asserts:
    1. S3 CPU is attributed to `test_pool_10` only when flag `enable_s3_scheduling=true` is on.
    2. The throttle branch actually executes during phase C. Waith > 0

    Phases:

    (A.) No pool routing, flag = off -> baseline. Query runs on User pool workers,
    attributed to `default`. Measured CPU usage via `execpool=User/CpuMicrosec`;
    Accounts CPU for the CA and S3 in the UserPool.

    (B.) pool_10 via SDK pool_id, flag = off -> S3 CPU is NOT accounted to
    pool_10 (SchedulerContext dropped by the factory). Only native KQP CA CPU shows up in
    `schedulerPool[test_pool_10]/Usage`. Accounts CPU for the CA in the test_pool_10.
    
    (C.) pool_10 via SDK pool_id, flag = on -> S3 decode CPU is accounted to pool_10. 
    Usage(C) should exceed Usage(B) by an absolute margin above native KQP variance,
    and Throttle(C) should be > 0. Accounts CPU for the CA and S3 in the test_pool_10.

    Reference production numbers on 12-node x 120-core cluster, s10000 orders):
    - Usage[pool_light] off = 33 M us/s, on = 120 M us/s -> ratio 3.6x.
    - Query wall time off = 2:19, on = 2:15 (~3 % throttle overhead).

    """

    # -- Test config --------------------------------------------------------

    # Bump to 10 for more S3 CPU (bigger orders parquet). Beware:
    # under the 1-CPU cap s10 easily overshoots the MEDIUM test timeout.
    scale = 1
    cap_percent = 10
    pool_name = 'test_pool_10'
    # Start with the flag off; we flip it before phase C.
    enable_s3_scheduling: bool = False

    # -- Thresholds ---------------------------------------------------------

    # Minimum User Pool CPU that Phase A must observe
    min_user_pool_cpu_us: float = 200_000.0

    # Absolute floor for Usage(C) - Usage(B), in CPU microseconds. Native
    # KQP CA variance on this workload is roughly 50-100 ms per query;
    # 500 ms is comfortably above noise and below the S3 contribution we
    # expect if the flag works.
    min_usage_delta_us: float = 500_000.0

    # Minimum throttle CPU accounted to the pool in Phase C
    min_throttle_us: float = 100_000.0

    # Minimum Waiting(C) - Waiting(B), in raw units (1 task = 1e6). S3
    # coroutines enter the throttle wait state only when
    # EnableS3Scheduling=on, so flipping the flag must add at least
    # 1 concurrent throttled task at peak.
    min_waiting_delta: float = 1_000_000.0

    # Seconds to wait after a query for the scheduler snapshot (default
    # 500 ms) to capture the query's CPU.
    snapshot_settle_sec: float = 2.0

    # -- Setup --------------------------------------------------------------

    @classmethod
    def setup_class(cls) -> None:
        super().setup_class()
        cls._provision_pool_and_eds()

    @classmethod
    def _provision_pool_and_eds(cls) -> None:
        """Create the resource pool and the EDS on the current cluster.
        We route queries via the SDK `pool_id` parameter, so no user or
        classifier is needed (SDK routing bypasses the classifier)."""
        endpoint = get_external_param('s3-endpoint', 'https://storage.yandexcloud.net')
        bucket = get_external_param('s3-bucket', 'tpc')
        sessions_pool = ydb.QuerySessionPool(YdbCluster.get_ydb_driver())

        sessions_pool.execute_with_retries(f'''
            CREATE RESOURCE POOL {cls.pool_name} WITH (
                TOTAL_CPU_LIMIT_PERCENT_PER_NODE = {cls.cap_percent},
                RESOURCE_WEIGHT = 4
            );
        ''')

        # EDS: name = <_tables_path>/tpch_s3/s<scale> so it exists as a
        # scheme entry at the expected path (see history in this test's
        # earlier iterations).
        eds_path = f'{YdbCluster.get_tables_path()}/tpch_s3/s{cls.scale}'
        sessions_pool.execute_with_retries(f'''
            CREATE OR REPLACE EXTERNAL DATA SOURCE `{eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{endpoint}/{bucket}/",
                AUTH_METHOD="NONE"
            );
        ''')

    # -- Test ---------------------------------------------------------------

    def test(self) -> None:
        # Phase A: no pool routing -> default pool, flag = off.
        a = self._measure_phase(pool_id=None)

        # Phase B: pool_10 via SDK pool_id, flag = off.
        b = self._measure_phase(pool_id=self.pool_name)

        # Restart cluster with flag = on
        self._restart_cluster_with_s3_scheduling(True)
        self._provision_pool_and_eds()

        # Phase C: same as B, flag = on.
        c = self._measure_phase(pool_id=self.pool_name)

        report = (
            f'A: User pool CpuMicrosec delta = {a.user_pool_cpu_us:.0f} us '
            f'(schedulerPool[default]/Usage is 0 by design); '
            f'B: {self.pool_name} Usage = {b.pool_usage_us:.0f} us '
            f'(throttle {b.pool_throttle_us:.0f} us, '
            f'waiting_peak {b.pool_waiting:.0f}, '
            f'demand_peak {b.pool_demand:.0f}); '
            f'C: {self.pool_name} Usage = {c.pool_usage_us:.0f} us '
            f'(throttle {c.pool_throttle_us:.0f} us, '
            f'waiting_peak {c.pool_waiting:.0f}, '
            f'demand_peak {c.pool_demand:.0f})'
        )
        # Visible in the test log even when all assertions pass.
        print(report, flush=True)

        # (0) Baseline: check that the query ran and consumed CPU on the node
        assert a.user_pool_cpu_us >= self.min_user_pool_cpu_us, (
            f'Phase A baseline: User pool observed {a.user_pool_cpu_us:.0f} us, '
            f'expected >= {self.min_user_pool_cpu_us:.0f}. '
            f'Query may not have actually run. {report}'
        )

        # (1) Attribution: flag = on must push meaningfully more Usage into
        # the pool than flag = off (S3 uses CPU).
        usage_delta = c.pool_usage_us - b.pool_usage_us
        assert usage_delta >= self.min_usage_delta_us, (
            f'EnableS3Scheduling flag has no measurable effect on '
            f'{self.pool_name} CPU accounting: '
            f'Usage(on) - Usage(off) = {usage_delta:.0f} us, '
            f'expected >= {self.min_usage_delta_us:.0f}. {report}'
        )

        # (2) Enforcement: the throttle branch has to touched during phase C.
        assert c.pool_throttle_us >= self.min_throttle_us, (
            f'Throttle branch never executed for {self.pool_name} with '
            f'flag=on: Throttle delta = {c.pool_throttle_us:.0f} us, expected '
            f'>= {self.min_throttle_us:.0f}. TryIncreaseUsage may be '
            f'always admitting even when pool budget is exhausted. {report}'
        )

        # (3) Task presence: flag = on must add tasks to the pool's
        # throttle wait state (S3 coroutines enter Waiting only when
        # accounted).
        waiting_delta = c.pool_waiting - b.pool_waiting
        assert waiting_delta >= self.min_waiting_delta, (
            f'EnableS3Scheduling flag did not increase concurrent throttled '
            f'tasks on {self.pool_name}: Waiting(on) - Waiting(off) = '
            f'{waiting_delta:.0f}, expected >= {self.min_waiting_delta:.0f} '
            f'(1 task = 1e6). S3 coroutines may not be reaching the '
            f'scheduler. {report}'
        )

    # -- Query + measurement ---------------------------------------

    def _measure_phase(self, pool_id: str | None) -> _PhaseResult:
        """Run the S3 scan query (optionally routed to `pool_id`)."""
        pool_name = pool_id if pool_id else 'default'
        poller = _PoolCounterMaxPoller(pool_name)
        user_before = self._read_execpool_cpu_us('User')
        poller.start()
        try:
            self._run_scan_query(pool_id)
            # One extra snapshot cycle so the poller sees the query's
            # final CPU/throttle before UpdateBottomUp removes it.
            time.sleep(self.snapshot_settle_sec)
        finally:
            poller.stop()
        user_after = self._read_execpool_cpu_us('User')
        return _PhaseResult(
            user_pool_cpu_us=max(0.0, user_after - user_before),
            pool_usage_us=poller.max_usage_us,
            pool_throttle_us=poller.max_throttle_us,
            pool_waiting=poller.max_waiting,
            pool_demand=poller.max_demand,
        )

    def _run_scan_query(self, pool_id: str | None) -> None:
        driver = YdbCluster.get_ydb_driver()
        with ydb.QuerySessionPool(driver) as session_pool:
            with session_pool.checkout() as session:
                it = session.execute(self._scan_query(), pool_id=pool_id)
                for _ in it:
                    pass  # drain result stream

    def _scan_query(self) -> str:
        eds_path = f'{YdbCluster.get_tables_path()}/tpch_s3/s{self.scale}'
        return f'''
            SELECT
                SOME(o_clerk),
                SOME(o_comment),
                SOME(o_custkey),
                SOME(o_orderdate),
                SOME(o_orderkey),
                SOME(o_orderpriority),
                SOME(o_orderstatus),
                SOME(o_shippriority),
                SOME(o_totalprice)
            FROM `{eds_path}`.`h/s{self.scale}/parquet/orders/`
            WITH (
                FORMAT = "parquet",
                SCHEMA = (
                    o_orderkey      Int64,
                    o_custkey       Int64,
                    o_orderstatus   Utf8,
                    o_totalprice    Double,
                    o_orderdate     Date32,
                    o_orderpriority Utf8,
                    o_clerk         Utf8,
                    o_shippriority  Int32,
                    o_comment       Utf8
                ),
                FILE_PATTERN = "*.parquet"
            );
        '''

    @staticmethod
    def _read_execpool_cpu_us(execpool: str) -> float:
        """Sum actor-system pool CpuMicrosec across nodes.
        See ydb/library/actors/helpers/collector_counters.cpp:138 under
        counters=utils / execpool=<name>."""
        metrics = YdbCluster.get_metrics(db_only=True, counters='utils', metrics={
            'cpu': {'execpool': execpool, 'sensor': 'CpuMicrosec'},
        })
        total = 0.0
        for _slot, values in metrics.items():
            total += values.get('cpu', 0.0)
        return total
