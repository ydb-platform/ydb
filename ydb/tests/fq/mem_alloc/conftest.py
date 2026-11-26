from types import SimpleNamespace

import pytest

from ydb.tests.fq.mem_alloc.test_base import (
    DEFAULT_DELTA,
    DEFAULT_LIMIT,
    MEMORY_POOL,
    NODE_MEMORY_LIMIT,
    MemoryQuery,
    wait_memory_released,
)
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env


@pytest.fixture
def kikimr(request):
    param = getattr(request, 'param', (None, None, None, None))
    multi_dc = len(param) == 3
    initial, total, step = param[:3]
    assert multi_dc or param[3] is None, 'No test sets a separate per-task hard limit'
    initial = DEFAULT_LIMIT if initial is None else initial
    total = NODE_MEMORY_LIMIT if total is None else total
    step = DEFAULT_DELTA if step is None else step
    config_request = SimpleNamespace(param={'checkpointing_period_ms': '3600000'})
    set_test_env(config_request)
    config = get_ydb_config(config_request)
    flags = config.yaml_config['feature_flags']
    flags['enable_secure_script_executions'] = False
    flags['enable_resource_pools'] = True
    flags['enable_resource_pools_scheduler'] = True
    flags['enable_compute_scheduler_memory'] = True
    config.yaml_config.setdefault('memory_controller_config', {})['query_execution_limit_bytes'] = NODE_MEMORY_LIMIT
    table = config.yaml_config['table_service_config']
    resources = table.setdefault('resource_manager', {})
    resources.update(
        # Isolate MKQL reservations from the fixed compute-actor footprint.
        execution_unit_memory=0,
        prefer_local_datacenter_execution=True,
    )
    if param[0] is not None:
        resources['mkql_light_program_memory_limit'] = initial
    if param[2] is not None:
        resources['min_mem_alloc_size'] = step
    table.setdefault('aggregation_config', {})['aggregation_compute_threads'] = 1
    if multi_dc:
        resources.update(
            max_non_parallel_tasks_execution_limit=0,
            max_non_parallel_top_stage_execution_limit=0,
        )
    cluster = Kikimr(config, enable_discovery=False)
    try:
        if multi_dc:
            cluster.cluster.register_and_start_slots(database=cluster.get_database_name(), count=2)
            cluster.datacenters = {}
            for index, (node_id, node) in enumerate(cluster.cluster.slots.items()):
                node.stop()
                node.data_center = f'DC{index % 2 + 1}'
                cluster.datacenters[node_id] = node.data_center
                node.start()
            cluster.recreate_driver(cluster.first_node.node_id)
        else:
            # Keep allocation pressure on one compute node, as in the original
            # tests; the storage node and the external topic cluster stay separate.
            spare = [node for node in cluster.cluster.slots.values() if node != cluster.first_node]
            cluster.cluster.unregister_and_stop_slots(spare)
        cluster.wait_kqp_node_count(4 if multi_dc else 1, timeout_seconds=120)
        cluster.ydb_client.retry_settings.retry_internal_error = False
        cluster.memory_settings = {'initial': initial, 'total': total, 'step': step}
        # A separate pool leaves resources for CREATE/ALTER and sys-view reads
        # when the test exhausts its own memory budget.
        percent = total * 100 / NODE_MEMORY_LIMIT
        cluster.ydb_client.query(f'''
            CREATE RESOURCE POOL `{MEMORY_POOL}` WITH (
                CONCURRENT_QUERY_LIMIT = '-1', TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = '{percent:.16g}'
            );
        ''')
        wait_memory_released(cluster)
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def memory_query(kikimr):
    queries = []

    def create(name, partitions=1):
        query = MemoryQuery(kikimr, name, partitions)
        queries.append(query)
        return query

    try:
        yield create
    finally:
        for query in reversed(queries):
            query.close()
