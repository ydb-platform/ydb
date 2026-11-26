from types import SimpleNamespace

import pytest

from ydb.tests.fq.mem_alloc.test_base import (
    DEFAULT_DELTA,
    DEFAULT_LIMIT,
    MEMORY_POOL,
    NODE_MEMORY_LIMIT,
    MemoryQuery,
    memory_metrics,
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
    # Large per-task reservations also affect metadata and sys-view queries.
    # Leave them headroom outside the test pool; these are accounting limits.
    node_memory_limit = max(NODE_MEMORY_LIMIT, initial * 64)
    config.yaml_config.setdefault('memory_controller_config', {})['query_execution_limit_bytes'] = node_memory_limit
    table = config.yaml_config['table_service_config']
    resources = table.setdefault('resource_manager', {})
    resources.update(
        # Isolate MKQL reservations from the fixed compute-actor footprint.
        execution_unit_memory=0,
        prefer_local_datacenter_execution=True,
    )
    if param[0] is not None:
        resources['mkql_light_program_memory_limit'] = initial
    else:
        # The harness lowers this to 64 KiB; exercise the actual YDB default.
        resources.pop('mkql_light_program_memory_limit', None)
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
            # NodeBroker does not allow changing the location of a registered
            # node. Keep the original two slots in their default DC and start
            # two new slots in a second DC from their first registration.
            cluster.cluster.register_and_start_slots(database=cluster.get_database_name(), count=2, data_center='DC2')
            cluster.datacenters = {node_id: node.data_center for node_id, node in cluster.cluster.slots.items()}
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
        percent = total * 100 / node_memory_limit
        cluster.ydb_client.query(f'''
            CREATE RESOURCE POOL `{MEMORY_POOL}` WITH (
                CONCURRENT_QUERY_LIMIT = '-1', TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE = '{percent:.16g}'
            );
        ''')
        cluster.idle_memory = {node_id: metrics['RM/Memory'] for node_id, metrics in memory_metrics(cluster).items()}
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
