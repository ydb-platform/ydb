#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.fq.mem_alloc.test_base import TIMEOUT, wait_memory_released
from ydb.tests.library.common.wait_for import wait_for


class TestAlloc:
    @pytest.mark.skip(reason='YDB topic source placement ignores PreferLocalDatacenterExecution')
    @pytest.mark.parametrize('kikimr', [(None, None, None)], indirect=['kikimr'])
    def test_dc_locality(self, kikimr, memory_query):
        io = memory_query('select_dc_locality', partitions=4)
        query = io.create(f'''
            PRAGMA ydb.MaxTasksPerStage = '4';
            INSERT INTO {io.output} SELECT Data FROM {io.input};
        ''')
        placement = {}

        def placed_in_one_dc():
            placement.clear()
            for node_id in kikimr.cluster.slots:
                placement[node_id] = io.get_actor_count(kikimr, node_id, 'KQP_COMPUTE_ACTOR')
            active = [node_id for node_id, count in placement.items() if count > 0]
            return len(active) == 2 and len({kikimr.datacenters[node_id] for node_id in active}) == 1

        assert wait_for(placed_in_one_dc, timeout_seconds=TIMEOUT, step_seconds=0.2, multiply=1), (
            placement,
            kikimr.datacenters,
        )
        messages = [f'message_{index}' for index in range(4)]
        io.write_stream(messages, endpoint=io.endpoint)
        assert sorted(io.read_stream(len(messages), endpoint=io.endpoint)) == sorted(messages)
        io.stop(query)
        wait_memory_released(kikimr)
