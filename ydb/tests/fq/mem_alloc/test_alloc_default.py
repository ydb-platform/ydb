#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import ydb

from ydb.tests.fq.mem_alloc.test_base import (
    DEFAULT_DELTA,
    DEFAULT_LIMIT,
    G,
    M,
    TIMEOUT,
    issue_messages,
    memory_metrics,
    total_metric,
    wait_memory_released,
    wait_pool_memory,
)
from ydb.tests.library.common.wait_for import wait_for

K = 1024


class TestAlloc:
    @pytest.mark.parametrize('kikimr', [(None, None, None, None)], indirect=['kikimr'])
    def test_default_limits(self, kikimr, memory_query):
        wait_memory_released(kikimr)
        io = memory_query('select_default_limits')
        query = io.create(f'INSERT INTO {io.output} SELECT Data FROM {io.input};')
        actors = total_metric(kikimr, 'RM/ComputeActors')
        assert actors > 0
        assert total_metric(kikimr, 'RM/ExternalMemory') == actors * DEFAULT_LIMIT, memory_metrics(kikimr)
        io.stop(query)
        wait_memory_released(kikimr)

    @pytest.mark.parametrize('kikimr', [(1 * M, 8 * G, None, None)], indirect=['kikimr'])
    def test_default_delta(self, kikimr, memory_query):
        io = memory_query('select_default_delta')
        query = io.create(io.aggregation_sql(count=True))
        actors = total_metric(kikimr, 'RM/ComputeActors')
        assert actors > 0
        assert total_metric(kikimr, 'RM/ExternalMemory') == actors * M, memory_metrics(kikimr)
        before = next(iter(memory_metrics(kikimr).values()))
        initial_memory = before['RM/Memory']
        initial_allocations = before['RM/ExtraMemAllocs']
        io.write_until(lambda: total_metric(kikimr, 'RM/Memory') > initial_memory, query)
        after = next(iter(memory_metrics(kikimr).values()))
        extra_memory = after['RM/Memory'] - initial_memory
        allocations = after['RM/ExtraMemAllocs'] - initial_allocations
        assert allocations > 0
        # MinMemAllocSize is a minimum, not a fixed step: a larger request is
        # valid, and the allocator rounds it to whole MiB.
        assert extra_memory >= allocations * DEFAULT_DELTA, (extra_memory, allocations, memory_metrics(kikimr))
        assert extra_memory % DEFAULT_DELTA == 0, extra_memory
        io.stop(query)
        wait_memory_released(kikimr)

    @pytest.mark.parametrize('kikimr', [(1 * M, 24 * M, 1 * M, None)], indirect=['kikimr'])
    def test_node_limit(self, kikimr, memory_query):
        io = memory_query('select_node_limit')
        running = []
        memory_per_query = None
        allocated = 0
        blocked = None
        creation_error = None
        # The pool is limited to 24 MiB on the single compute node. Bound the
        # number of attempts so a missing limit cannot create unlimited queries.
        for _ in range(32):
            try:
                query = io.create(io.aggregation_sql(count=True), wait=False)
            except ydb.Overloaded as error:
                blocked = io.queries[-1]
                creation_error = str(error)
                break
            if not io.wait_started_or_memory_error(query):
                blocked = query
                break
            io.wait_pool_limit()
            if memory_per_query is None:
                assert wait_for(
                    lambda: total_metric(kikimr, 'MemoryUsage') > 0,
                    timeout_seconds=TIMEOUT,
                    step_seconds=0.2,
                ), memory_metrics(kikimr)
                memory_per_query = total_metric(kikimr, 'MemoryUsage')
            running.append(query)
            allocated = len(running) * memory_per_query
            wait_pool_memory(kikimr, allocated)
            assert allocated <= kikimr.memory_settings['total'], memory_metrics(kikimr)
        assert blocked is not None, 'Queries did not reach the configured memory limit'
        assert running and memory_per_query > 0
        state = io.state(blocked)
        if creation_error is None:
            assert state['RetryCount'] > 0 or state['Status'] in ('SUSPENDED', 'FAILED'), state
        issues = creation_error or '\n'.join(issue_messages(state['Issues']))
        assert any(
            text in issues.lower() for text in ('not enough memory', 'memory limit exceeded', 'not enough resources')
        ), (creation_error, state)
        io.stop(blocked)
        wait_pool_memory(kikimr, allocated)
        io.stop(running[0])
        wait_pool_memory(kikimr, allocated - memory_per_query)
        replacement = io.create(io.aggregation_sql(count=True))
        wait_pool_memory(kikimr, allocated)
        for query in [*running[1:], replacement]:
            io.stop(query)
        wait_memory_released(kikimr)

    @pytest.mark.parametrize('kikimr', [(1 * G, 8 * G, None, None)], indirect=['kikimr'])
    def test_alloc_and_free(self, kikimr, memory_query):
        wait_memory_released(kikimr)
        io = memory_query('select_alloc_and_free')
        query = io.create(io.aggregation_sql(count=True))
        actors = total_metric(kikimr, 'RM/ComputeActors')
        assert actors > 0
        assert total_metric(kikimr, 'RM/ExternalMemory') == actors * G, memory_metrics(kikimr)
        io.stop(query)
        wait_memory_released(kikimr)

    @pytest.mark.parametrize('kikimr', [(1 * M, 1 * G, 16 * K, None)], indirect=['kikimr'])
    def test_up_down(self, kikimr, memory_query):
        io = memory_query('select_up_down')
        query = io.create(io.aggregation_sql())
        initially_allocated = total_metric(kikimr, 'RM/Memory')
        io.write_until(lambda: total_metric(kikimr, 'RM/Memory') > initially_allocated + M, query)
        io.stop(query)
        wait_memory_released(kikimr)

    @pytest.mark.parametrize('kikimr', [(1 * M, 10 * M, 1 * G, None)], indirect=['kikimr'])
    def test_mkql_not_increased(self, kikimr, memory_query):
        io = memory_query('test_mkql_not_increased')
        query = io.create(io.aggregation_sql())

        def memory_error():
            return any('Mkql memory limit exceeded' in message for message in issue_messages(io.state(query)['Issues']))

        io.write_until(memory_error, query)
        assert wait_for(
            lambda: io.state(query)['RetryCount'] > 0 or io.state(query)['Status'] == 'FAILED',
            timeout_seconds=TIMEOUT,
            step_seconds=0.2,
        ), io.state(query)
        # Check cleanup after the failed attempt while the query is suspended;
        # then stop its automatic retries and verify complete cleanup as well.
        assert wait_for(
            lambda: io.state(query)['Status'] in ('SUSPENDED', 'FAILED') and total_metric(kikimr, 'MemoryUsage') == 0,
            timeout_seconds=TIMEOUT,
            step_seconds=0.1,
            multiply=1,
        ), (io.state(query), memory_metrics(kikimr))
        io.stop(query)
        wait_memory_released(kikimr)
