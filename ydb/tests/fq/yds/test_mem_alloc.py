#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.library.common.wait_for import wait_for


class TestMemAlloc(StreamingTestBase):
    def test_join_alloc(self, ydb_topics):
        first = ydb_topics('select_join_alloc')
        second = ydb_topics('select_join_alloc_right')
        client = first.kikimr.ydb_client
        query = first.kikimr.get_database_name() + '/' + first.name + '_query'
        first.queries.append(query)
        messages = ['A', 'B', 'C']
        first.write(messages)
        second.write(messages)

        try:
            client.query(f"""
                CREATE STREAMING QUERY `{query}` WITH (
                    STREAMING_DISPOSITION = OLDEST, CHECKPOINT_INTERVAL = 'PT1S'
                ) AS DO BEGIN
                    PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
                    $joined = (
                        SELECT S1.Data AS Data1, S2.Data AS Data2,
                            ListReplicate(S2.Data, 1000000) AS Data20
                        FROM {first.input} AS S1
                        INNER JOIN (SELECT * FROM {second.input} LIMIT 2) AS S2
                        ON S1.Data = S2.Data
                        LIMIT 2
                    );
                    -- Serialize the full list to exercise allocations, then send
                    -- a compact summary that fits in a topic message.
                    $summary = (
                        SELECT Data1, Data2,
                            Length(Unwrap(Yson::SerializeJson(Yson::From(Data20)))) AS ReplicatedBytes
                        FROM $joined
                    );
                    INSERT INTO {first.output}
                    SELECT Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) FROM $summary;
                END DO;
            """)
            assert wait_for(
                lambda: first.state(query)['Status'] == 'COMPLETED',
                timeout_seconds=120,
                step_seconds=0.5,
            ), first.state(query)

            rows = sorted((json.loads(message) for message in first.read(2)), key=lambda row: row['Data1'])
            assert rows == [
                {'Data1': 'A', 'Data2': 'A', 'ReplicatedBytes': 4000001},
                {'Data1': 'B', 'Data2': 'B', 'ReplicatedBytes': 4000001},
            ], rows

            result = client.query(f"""
                SELECT Plan FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows
            plan = json.loads(result[0]['Plan'])

            def stage_memory(node):
                memory = node.get('Stats', {}).get('MaxMemoryUsage', {}).get('Max', 0)
                if memory:
                    yield memory
                for child in node.get('Plans', []):
                    yield from stage_memory(child)

            memory = list(stage_memory(plan['Plan']))
            assert memory, plan
            logging.debug('Join peak stage memory usage: %s bytes', max(memory))
        finally:
            client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')
