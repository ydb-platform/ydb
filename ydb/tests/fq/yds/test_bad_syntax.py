#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.library.common.wait_for import wait_for

import ydb as ydb_sdk


class TestBadSyntax(TestYdsBase):
    @pytest.mark.parametrize(
        ('after_modify', 'with_read_rules'),
        [(False, False), (True, False), (True, True)],
        ids=['create', 'modify', 'modify_with_created_read_rules'],
    )
    def test_bad_syntax(self, ydb_topics, after_modify, with_read_rules):
        io = ydb_topics('bad_syntax')
        client = io.kikimr.ydb_client
        query = io.kikimr.get_database_name() + '/' + io.name + '_query'
        io.queries.append(query)

        def describe():
            return client.query(f"""
                SELECT Text, Status FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows

        if after_modify:
            consumer_pragma = ''
            if with_read_rules:
                consumer = io.consumer_name + '_input'
                io.kikimr.external_ydb_client.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{consumer}`;')
                consumer_pragma = f'PRAGMA pq.Consumer = "{consumer}";'

            # Streaming queries need a topic source. RUN=FALSE covers ALTER
            # before a reader exists; LIMIT 1 covers ALTER after successful execution.
            client.query(f"""
                CREATE STREAMING QUERY `{query}` WITH (
                    RUN = {with_read_rules}, CHECKPOINT_INTERVAL = 'PT1S'
                ) AS DO BEGIN
                    PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
                    {consumer_pragma}
                    INSERT INTO {io.output} SELECT Data FROM {io.input} LIMIT 1;
                END DO;
            """)
            if with_read_rules:
                io.checkpoint(query)
                io.write(['A'])
                assert io.read(1) == ['A']
                assert wait_for(
                    lambda: io.state(query)['Status'] == 'COMPLETED',
                    timeout_seconds=120,
                    step_seconds=0.5,
                ), io.state(query)
            else:
                assert io.state(query)['Status'] == 'CREATED'
            before = describe()[0]
            operation = f'ALTER STREAMING QUERY `{query}` SET (FORCE = TRUE)'
        else:
            operation = f'CREATE STREAMING QUERY `{query}`'

        with pytest.raises(ydb_sdk.issues.GenericError) as error:
            client.query(f'{operation} AS DO BEGIN blablabla; END DO;')
        issues = str(error.value)
        assert 'blablabla' in issues, issues
        assert 'internal error' not in issues.lower(), issues
        assert any(
            message in issues.lower()
            for message in (
                'unexpected',
                'extraneous input',
                'mismatched input',
                'no viable alternative',
                'failed to parse',
            )
        ), issues

        # DDL syntax errors must neither create a query nor replace an existing definition.
        after = describe()
        if after_modify:
            assert len(after) == 1, after
            assert after[0]['Text'] == before['Text'], after
            assert after[0]['Status'] == before['Status'], after
        else:
            assert not after, after

    def test_require_as(self, ydb_cluster):
        with pytest.raises(ydb_sdk.issues.GenericError) as error:
            ydb_cluster.ydb_client.query('PRAGMA DisableAnsiOptionalAs; SELECT 42 a;')
        assert 'Expecting mandatory AS here' in str(error.value)

    def test_type_as_column(self, ydb_cluster):
        with pytest.raises(ydb_sdk.issues.GenericError) as error:
            ydb_cluster.ydb_client.query('select max(DateTime) from AS_TABLE([<|x:1|>]);')
        assert 'Member not found: DateTime' in str(error.value)
