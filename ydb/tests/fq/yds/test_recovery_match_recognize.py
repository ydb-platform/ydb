#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.fq.streaming_common.common import StreamingTestBase, get_checkpoint_coordinator_metric
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for


class TestRecoveryMatchRecognize(StreamingTestBase):
    def recovery_impl(
        self,
        ydb_topics,
        sql_template,
        test_name,
        messages_before_restart,
        messages_after_restart,
        expected,
    ):
        io = ydb_topics(test_name)
        query = io.start(sql_template.format(io.input, io.output))
        io.write(messages_before_restart)
        # Cross barriers after all the prefix has entered the stateful operators.
        io.checkpoint(query, count=3)

        # Restart all compute nodes so TimeOrderRecoverer and MATCH_RECOGNIZE
        # must reload their saved state regardless of the execution placement.
        nodes = list(io.kikimr.cluster.slots.values())
        try:
            for node in nodes:
                node.stop()
        finally:
            for node in nodes:
                node.set_log_file_prefix('logfile_restarted_')
                node.start()
        io.kikimr.wait_kqp_node_count(len(nodes))
        assert wait_for(
            lambda: get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'RestoredFromSavedCheckpoint') > 0,
            timeout_seconds=plain_or_under_sanitizer(120, 500),
            step_seconds=0.5,
        ), io.state(query)
        io.checkpoint(query)
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)

        io.write(messages_after_restart)
        assert io.read(len(expected)) == expected
        io.stop(query)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 0)

    def test_time_order_recoverer(self, ydb_topics, request):
        sql = R'''
            PRAGMA ydb.MaxTasksPerStage="2";

            pragma FeatureR010="prototype";
            pragma config.flags("TimeOrderRecoverDelay", "-1000000");
            pragma config.flags("TimeOrderRecoverAhead", "1000000");

            INSERT INTO {1}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow()))))
            FROM (SELECT * FROM {0}
                WITH (
                    format=json_each_row,
                    SCHEMA
                    (
                        dt UINT64
                    )))
            MATCH_RECOGNIZE(
                ORDER BY CAST(dt as Timestamp)
                MEASURES
                LAST(ALL_TRUE.dt) as dt
                ONE ROW PER MATCH
                PATTERN ( ALL_TRUE )
                DEFINE
                    ALL_TRUE as True)'''

        messages_before_restart = ['{"dt":1696849942400002}', '{"dt":1696849942000001}']
        messages_after_restart = [
            '{"dt":1696849942800000}',
            '{"dt":1696849943200003}',
            '{"dt":1696849943300003}',
            '{"dt":1696849943600003}',
            '{"dt":1696849943900003}',
        ]
        expected = ['{"dt":1696849942000001}', '{"dt":1696849942400002}', '{"dt":1696849942800000}']

        self.recovery_impl(
            ydb_topics,
            sql,
            request.node.name,
            messages_before_restart,
            messages_after_restart,
            expected,
        )

    def test_match_recognize(self, ydb_topics, request):
        sql = R'''
            PRAGMA ydb.MaxTasksPerStage="2";

            pragma FeatureR010="prototype";
            pragma config.flags("TimeOrderRecoverDelay", "-1000000");
            pragma config.flags("TimeOrderRecoverAhead", "1000000");
            pragma config.flags("MatchRecognizeStream", "auto");

            INSERT INTO {1}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow()))))
            FROM (SELECT * FROM {0}
                WITH (
                    format=json_each_row,
                    SCHEMA
                    (
                        dt UINT64,
                        str STRING
                    )))
            MATCH_RECOGNIZE(
                ORDER BY CAST(dt as Timestamp)
                MEASURES
                   LAST(A.dt) as dt_begin,
                   LAST(C.dt) as dt_end,
                   LAST(A.str) as a_str,
                   LAST(B.str) as b_str,
                   LAST(C.str) as c_str
                ONE ROW PER MATCH
                PATTERN ( A B C )
                DEFINE
                    A as A.str='A',
                    B as B.str='B',
                    C as C.str='C')'''

        messages_before_restart = [
            '{"dt": 1696849942000001, "str": "A" }',
            '{"dt": 1696849942500001, "str": "B" }',
            '{"dt": 1696849943000001, "str": "C" }',
            '{"dt": 1696849943600001, "str": "D" }',
        ]  # push A+B from TimeOrderRecoverer to MatchRecognize

        # Before restart:
        #    A + B : in MatchRecognize
        #    C + D : in TimeOrderRecoverer

        messages_after_restart = ['{"dt": 1696849944100001, "str": "E" }']
        expected = ['{"a_str":"A","b_str":"B","c_str":"C","dt_begin":1696849942000001,"dt_end":1696849943000001}']
        self.recovery_impl(
            ydb_topics,
            sql,
            request.node.name,
            messages_before_restart,
            messages_after_restart,
            expected,
        )
