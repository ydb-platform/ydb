#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.fq.streaming_common.common import get_checkpoint_coordinator_metric

KeyLen = 900000  # State size ~ 12*KeyLen


class TestBigState(TestYdsBase):
    def test_gt_8mb(self, ydb_topics):
        io = ydb_topics('select_hop_8mb')
        query = io.start(f"""
            $raw_stream = SELECT Yson::Parse(Data) AS yson_data FROM {io.input}
                WITH (FORMAT=raw, SCHEMA=(Data String NOT NULL));
            $format_stream = SELECT time, ListConcat(ListReplicate(key, {KeyLen})) AS key FROM (
                SELECT Yson::LookupInt64(yson_data, 'time') AS time,
                       Yson::LookupString(yson_data, 'key') AS key FROM $raw_stream
            );
            INSERT INTO {io.output}
            SELECT Yson::SerializeText(Yson::From(TableRow())) FROM (
                SELECT key, COUNT(*) AS cnt FROM $format_stream
                GROUP BY HOP(CAST(time AS Timestamp), 'PT1S', 'PT1S', 'PT1S'), key
            );
        """)
        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
        io.write([f'{{"time" = 1; "key" = "{c}";}}' for c in symbols])
        io.checkpoint(query, count=3)
        io.restart_streaming_node(io.kikimr)
        io.checkpoint(query, count=3)
        io.write(['{"time" = 2000000; "key" = "M";}'])
        expected = sorted(f'{{"cnt" = 1u; "key" = "{c * KeyLen}"}}' for c in symbols)
        assert sorted(io.read(len(symbols))) == expected
        assert get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'AbortedCheckpoints') == 0
