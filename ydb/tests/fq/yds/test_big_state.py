#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1

KeyLen = 900000  # State size ~ 12*KeyLen


class TestBigState(TestYdsBase):
    @yq_v1
    def test_gt_8mb(self, kikimr, client):
        self.init_topics("select_hop_8mb")
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        sql = Rf'''
            $raw_stream = SELECT Yson::Parse(Data) AS yson_data
                FROM myyds.`{self.input_topic}` WITH SCHEMA (Data String NOT NULL);

            $format_stream = SELECT time, ListConcat(ListReplicate(key, {KeyLen})) as key
                FROM (
                    SELECT
                        Yson::LookupInt64(yson_data, "time") AS time,
                        Yson::LookupString(yson_data, "key") AS key
                     FROM $raw_stream);

            INSERT INTO myyds.`{self.output_topic}`
            SELECT STREAM
                Yson::SerializeText(Yson::From(TableRow()))
            FROM (
                SELECT key, COUNT(*) as cnt from $format_stream
                GROUP BY HOP(CAST(time AS Timestamp), "PT1S", "PT1S", "PT1S"), key
                )
            '''
        print(f'sql {sql}')
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']
        input_messages = [Rf'''{{"time" = 1; "key" = "{c}";}}''' for c in symbols]
        logging.debug(f"input_messages {input_messages}")
        self.write_stream(input_messages)

        logging.debug("Wait checkpoint... ")
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )
        logging.debug("Checkpoint created")

        logging.debug("Restarting...")
        master_node_index = None
        for node_index in kikimr.control_plane.kikimr_cluster.nodes:
            if kikimr.control_plane.get_task_count(node_index, query_id) > 0:
                master_node_index = node_index
        assert master_node_index is not None, "Can't find master node"

        kikimr.control_plane.kikimr_cluster.nodes[master_node_index].stop()
        kikimr.control_plane.kikimr_cluster.nodes[master_node_index].start()
        kikimr.control_plane.wait_bootstrap(master_node_index)
        logging.debug("Restart finished")

        self.write_stream(['{"time" = 2000000; "key" = "M";}'])

        expected_messages = [Rf'''{{"cnt" = 1u; "key" = "{c*KeyLen}"}}''' for c in symbols]
        expected_messages.sort()

        actual_messages = self.read_stream(len(input_messages))
        actual_messages.sort()

        assert actual_messages == expected_messages

        aborted_checkpoints = kikimr.compute_plane.get_checkpoint_coordinator_metric(query_id, "AbortedCheckpoints")
        assert aborted_checkpoints == 0
