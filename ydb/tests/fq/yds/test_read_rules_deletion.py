#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest


class TestReadRulesDeletion:
    @pytest.mark.parametrize('with_recovery', [False, True], ids=['simple', 'with_recovery'])
    def test_delete_read_rules(self, ydb_topics, with_recovery):
        io = ydb_topics('delete_read_rules')
        external = io.kikimr.external_ydb_client
        consumer = io.name + '_user_consumer'
        external.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{consumer}`;')

        def consumers():
            return {item.name for item in external.driver.topic_client.describe_topic(io.input_topic).consumers}

        assert consumers() == {consumer}
        # Unlike YQ, KQP reads without a consumer when pq.Consumer is absent.
        # Neither starting, recovering, stopping nor dropping a query may leak
        # consumers or remove a consumer managed by the user.
        query = io.start(f'INSERT INTO {io.output} SELECT Data FROM {io.input};')
        io.write(['before_restart'])
        assert io.read(1) == ['before_restart']
        assert consumers() == {consumer}
        if with_recovery:
            io.checkpoint(query, count=3)
            io.restart_compute_node()
            io.checkpoint(query, count=3)
            io.write(['after_restart'])
            assert io.read(1) == ['after_restart']
            assert consumers() == {consumer}
        io.stop(query)
        io.wait_actor_count('DQ_MESSAGE_STREAM_READ_ACTOR', 0)
        assert consumers() == {consumer}
        io.kikimr.ydb_client.query(f'DROP STREAMING QUERY `{query}`;')
        assert consumers() == {consumer}
