#!/usr/bin/env python
# -*- coding: utf-8 -*-


from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


import pytest
import ydb as ydb_sdk


class TestConsumer(TestYdsBase):
    def test_invalid(self, ydb_topics):
        io = ydb_topics('invalid_consumer')
        with pytest.raises(ydb_sdk.issues.GenericError) as error:
            io.kikimr.ydb_client.query(
                f"""
                PRAGMA pq.Consumer = 'InvalidConsumerName';
                SELECT * FROM {io.input} WITH (STREAMING = 'TRUE') LIMIT 1;
            """,
                timeout=60,
            )
        assert 'Consumer `InvalidConsumerName` does not exist in topic' in str(error.value)
