import logging
import time

from ydb.tests.fq.streaming_common.common import StreamingTestBase

logger = logging.getLogger(__name__)


class TestStreamingRollingRestart(StreamingTestBase):

    def do_write_read(self, input, expected_output, endpoint):
        logger.debug("do_write_read")
        time.sleep(2)
        logger.debug("write data to stream")
        self.write_stream(input, endpoint=endpoint)
        logger.debug("read data from stream")
        read_data = self.read_stream(len(expected_output), topic_path=self.output_topic, endpoint=endpoint, timeout=60)
        if len(read_data) != len(expected_output):
            read_data = sorted(read_data)
            read_data = read_data[-len(expected_output) :]  # deduplication disabled
        assert sorted(read_data) == sorted(expected_output)

    # @pytest.mark.parametrize("local_topics", [True, False])
    def test_shared_reading(self, kikimr, entity_name):

        # local_topics over EDS
        inp, out, endpoint = self.get_io_names(
            kikimr,
            "test_shared_reading",
            local_topics=False,
            entity_name=entity_name,
            shared=True,
            partitions_count=1,
            endpoint=kikimr.endpoint,
        )

        query_count = 2
        for i in range(query_count):
            name = f"test_shared_reading_{i}"
            sql = f"""
                CREATE STREAMING QUERY `{name}` AS DO BEGIN
                $input = (
                    SELECT * FROM
                        {inp} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );
                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) FROM $input);
                INSERT INTO {out} SELECT * FROM $json;
                END DO;"""

            path = f"/Root/{name}"
            kikimr.ydb_client.query(sql.format(query_name=name, inp=inp, out=out))
            self.wait_completed_checkpoints(kikimr, path)

        for i, _ in enumerate(self.roll(kikimr)):
            logger.debug(f"RollingUpgrade {i}")
            input = [f'{{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-{i}"}}']
            expected_data = [
                f'{{"host":"host-{i}","level":"error","time":"2025-01-01T00:15:00.000000Z"}}'
            ] * query_count
            self.do_write_read(input, expected_data, endpoint)
            time.sleep(10)
