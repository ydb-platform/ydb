# -*- coding: utf-8 -*-
import json
import os
import sys
import time
import uuid

import yatest.common

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.msgbus_types import MessageBusStatus, SchemeStatus
from ydb.tests.library.common.protobuf_ss import CreateTopicRequest
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


DATABASE = "/Root"


def _test_param(name, default=None):
    value = yatest.common.get_param(name, None)
    if value is None:
        value = os.environ.get("YDB_" + name.upper(), default)
    return value


def _int_param(name, default):
    return int(_test_param(name, default))


def _bool_param(name, default):
    value = _test_param(name, "true" if default else "false")
    if isinstance(value, bool):
        return value

    value = str(value).lower()
    if value in ("1", "true", "yes", "y", "on"):
        return True
    if value in ("0", "false", "no", "n", "off"):
        return False

    raise ValueError("Expected boolean value for {}, got {!r}".format(name, value))


def _optional_int_param(name):
    value = _test_param(name)
    if value in (None, ""):
        return None
    return int(value)


def _read_proc_status_kb(pid):
    result = {}
    wanted = {"VmRSS", "VmHWM", "RssAnon", "RssFile", "RssShmem"}
    with open("/proc/{}/status".format(pid), "r") as status:
        for line in status:
            key, sep, value = line.partition(":")
            if sep and key in wanted:
                parts = value.split()
                if parts:
                    result[key + "_kb"] = int(parts[0])

    if "VmRSS_kb" not in result:
        raise RuntimeError("VmRSS is missing in /proc/{}/status".format(pid))
    return result


def _rss_sample(pid, label, settle_seconds):
    if settle_seconds:
        time.sleep(settle_seconds)

    sample = _read_proc_status_kb(pid)
    sample["pid"] = pid
    sample["label"] = label
    sample["timestamp"] = time.time()
    print("PQ_HEAD_RSS_SAMPLE {}".format(json.dumps(sample, sort_keys=True)), file=sys.stderr)
    return sample


class TestPersQueueHeadRss(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=1,
            use_in_memory_pdisks=False,
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def _endpoint(self):
        return "grpc://localhost:{}".format(self.cluster.nodes[1].grpc_port)

    def _make_driver(self):
        driver = ydb.Driver(ydb.DriverConfig(
            endpoint=self._endpoint(),
            database=DATABASE,
        ))
        driver.wait(timeout=60, fail_fast=True)
        return driver

    def _restart_node(self, driver):
        driver.stop()
        old_pid = self.cluster.nodes[1].pid
        self.cluster.nodes[1].stop()
        self.cluster.nodes[1].start()
        return old_pid, self.cluster.nodes[1].pid, self._make_driver()

    def _create_topic(self, topic_name, partitions, partition_per_tablet):
        options = (
            CreateTopicRequest.Options()
            .with_partitions_count(partitions)
            .with_partition_per_table(partition_per_tablet)
        )
        response = self.cluster.client.send_and_poll_request(
            CreateTopicRequest(os.path.join(DATABASE, topic_name), options=options).protobuf
        )
        assert response.Status == MessageBusStatus.MSTATUS_OK, response
        assert response.SchemeStatus == SchemeStatus.StatusSuccess, response

    def _topic_end_offset(self, driver, topic_name):
        description = driver.topic_client.describe_topic(topic_name, include_stats=True)
        return sum(partition.partition_stats.partition_end for partition in description.partitions)

    def _wait_topic_end_offset(self, driver, topic_name, expected, timeout_seconds):
        deadline = time.time() + timeout_seconds
        last_offset = None
        last_error = None

        while time.time() < deadline:
            try:
                last_offset = self._topic_end_offset(driver, topic_name)
                if last_offset >= expected:
                    return last_offset
            except Exception as error:
                last_error = error
            time.sleep(1)

        raise AssertionError(
            "Topic {} end offset did not reach {} in {}s, last offset {}, last error {}".format(
                topic_name,
                expected,
                timeout_seconds,
                last_offset,
                last_error,
            )
        )

    def _write_messages(self, driver, topic_name, messages_per_partition, partitions, payload_bytes, flush_every, progress):
        payload = "x" * payload_bytes
        total_messages = messages_per_partition * partitions
        total_sent = 0
        started_at = time.time()
        next_progress_percent = 1

        for partition_id in range(partitions):
            with driver.topic_client.writer(
                topic_name,
                partition_id=partition_id,
                producer_id="rss-producer-{}".format(partition_id),
            ) as writer:
                for index in range(messages_per_partition):
                    writer.write(ydb.TopicWriterMessage(payload))
                    if (index + 1) % flush_every == 0:
                        writer.flush()

                    total_sent += 1
                    if progress:
                        sent_percent = min(total_sent * 100 // total_messages, 100)
                        if sent_percent >= next_progress_percent or total_sent == total_messages:
                            elapsed = time.time() - started_at
                            print(
                                "PQ_HEAD_RSS_WRITE_PROGRESS sent_percent={} written={} total={} partition={} elapsed_sec={:.1f}".format(
                                    sent_percent,
                                    total_sent,
                                    total_messages,
                                    partition_id,
                                    elapsed,
                                ),
                                file=sys.stderr,
                            )
                            next_progress_percent = sent_percent + 1

                writer.flush()

    def test_pq_partition_head_reload_rss(self):
        messages_per_partition = _int_param("pq_head_rss_messages", 100000)
        payload_bytes = _int_param("pq_head_rss_payload_bytes", 16)
        flush_every = _int_param("pq_head_rss_flush_every", 1)
        partitions = _int_param("pq_head_rss_partitions", 1)
        partition_per_tablet = _int_param("pq_head_rss_partition_per_tablet", 1)
        settle_seconds = _int_param("pq_head_rss_settle_seconds", 3)
        wait_timeout_seconds = _int_param("pq_head_rss_wait_timeout_seconds", 300)
        progress = _bool_param("progress", True)
        max_delta_mb = _optional_int_param("pq_head_rss_max_delta_mb")
        total_messages = messages_per_partition * partitions

        assert messages_per_partition > 0
        assert partitions > 0
        assert partition_per_tablet > 0
        assert payload_bytes >= 0
        assert flush_every > 0

        topic_name = "pq_head_rss_{}".format(uuid.uuid4().hex)
        driver = self._make_driver()

        try:
            self._create_topic(topic_name, partitions, partition_per_tablet)
            self._wait_topic_end_offset(driver, topic_name, 0, wait_timeout_seconds)
            empty_live = _rss_sample(self.cluster.nodes[1].pid, "empty_live", settle_seconds)

            _, empty_restart_pid, driver = self._restart_node(driver)
            self._wait_topic_end_offset(driver, topic_name, 0, wait_timeout_seconds)
            empty_restart = _rss_sample(empty_restart_pid, "empty_restart", settle_seconds)

            self._write_messages(
                driver,
                topic_name,
                messages_per_partition,
                partitions,
                payload_bytes,
                flush_every,
                progress,
            )
            written = self._wait_topic_end_offset(driver, topic_name, total_messages, wait_timeout_seconds)
            after_write = _rss_sample(self.cluster.nodes[1].pid, "after_write", settle_seconds)

            _, head_restart_pid, driver = self._restart_node(driver)
            written_after_restart = self._wait_topic_end_offset(
                driver,
                topic_name,
                total_messages,
                wait_timeout_seconds,
            )
            head_restart = _rss_sample(head_restart_pid, "head_restart", settle_seconds)

            delta_kb = head_restart["VmRSS_kb"] - empty_restart["VmRSS_kb"]
            delta_per_message = delta_kb * 1024.0 / max(total_messages, 1)
            report = {
                "topic": topic_name,
                "database": DATABASE,
                "messages_per_partition": messages_per_partition,
                "partitions": partitions,
                "partition_per_tablet": partition_per_tablet,
                "total_messages": total_messages,
                "payload_bytes": payload_bytes,
                "flush_every": flush_every,
                "progress": progress,
                "written_before_restart": written,
                "written_after_restart": written_after_restart,
                "rss_delta_after_restart_kb": delta_kb,
                "rss_delta_per_message_bytes": delta_per_message,
                "samples": {
                    "empty_live": empty_live,
                    "empty_restart": empty_restart,
                    "after_write": after_write,
                    "head_restart": head_restart,
                },
            }

            report_path = yatest.common.output_path("pq_head_rss_report.json")
            with open(report_path, "w") as report_file:
                json.dump(report, report_file, sort_keys=True, indent=2)

            print("PQ_HEAD_RSS_REPORT {}".format(json.dumps(report, sort_keys=True)), file=sys.stderr)
            print("PQ_HEAD_RSS_REPORT_PATH {}".format(report_path), file=sys.stderr)

            assert written_after_restart >= total_messages
            if max_delta_mb is not None:
                assert delta_kb <= max_delta_mb * 1024, (
                    "PQ head reload RSS delta is {} KB, expected <= {} MB".format(delta_kb, max_delta_mb)
                )
        finally:
            driver.stop()
