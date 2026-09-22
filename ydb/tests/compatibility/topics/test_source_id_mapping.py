# -*- coding: utf-8 -*-
import logging
import time

import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, current_binary_path
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)

PARTITION_COUNT = 100
PARTITION_ID = 37
PRODUCER_ID = "compat-srcid-producer"
SEQNO_FIRST = 13
SEQNO_SECOND = 14
PAYLOAD = b"compat-srcid-payload"

FCC_TOPIC = "srcid_compat_topic"

FED_DC = "dc1"
FED_ACCOUNT = "account"
FED_TOPIC_PATH = "/Root/account/topic"
FED_TOPIC_ALIASES = (
    FED_TOPIC_PATH,
    "account/topic",
    "account--topic",
)
SOURCE_ID_META2_PATH = "/Root/PQ/SourceIdMeta2"
CLUSTER_TABLE_PATH = "/Root/PQ/Config/V2/Cluster"
VERSION_TABLE_PATH = "/Root/PQ/Config/V2/Versions"

MAPPING_BY_ID_FLAG = "enable_topic_source_id_mapping_by_id"
TOPIC_ID = "1234567"


def execute_query(driver, query):
    with ydb.QuerySessionPool(driver) as pool:
        return pool.execute_with_retries(query)


def write_with_seqno(driver, topic, seqno, partition_id=None):
    deadline = time.time() + 180
    last_error = None
    while time.time() < deadline:
        try:
            with driver.topic_client.writer(
                topic,
                producer_id=PRODUCER_ID,
                partition_id=partition_id,
                auto_seqno=False,
            ) as writer:
                return writer.write_with_ack(
                    ydb.TopicWriterMessage(PAYLOAD, seqno=seqno),
                    timeout=60,
                )
        except Exception as exc:
            last_error = exc
            logger.info("write retry: %s", exc)
            time.sleep(2)
    raise AssertionError(f"write seqno={seqno} to {topic!r} failed: {last_error}")


def wait_init_last_seqno(driver, topic, expected_seqno):
    deadline = time.time() + 180
    last_error = None
    last_seqno = None
    while time.time() < deadline:
        try:
            with driver.topic_client.writer(topic, producer_id=PRODUCER_ID, auto_seqno=False) as writer:
                last_seqno = writer.wait_init(timeout=60).last_seqno
                if last_seqno == expected_seqno:
                    return
                logger.info("init on %r last_seqno=%s, expected %s", topic, last_seqno, expected_seqno)
        except Exception as exc:
            last_error = exc
            logger.info("wait_init retry: %s", exc)
        time.sleep(2)
    raise AssertionError(
        f"writer init on {topic!r} last_seqno={last_seqno}, expected {expected_seqno}, error={last_error}"
    )


def write_after_init(driver, topic, expected_init_seqno, seqno):
    deadline = time.time() + 180
    last_error = None
    last_seqno = None
    while time.time() < deadline:
        try:
            with driver.topic_client.writer(topic, producer_id=PRODUCER_ID, auto_seqno=False) as writer:
                last_seqno = writer.wait_init(timeout=60).last_seqno
                if last_seqno != expected_init_seqno:
                    logger.info("init on %r last_seqno=%s, expected %s", topic, last_seqno, expected_init_seqno)
                else:
                    return writer.write_with_ack(
                        ydb.TopicWriterMessage(PAYLOAD, seqno=seqno),
                        timeout=60,
                    )
        except Exception as exc:
            last_error = exc
            logger.info("write_after_init retry: %s", exc)
        time.sleep(2)
    raise AssertionError(
        f"write seqno={seqno} to {topic!r} after init last_seqno={last_seqno} "
        f"(expected {expected_init_seqno}) failed: {last_error}"
    )


def init_federation_tables(driver):
    scheme = ydb.SchemeClient(driver)
    for path in ("/Root/account", "/Root/PQ", "/Root/PQ/Config", "/Root/PQ/Config/V2"):
        try:
            scheme.make_directory(path)
        except Exception as exc:
            logger.info("make_directory %s: %s", path, exc)

    for query in (
        f"""
        --!syntax_v1
        CREATE TABLE `{CLUSTER_TABLE_PATH}` (
            name Utf8,
            balancer Utf8,
            local Bool,
            enabled Bool,
            weight Uint64,
            PRIMARY KEY (name)
        )
        """,
        f"""
        --!syntax_v1
        CREATE TABLE `{VERSION_TABLE_PATH}` (
            name Utf8,
            version Int64,
            PRIMARY KEY (name)
        )
        """,
        f"""
        --!syntax_v1
        CREATE TABLE `{SOURCE_ID_META2_PATH}` (
            Hash Uint32,
            SourceId Utf8,
            Topic Utf8,
            Partition Uint32,
            CreateTime Uint64,
            AccessTime Uint64,
            SeqNo Uint64,
            PRIMARY KEY (Hash, SourceId, Topic)
        )
        """,
    ):
        try:
            execute_query(driver, query)
        except Exception as exc:
            logger.info("ddl: %s", exc)

    execute_query(
        driver,
        f"""
        UPSERT INTO `{CLUSTER_TABLE_PATH}` (name, balancer, local, enabled, weight)
        VALUES ('{FED_DC}', 'localhost', true, true, 1000)
        """,
    )
    execute_query(
        driver,
        f"""
        UPSERT INTO `{VERSION_TABLE_PATH}` (name, version)
        VALUES ('Cluster', 1)
        """,
    )
    # ClusterTracker polls Cluster/Versions; writers fail with INITIALIZING until then.
    time.sleep(5)


def assign_topic_id(driver, path, topic_id=TOPIC_ID):
    deadline = time.time() + 180
    last_error = None
    while time.time() < deadline:
        try:
            driver.topic_client.alter_topic(path, alter_attributes={"_id": topic_id})
            return
        except Exception as exc:
            last_error = exc
            logger.info("alter_topic _id retry: %s", exc)
            time.sleep(2)
    raise AssertionError(f"alter_topic {path} _id={topic_id} failed: {last_error}")


def create_topic(driver, path, attributes=None):
    deadline = time.time() + 180
    last_error = None
    kwargs = {"min_active_partitions": PARTITION_COUNT}
    if attributes:
        kwargs["attributes"] = attributes
    while time.time() < deadline:
        try:
            driver.topic_client.create_topic(path, **kwargs)
            return
        except Exception as exc:
            last_error = exc
            logger.info("create_topic retry: %s", exc)
            time.sleep(2)
    raise AssertionError(f"create_topic {path} failed: {last_error}")


def run_scenario(fixture, topic, aliases, attributes=None):
    create_topic(fixture.driver, topic, attributes)

    first_ack = write_with_seqno(fixture.driver, topic, SEQNO_FIRST, partition_id=PARTITION_ID)
    assert isinstance(first_ack, ydb.TopicWriteResult.Written), first_ack

    fixture.change_cluster_version()

    for alias in aliases:
        if alias != topic:
            wait_init_last_seqno(fixture.driver, alias, SEQNO_FIRST)

    second_ack = write_after_init(fixture.driver, topic, SEQNO_FIRST, SEQNO_SECOND)
    assert isinstance(second_ack, ydb.TopicWriteResult.Written), second_ack


def run_mapping_by_id_scenario(fixture, topic, aliases, attributes=None):
    create_topic(fixture.driver, topic, attributes)

    first_ack = write_with_seqno(fixture.driver, topic, SEQNO_FIRST, partition_id=PARTITION_ID)
    assert isinstance(first_ack, ydb.TopicWriteResult.Written), first_ack

    fixture.config.yaml_config.setdefault("feature_flags", {})[MAPPING_BY_ID_FLAG] = True
    fixture.change_cluster_version()

    assign_topic_id(fixture.driver, topic)

    for alias in aliases:
        if alias != topic:
            wait_init_last_seqno(fixture.driver, alias, SEQNO_FIRST)

    second_ack = write_after_init(fixture.driver, topic, SEQNO_FIRST, SEQNO_SECOND)
    assert isinstance(second_ack, ydb.TopicWriteResult.Written), second_ack


class TestSourceIdMappingFcc(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_producer_session_after_restart(self):
        run_scenario(
            self,
            topic=FCC_TOPIC,
            aliases=(),
        )


class TestSourceIdMappingFederation(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(use_legacy_pq=True)

    def test_producer_session_after_restart(self):
        init_federation_tables(self.driver)
        run_scenario(
            self,
            topic=FED_TOPIC_PATH,
            aliases=FED_TOPIC_ALIASES,
            attributes={"_federation_account": FED_ACCOUNT},
        )


class TestSourceIdMappingByIdFederation(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.all_binary_paths[1] != current_binary_path:
            pytest.skip("EnableTopicSourceIdMappingById is available only on current")
        yield from self.setup_cluster(use_legacy_pq=True)

    def test_producer_session_after_enabling_mapping_by_id(self):
        init_federation_tables(self.driver)
        run_mapping_by_id_scenario(
            self,
            topic=FED_TOPIC_PATH,
            aliases=FED_TOPIC_ALIASES,
            attributes={"_federation_account": FED_ACCOUNT},
        )
