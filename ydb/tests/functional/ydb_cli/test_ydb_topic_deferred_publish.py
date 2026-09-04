# -*- coding: utf-8 -*-
"""
Functional smoke tests for deferred topic publish via experimental CLI.

Scenarios mirror the demo happy path / cancel / list-describe flows:
  begin → write --deferred-int-publication-id → publish|cancel → topic read

The same suite runs on dedicated (/Root) and serverless databases.
"""

import logging
import json
import os
import tempfile
import uuid

import yatest

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import (
    BaseCliTestWithDatabase,
    set_ydb_cli_test_canondata_root,
    ydb_bin,
)
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

AUTH_TOKEN = "root@builtin"
CONSUMER = "dp-consumer"


class TestTopicDeferredPublishCli(BaseCliTestWithDatabase):
    CLI_BINARY_ENV = "YDB_CLI_EXPERIMENTAL_BINARY"

    @classmethod
    def get_cluster_configurator(cls):
        return KikimrConfigGenerator(
            extra_feature_flags=["enable_topic_deferred_publish"],
        )

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.cli_database = cls.root_dir

    @classmethod
    def _auth_env(cls, extra=None):
        env = os.environ.copy()
        env["YDB_TOKEN"] = AUTH_TOKEN
        if extra:
            env.update(extra)
        return env

    @classmethod
    def execute_exp(cls, args, stdin=None, check_exit_code=True, with_auth=True):
        execution = yatest.common.execute(
            [
                ydb_bin(cls.CLI_BINARY_ENV),
                "--endpoint", cls.grpc_endpoint(),
                "--database", cls.cli_database,
            ] + args,
            stdin=stdin,
            check_exit_code=check_exit_code,
            env=cls._auth_env() if with_auth else None,
        )
        result = cls.ExecutionResult(
            stdout=execution.std_out.decode("utf-8") if execution.std_out else "",
            stderr=execution.std_err.decode("utf-8") if execution.std_err else "",
            exit_code=execution.exit_code,
        )
        logger.debug("args: %s", args)
        logger.debug("stdout:\n%s", result.stdout)
        logger.debug("stderr:\n%s", result.stderr)
        logger.debug("exit_code: %d", result.exit_code)
        return result

    @classmethod
    def _unique_topic(cls, prefix):
        return f"{cls.cli_database}/{prefix}-{uuid.uuid4().hex[:8]}"

    @classmethod
    def _prepare_topic(cls, topic_path):
        # Scheme/topic DDL in this harness is anonymous-friendly; deferred RPCs require auth.
        cls.driver.topic_client.create_topic(
            topic_path,
            consumers=[CONSUMER],
            min_active_partitions=1,
        )

    @classmethod
    def _begin(cls, ext_id, writer_identity=None):
        args = ["experimental", "topic", "deferred-publication", "begin", "--ext-publication-id", ext_id]
        if writer_identity is not None:
            args.extend(["--writer-identity", writer_identity])
        result = cls.execute_exp(args)
        int_id = result.stdout.strip()
        assert int_id.isdigit(), f"expected int_publication_id, got: {result.stdout!r}"
        return int_id

    @classmethod
    def _write_deferred(cls, topic_path, int_id, payload, ext_id=None):
        args = [
            "experimental", "topic", "write", topic_path,
            "--deferred-int-publication-id", int_id,
            "--format", "single-message",
        ]
        if ext_id is not None:
            args.extend(["--deferred-ext-publication-id", ext_id])
        with tempfile.NamedTemporaryFile("w+b") as stdin_file:
            stdin_file.write(payload.encode("utf-8"))
            stdin_file.flush()
            stdin_file.seek(0)
            return cls.execute_exp(args, stdin=stdin_file)

    @classmethod
    def _publish(cls, int_id, check_exit_code=True):
        return cls.execute_exp(
            ["experimental", "topic", "deferred-publication", "publish", "--int-publication-id", int_id],
            check_exit_code=check_exit_code,
        )

    @classmethod
    def _cancel(cls, int_id, check_exit_code=True):
        return cls.execute_exp(
            ["experimental", "topic", "deferred-publication", "cancel", "--int-publication-id", int_id],
            check_exit_code=check_exit_code,
        )

    @classmethod
    def _list(cls, writer_identity=None):
        args = ["experimental", "topic", "deferred-publication", "list"]
        if writer_identity is not None:
            args.extend(["--writer-identity", writer_identity])
        return cls.execute_exp(args)

    @classmethod
    def _describe(cls, int_id, check_exit_code=True, output_format=None):
        args = ["experimental", "topic", "deferred-publication", "describe", "--int-publication-id", int_id]
        if output_format is not None:
            args.extend(["--format", output_format])
        return cls.execute_exp(args, check_exit_code=check_exit_code)

    @classmethod
    def _read(cls, topic_path, limit=10):
        # Prefer CLI topic read without token: anonymous works for read in this harness.
        return cls.execute_exp(
            [
                "topic", "read", topic_path,
                "--consumer", CONSUMER,
                "--limit", str(limit),
                "--format", "single-message",
                "--idle-timeout", "2s",
                "--commit", "1",
            ],
            check_exit_code=True,
            with_auth=False,
        )

    def test_happy_path_publish(self):
        topic = self._unique_topic("dp-happy")
        self._prepare_topic(topic)
        ext_id = f"order-{uuid.uuid4().hex[:8]}"
        payload = "hello-deferred-publish"

        int_id = self._begin(ext_id)
        describe = self._describe(int_id)
        assert ext_id in describe.stdout
        assert AUTH_TOKEN in describe.stdout
        assert f"int_publication_id: {int_id}" in describe.stdout

        describe_json = self._describe(int_id, output_format="json")
        description = json.loads(describe_json.stdout)
        assert description["int_publication_id"] == int(int_id)
        assert description["ext_publication_id"] == ext_id

        listed = self._list()
        assert ext_id in listed.stdout
        assert int_id in listed.stdout

        self._write_deferred(topic, int_id, payload, ext_id=ext_id)

        before = self._read(topic)
        assert payload not in before.stdout

        publish = self._publish(int_id)
        assert publish.exit_code == 0

        after = self._read(topic)
        assert payload in after.stdout

        repeat = self._publish(int_id, check_exit_code=False)
        assert repeat.exit_code != 0

    def test_cancel_discards_staged_data(self):
        topic = self._unique_topic("dp-cancel")
        self._prepare_topic(topic)
        ext_id = f"cancel-{uuid.uuid4().hex[:8]}"
        payload = "to-be-cancelled"

        int_id = self._begin(ext_id)
        self._write_deferred(topic, int_id, payload, ext_id=ext_id)

        cancel = self._cancel(int_id)
        assert cancel.exit_code == 0

        read = self._read(topic)
        assert payload not in read.stdout

        describe = self._describe(int_id, check_exit_code=False)
        assert describe.exit_code != 0

    def test_write_without_ext_id(self):
        topic = self._unique_topic("dp-no-ext")
        self._prepare_topic(topic)
        ext_id = f"no-ext-{uuid.uuid4().hex[:8]}"
        payload = "payload-without-ext"

        int_id = self._begin(ext_id)
        self._write_deferred(topic, int_id, payload, ext_id=None)
        self._publish(int_id)

        read = self._read(topic)
        assert payload in read.stdout

    def test_duplicate_begin_while_active(self):
        topic = self._unique_topic("dp-dup-begin")
        self._prepare_topic(topic)
        ext_id = f"dup-{uuid.uuid4().hex[:8]}"

        int_id = self._begin(ext_id)
        duplicate = self.execute_exp(
            ["experimental", "topic", "deferred-publication", "begin", "--ext-publication-id", ext_id],
            check_exit_code=False,
        )
        assert duplicate.exit_code != 0

        self._cancel(int_id)
        self._begin(ext_id)

    def test_write_rejects_zero_deferred_int_id(self):
        topic = self._unique_topic("dp-zero-int")
        self._prepare_topic(topic)
        result = self.execute_exp(
            [
                "experimental", "topic", "write", topic,
                "--deferred-int-publication-id", "0",
                "--format", "single-message",
            ],
            check_exit_code=False,
        )
        assert result.exit_code != 0
        combined = (result.stdout + result.stderr).lower()
        assert "deferred-int-publication-id" in combined
        assert "positive" in combined

    def test_list_filter_by_writer_identity(self):
        writer = f"writer-{uuid.uuid4().hex[:8]}"
        other = f"other-{uuid.uuid4().hex[:8]}"
        ext_a = f"list-a-{uuid.uuid4().hex[:8]}"
        ext_b = f"list-b-{uuid.uuid4().hex[:8]}"

        int_a = self._begin(ext_a, writer_identity=writer)
        int_b = self._begin(ext_b, writer_identity=other)

        filtered = self._list(writer_identity=writer)
        assert ext_a in filtered.stdout
        assert int_a in filtered.stdout
        assert ext_b not in filtered.stdout

        self._cancel(int_a)
        self._cancel(int_b)


class TestTopicDeferredPublishCliServerless(TestTopicDeferredPublishCli):
    """Same CLI suite on a serverless tenant over a shared hostel database."""

    HOSTEL_DB = "/Root/hostel"
    SERVERLESS_DB = "/Root/serverless"

    @classmethod
    def setup_class(cls):
        set_ydb_cli_test_canondata_root()
        cls.cluster = cls._start_cluster(configurator=cls.get_cluster_configurator())
        cls.root_dir = "/Root"

        cls.cluster.create_hostel_database(
            cls.HOSTEL_DB,
            storage_pool_units_count={"hdd": 1},
        )
        cls.cluster.register_and_start_slots(cls.HOSTEL_DB, count=1)
        cls.cluster.wait_tenant_up(cls.HOSTEL_DB)
        cls.cluster.create_serverless_database(cls.SERVERLESS_DB, hostel_db=cls.HOSTEL_DB)

        cls.driver = cls._start_driver(
            cls.SERVERLESS_DB,
            credentials=ydb.AuthTokenCredentials(AUTH_TOKEN),
        )
        cls.cli_database = cls.SERVERLESS_DB
