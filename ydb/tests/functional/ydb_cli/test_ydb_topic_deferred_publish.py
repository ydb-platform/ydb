# -*- coding: utf-8 -*-
"""
Functional smoke tests for deferred topic publish via experimental CLI.

Scenarios mirror the demo happy path / cancel / list-describe flows:
  begin → write --deferred-int-id → publish|cancel → topic read

Also covers serverless DB: registry tables under the tenant .metadata and
Publish/Cancel finalize transactions.
"""

import logging
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
PUBLICATIONS_TABLE = "topic_deferred_publications"
DESTINATIONS_TABLE = "topic_deferred_publication_destinations"


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
    def _scheme_path_exists(cls, path, scheme_driver=None):
        driver = scheme_driver if scheme_driver is not None else cls.driver
        try:
            driver.scheme_client.describe_path(path)
            return True
        except ydb.SchemeError:
            return False
        except ydb.Unauthorized:
            # System .metadata tables often deny Describe to ordinary SIDs; that still
            # proves the path exists (missing paths come back as SchemeError).
            return True

    @classmethod
    def _metadata_table_path(cls, database, table_name):
        return f"{database}/.metadata/{table_name}"

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
        args = ["experimental", "topic", "deferred-publication", "begin", "--ext-id", ext_id]
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
            "--deferred-int-id", int_id,
            "--format", "single-message",
        ]
        if ext_id is not None:
            args.extend(["--deferred-ext-id", ext_id])
        with tempfile.NamedTemporaryFile("w+b") as stdin_file:
            stdin_file.write(payload.encode("utf-8"))
            stdin_file.flush()
            stdin_file.seek(0)
            return cls.execute_exp(args, stdin=stdin_file)

    @classmethod
    def _publish(cls, int_id, check_exit_code=True):
        return cls.execute_exp(
            ["experimental", "topic", "deferred-publication", "publish", "--int-id", int_id],
            check_exit_code=check_exit_code,
        )

    @classmethod
    def _cancel(cls, int_id, check_exit_code=True):
        return cls.execute_exp(
            ["experimental", "topic", "deferred-publication", "cancel", "--int-id", int_id],
            check_exit_code=check_exit_code,
        )

    @classmethod
    def _list(cls, writer_identity=None):
        args = ["experimental", "topic", "deferred-publication", "list"]
        if writer_identity is not None:
            args.extend(["--writer-identity", writer_identity])
        return cls.execute_exp(args)

    @classmethod
    def _describe(cls, int_id, check_exit_code=True):
        return cls.execute_exp(
            ["experimental", "topic", "deferred-publication", "describe", "--int-id", int_id],
            check_exit_code=check_exit_code,
        )

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
            ["experimental", "topic", "deferred-publication", "begin", "--ext-id", ext_id],
            check_exit_code=False,
        )
        assert duplicate.exit_code != 0

        self._cancel(int_id)
        again = self._begin(ext_id)
        assert again.isdigit()

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
    """Same CLI flows on a serverless tenant; assert registry tables land in the tenant .metadata."""

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

        credentials = ydb.AuthTokenCredentials(AUTH_TOKEN)
        cls.root_driver = cls._start_driver(cls.root_dir, credentials=credentials)
        cls.hostel_driver = cls._start_driver(cls.HOSTEL_DB, credentials=credentials)
        cls.driver = cls._start_driver(cls.SERVERLESS_DB, credentials=credentials)
        cls.cli_database = cls.SERVERLESS_DB

    @classmethod
    def teardown_class(cls):
        for attr in ("root_driver", "hostel_driver"):
            driver = getattr(cls, attr, None)
            if driver is not None:
                driver.stop()
        super().teardown_class()

    @classmethod
    def _assert_registry_tables_only_under(cls, database):
        # Tenant paths must be described with a driver bound to that database;
        # /Root DescribePath cannot see inside a serverless/hostel subdomain.
        tenant_driver = cls.driver if database == cls.cli_database else cls.root_driver
        pubs = cls._metadata_table_path(database, PUBLICATIONS_TABLE)
        dests = cls._metadata_table_path(database, DESTINATIONS_TABLE)
        assert cls._scheme_path_exists(pubs, tenant_driver), f"missing {pubs}"
        assert cls._scheme_path_exists(dests, tenant_driver), f"missing {dests}"

        for foreign_db, foreign_driver in (
            (cls.root_dir, cls.root_driver),
            (cls.HOSTEL_DB, cls.hostel_driver),
        ):
            if foreign_db == database:
                continue
            foreign_pubs = cls._metadata_table_path(foreign_db, PUBLICATIONS_TABLE)
            foreign_dests = cls._metadata_table_path(foreign_db, DESTINATIONS_TABLE)
            assert not cls._scheme_path_exists(foreign_pubs, foreign_driver), (
                f"registry leaked to {foreign_pubs}"
            )
            assert not cls._scheme_path_exists(foreign_dests, foreign_driver), (
                f"registry leaked to {foreign_dests}"
            )

    def test_metadata_tables_created_under_serverless_db(self):
        assert not self._scheme_path_exists(
            self._metadata_table_path(self.SERVERLESS_DB, PUBLICATIONS_TABLE),
            self.driver,
        )

        ext_id = f"sls-meta-{uuid.uuid4().hex[:8]}"
        int_id = self._begin(ext_id)
        self._assert_registry_tables_only_under(self.SERVERLESS_DB)

        describe = self._describe(int_id)
        assert ext_id in describe.stdout

        self._cancel(int_id)
        # Tables remain after Cancel; only the publication row is removed.
        self._assert_registry_tables_only_under(self.SERVERLESS_DB)
        gone = self._describe(int_id, check_exit_code=False)
        assert gone.exit_code != 0

    def test_happy_path_publish(self):
        topic = self._unique_topic("sls-happy")
        self._prepare_topic(topic)
        ext_id = f"sls-order-{uuid.uuid4().hex[:8]}"
        payload = "serverless-deferred-publish"

        int_id = self._begin(ext_id)
        self._assert_registry_tables_only_under(self.SERVERLESS_DB)

        self._write_deferred(topic, int_id, payload, ext_id=ext_id)
        describe = self._describe(int_id)
        assert topic in describe.stdout or topic.rsplit("/", 1)[-1] in describe.stdout

        before = self._read(topic)
        assert payload not in before.stdout

        publish = self._publish(int_id)
        assert publish.exit_code == 0

        after = self._read(topic)
        assert payload in after.stdout

        # Publish deleted the registry row; tables stay in the serverless tenant.
        self._assert_registry_tables_only_under(self.SERVERLESS_DB)
        gone = self._describe(int_id, check_exit_code=False)
        assert gone.exit_code != 0

        repeat = self._publish(int_id, check_exit_code=False)
        assert repeat.exit_code != 0

    def test_cancel_discards_staged_data(self):
        topic = self._unique_topic("sls-cancel")
        self._prepare_topic(topic)
        ext_id = f"sls-cancel-{uuid.uuid4().hex[:8]}"
        payload = "serverless-to-be-cancelled"

        int_id = self._begin(ext_id)
        self._write_deferred(topic, int_id, payload, ext_id=ext_id)

        cancel = self._cancel(int_id)
        assert cancel.exit_code == 0

        read = self._read(topic)
        assert payload not in read.stdout

        self._assert_registry_tables_only_under(self.SERVERLESS_DB)
        describe = self._describe(int_id, check_exit_code=False)
        assert describe.exit_code != 0
