# -*- coding: utf-8 -*-
import boto3
import json
import os
import sys
import tempfile
import threading
import time
import uuid
import yatest
import ydb
from typing import Any, Dict, List, Optional

from ydb.tests.stress.common.common import WorkloadBase

from ydb.export import ExportClient
from ydb.export import ExportToS3Settings
from ydb.operation import OperationClient


class WorkloadS3Export(WorkloadBase):
    def __init__(self, client, endpoint, stop, s3_settings):
        super().__init__(client, "", "s3_export", stop)
        self.lock = threading.Lock()
        self.s3_settings = s3_settings
        self.endpoint = endpoint
        self.s3_client = ExportClient(self.client.driver)
        self.op_client = OperationClient(self.client.driver)

        self.limit = 10  # limit on the number of exports for a database
        self.in_progress = []
        # Statistics
        self._export_stats = {
            "DONE": 0,
            "CANCELLED": 0,
            "UNSPECIFIED": 0,
        }

    def get_stat(self):
        with self.lock:
            export_stats_str = ", ".join(
                f"exports_{k.lower()}={v}"
                for k, v in self._export_stats.items()
            )
            return export_stats_str

    def _create_tables(self, table_names: List[str]):
        """Create several tables with the same schema."""
        for name in table_names:
            self.client.query(
                f"""
                    CREATE TABLE `{name}` (
                        id Uint32 NOT NULL,
                        message Utf8,
                        INDEX idx_message GLOBAL SYNC ON (message),
                        PRIMARY KEY (id)
                    );
                """,
                True
            )

    def _create_topics(self, topic_names: List[str], consumers: Optional[Dict[str, List[str]]] = None):
        """Create several topics, optionally with consumers.
        consumers: {topic_name: [consumer1, consumer2, ...]}
        """
        for name in topic_names:
            self.client.query(
                f"CREATE TOPIC `{name}`;",
                True
            )
            if consumers and name in consumers:
                for consumer in consumers[name]:
                    self.client.query(
                        f"ALTER TOPIC `{name}` ADD CONSUMER {consumer};",
                        True
                    )

    def _insert_into_table(self, table_name: str, rows: List[Dict[str, Any]]):
        """Insert data in tables. rows: [{"id": ..., "message": ...}, ...]"""
        for row in rows:
            id_val = row["id"]
            msg_val = row["message"]
            self.client.query(
                f"INSERT INTO `{table_name}` (id, message) VALUES ({id_val}, '{msg_val}');",
                True
            )

    def _setup_tables(self):
        tables = [
            f"{self.prefix}/table{i}" for i in range(1, 10)
        ]
        self._create_tables(tables)
        self._tables = tables

    def _setup_topics(self):
        topics = [
            f"{self.prefix}/topic{i}" for i in range(1, 10)
        ]
        consumers = {}
        for i, topic in enumerate(topics, 1):
            consumers[topic] = [
                f"consumerA_{i}",
                f"consumerB_{i}"
            ]
        self._create_topics(topics, consumers)
        self._topics = topics
        self._consumers = consumers

    def _insert_rows(self):
        # Insert 5 rows into each table
        for idx, table in enumerate(getattr(self, '_tables', []), 1):
            rows = [
                {"id": row_id, "message": f"Table {idx} ({table}) row {row_id}"}
                for row_id in range(1, 6)
            ]
            self._insert_into_table(table, rows)

    @staticmethod
    def _execute_command_and_get_result(command):
        with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file:
            yatest.common.execute(command, wait=True, stdout=temp_file, stderr=sys.stderr)
            temp_file.flush()
            temp_file.seek(0)
            content = temp_file.read()
            print(content)
            result = json.loads(content)
            return result

    def _export_forget(self, export_id):
        self.op_client.forget(export_id)

    def _export_is_completed(self, export_id):
        progress_export = self.s3_client.get_export_to_s3_operation(export_id).progress.name
        if progress_export in self._export_stats:
            self._export_stats[progress_export] += 1
            self._export_forget(export_id)
            return True
        else:
            return False

    def _cleanup_in_progress(self):
        self.in_progress = [export_id for export_id in self.in_progress if not self._export_is_completed(export_id)]

    def _export_to_s3(self):
        with self.lock:
            self._cleanup_in_progress()
            while len(self.in_progress) >= self.limit:
                self._cleanup_in_progress()
                time.sleep(0.1)
            result_export = self.s3_client.export_to_s3(self.settings)
            export_id = result_export.id
            self.in_progress.append(export_id)

    def _loop(self):
        while not self.is_stop_requested():
            self.id = f"{uuid.uuid1()}".replace("-", "_")
            self.prefix = f"block_{self.id}"
            s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_settings
            self.settings = (
                ExportToS3Settings()
                .with_endpoint(s3_endpoint)
                .with_access_key(s3_access_key)
                .with_secret_key(s3_secret_key)
                .with_bucket(s3_bucket)
                .with_source_and_destination(self.prefix, self.prefix)
            )
            self._setup_tables()
            self._setup_topics()
            self._insert_rows()
            self._export_to_s3()

    def get_workload_thread_funcs(self):
        return [self._loop]


class WorkloadRunner:
    def __init__(self, client, endpoint, duration):
        self.client = client
        self.endpoint = endpoint
        self.duration = duration
        ydb.interceptor.monkey_patch_event_handler()

    @staticmethod
    def _setup_s3():
        s3_endpoint = os.getenv("S3_ENDPOINT")
        s3_access_key = "minio"
        s3_secret_key = "minio123"
        s3_bucket = "export_test_bucket"

        resource = boto3.resource("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

        bucket = resource.Bucket(s3_bucket)
        if not bucket.creation_date:
            bucket.create()
        bucket.objects.all().delete()

        return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        stop = threading.Event()
        s3_settings = self._setup_s3()
        workloads = [
            WorkloadS3Export(self.client, self.endpoint, stop, s3_settings)
        ]

        for w in workloads:
            w.start()
        started_at = time.time()
        while time.time() - started_at < self.duration:
            print(f"Elapsed {(int)(time.time() - started_at)} seconds, stat:", file=sys.stderr)
            for w in workloads:
                print(f"\t{w.name}: {w.get_stat()}", file=sys.stderr)
            time.sleep(10)
        stop.set()
        print("Waiting for stop...", file=sys.stderr)
        for w in workloads:
            w.join()
        print("Stopped", file=sys.stderr)
