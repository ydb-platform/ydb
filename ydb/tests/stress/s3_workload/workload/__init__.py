# -*- coding: utf-8 -*-
import ydb
import boto3
import tempfile
import time
import threading
import uuid
import os
import sys
import yatest
import json

from typing import List, Dict, Any, Optional

from ydb.tests.stress.common.common import WorkloadBase

class Workload(WorkloadBase):
    def __init__(self, client, endpoint, stop, s3_settings):
        super().__init__(client, "", "s3_workload", stop)
        self.lock = threading.Lock()
        self.s3_settings = s3_settings
        self.endpoint = endpoint
        self.limit = 10 # limit on the number of exports for a database
        self.in_progress = []
        # Statistics
        self._export_stats = {
            "PROGRESS_DONE": 0,
            "PROGRESS_CANCELLED": 0,
            "PROGRESS_UNSPECIFIED": 0,
        }
    
    def get_stat(self):
        with self.lock:
            export_stats_str = ", ".join(
                f"exports{ k[8:].lower() }={v}" if k.startswith("PROGRESS_") else f"exports_{k.lower()}={v}"
                for k, v in self._export_stats.items()
            )
            return export_stats_str

    def create_tables(self, table_names: List[str]):
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

    def create_topics(self, topic_names: List[str], consumers: Optional[Dict[str, List[str]]] = None):
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

    def insert_into_table(self, table_name: str, rows: List[Dict[str, Any]]):
        """Insert data in tables. rows: [{"id": ..., "message": ...}, ...]"""
        for row in rows:
            id_val = row["id"]
            msg_val = row["message"]
            self.client.query(
                f"INSERT INTO `{table_name}` (id, message) VALUES ({id_val}, '{msg_val}');",
                True
            )

    def setup_tables(self):
        tables = [
            f"{self.prefix}/table{i}" for i in range(1, 10)
        ]
        self.create_tables(tables)
        self._tables = tables

    def setup_topics(self):
        topics = [
            f"{self.prefix}/topic{i}" for i in range(1, 10)
        ]
        consumers = {}
        for i, topic in enumerate(topics, 1):
            consumers[topic] = [
                f"consumerA_{i}",
                f"consumerB_{i}"
            ]
        self.create_topics(topics, consumers)
        self._topics = topics
        self._consumers = consumers

    def insert_rows(self):
        # Insert 5 rows into each table
        for idx, table in enumerate(getattr(self, '_tables', []), 1):
            rows = [
                {"id": row_id, "message": f"Table {idx} ({table}) row {row_id}"}
                for row_id in range(1, 6)
            ]
            self.insert_into_table(table, rows)

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
        operation_forget_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--endpoint",
            self.endpoint,
            "--database=%s" % self.client.database,
            "operation",
            "forget",
            "%s" % export_id,
        ]
        yatest.common.execute(operation_forget_command, wait=True, stdout=sys.stdout, stderr=sys.stderr)
    
    def _export_is_completed(self, export_id):
        operation_get_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--endpoint",
            self.endpoint,
            "--database=%s" % self.client.database,
            "operation",
            "get",
            "%s" % export_id,
            "--format",
            "proto-json-base64"
        ]
        result_get = self._execute_command_and_get_result(operation_get_command)
        progress_export = result_get["metadata"]["progress"]
        if progress_export in self._export_stats:
            self._export_stats[progress_export] += 1
            self._export_forget(export_id)
            return True
        else:
            return False

    def _cleanup_in_progress(self):
        self.in_progress = [export_id for export_id in self.in_progress if not self._export_is_completed(export_id)]

    def export_to_s3(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_settings
        export_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--endpoint",
            self.endpoint,
            "--database=" + self.client.database,
            "export",
            "s3",
            "--s3-endpoint",
            s3_endpoint,
            "--bucket",
            s3_bucket,
            "--access-key",
            s3_access_key,
            "--secret-key",
            s3_secret_key,
            "--item",
            "src=" + self.prefix + ",dst=" + self.prefix,
            "--format",
            "proto-json-base64"
        ]

        with self.lock:
            self._cleanup_in_progress()
            while len(self.in_progress) >= self.limit:
                self._cleanup_in_progress()

            result_export = self._execute_command_and_get_result(export_command)
            export_id = result_export["id"]
            self.in_progress.append(export_id)

    def _loop(self):
        for _ in range(0, 11):
            self.id = f"{uuid.uuid1()}".replace("-", "_")
            self.prefix = f"block_{self.id}"
            self.setup_tables()
            self.setup_topics()
            self.insert_rows()
            self.export_to_s3()
    
    def get_workload_thread_funcs(self):
        return [self._loop]

class WorkloadRunner:
    def __init__(self, client, endpoint, duration):
        self.client = client
        self.endpoint = endpoint
        self.duration = duration
        ydb.interceptor.monkey_patch_event_handler()

    @staticmethod
    def setup_s3():
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
        s3_settings = self.setup_s3()
        workloads = [
            Workload(self.client, self.endpoint, stop, s3_settings)
        ]

        for w in workloads:
            w.start()
        started_at = started_at = time.time()
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
