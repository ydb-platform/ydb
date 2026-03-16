# -*- coding: utf-8 -*-
import os
import subprocess
import time


def cluster_endpoint(cluster):
    return f'{cluster.nodes[1].host}:{cluster.nodes[1].grpc_port}'


class CaptureFileOutput:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __exit__(self, *exc):
        time.sleep(0.5)
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            self.captured = f.read().decode('utf-8')


def ydbcli_db_schema_exec(cluster, operation_proto):
    endpoint = cluster_endpoint(cluster)
    args = [
        cluster.nodes[1].binary_path,
        f'--server=grpc://{endpoint}',
        'db',
        'schema',
        'exec',
        operation_proto,
    ]
    r = subprocess.run(args, capture_output=True, env={**os.environ})
    assert r.returncode == 0, r.stderr.decode('utf-8')


def ydbcli_db_schema_exec_allow_failure(cluster, operation_proto):
    """Execute ModifyScheme; does not assert on failure. Returns subprocess.CompletedProcess."""
    endpoint = cluster_endpoint(cluster)
    args = [
        cluster.nodes[1].binary_path,
        f'--server=grpc://{endpoint}',
        'db',
        'schema',
        'exec',
        operation_proto,
    ]
    return subprocess.run(args, capture_output=True, env={**os.environ})
