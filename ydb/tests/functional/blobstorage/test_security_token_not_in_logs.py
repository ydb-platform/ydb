# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import uuid

import grpc
import pytest

from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels

BLOB_STORAGE_CONFIG_MARKER = 'NKikimrClient.TGRpcServer/BlobStorageConfig'


@pytest.fixture(scope='module')
def cluster(tmp_path_factory):
    work_dir = tmp_path_factory.mktemp('kikimr_grpc_log')
    config = KikimrConfigGenerator(
        additional_log_configs={'GRPC_SERVER': LogLevels.DEBUG},
        use_log_files=True,
        output_path=str(work_dir),
    )
    cluster = KiKiMR(configurator=config)
    cluster.start()
    yield cluster
    cluster.stop()


def test_blob_storage_config_security_token_not_logged_plaintext(cluster):
    # We need minimally filled request to trigger initial logging
    req = msgbus_pb2.TBlobStorageConfigRequest()
    req.SecurityToken = 'YDB_SENSITIVE_LEAK_' + uuid.uuid4().hex

    node = cluster.nodes[1]
    log_path = node.ydbd_log_file_path
    assert log_path, 'use_log_files must set ydbd log path'
    target = '{}:{}'.format(node.host, node.grpc_port)
    with grpc.insecure_channel(target) as channel:
        stub = grpc_pb2_grpc.TGRpcServerStub(channel)
        stub.BlobStorageConfig(req, timeout=60)

    deadline = time.time() + 30.0
    found = False
    while time.time() < deadline:
        with open(log_path, 'rb') as f:
            combined = f.read().decode('utf-8', errors='replace')
        if BLOB_STORAGE_CONFIG_MARKER in combined and 'SecurityToken' in combined:
            found = True
            assert req.SecurityToken not in combined, 'SecurityToken plaintext leaked into ydbd log'
            break
        time.sleep(0.2)

    assert found, 'Expected log line was not found in GRPC_SERVER request log'
