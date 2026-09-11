import json
import re
import time
from pathlib import Path

import pytest
import yatest.common

from ydb.core.protos import node_broker_pb2
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


def canonical_log(tmp_path, observations):
    path = tmp_path / 'result.log'
    path.write_text(json.dumps(observations, indent=2, sort_keys=True) + '\n')
    return yatest.common.canonical_file(str(path), local=True, universal_lines=True)


class YdbGrpcLog:
    def __init__(self, cluster):
        node = cluster.nodes[1]
        self.path = Path(node.ydbd_log_file_path)
        self.offset = self.path.stat().st_size
        self.static_node_id = node.node_id

    def response(self, method):
        # Tests issue requests sequentially. Start at the pre-request offset so
        # previous requests and cluster setup cannot supply the expected line.
        pattern = re.compile(r'issuing response Name# [\w.]+/' + re.escape(method) + r' ')
        deadline = time.monotonic() + 10
        with self.path.open('rb') as log:
            log.seek(self.offset)
            while time.monotonic() < deadline:
                position = log.tell()
                line = log.readline()
                if not line.endswith(b'\n'):
                    log.seek(position)
                    time.sleep(0.05)
                    continue
                line = line.decode()
                match = pattern.search(line)
                if match is not None:
                    self.offset = log.tell()
                    return ['GRPC_SERVER DEBUG: ' + self.normalize(line[match.start():].strip())]
        pytest.fail(f'No YDB response log for {method} in {self.path} after offset {self.offset}')

    def normalize(self, line):
        line = re.sub(r' peer# [^,\s]+', ' peer# <peer>', line)
        # Discovery logs the result as binary protobuf in Any.value. Its
        # contents are checked separately by registration_result().
        line = re.sub(
            r'(type_url: "type.googleapis.com/Ydb.Discovery.NodeRegistrationResult" value: )"(?:[^"\\]|\\.)*"',
            r'\1"<registration-result>"', line,
        )
        line = re.sub(
            r'\bNodeId: (\d+)',
            lambda match: 'NodeId: ' + (
                '<dynamic-node>' if int(match[1]) > self.static_node_id else match[1]
            ), line,
        )
        line = re.sub(r'\bPort: [1-9]\d*', 'Port: <port>', line)
        return re.sub(r'\b(Expire(?:V2)?): [1-9]\d*', r'\1: <expiry>', line)


def registration_result(response, api, static_node_id, port):
    if api == 'discovery':
        operation = response.operation
        result = ydb_discovery_pb2.NodeRegistrationResult()
        log = {
            'ready': operation.ready,
            'status': StatusIds.StatusCode.Name(operation.status),
            'result_unpacked': operation.result.Unpack(result),
            'issues': [issue.message for issue in operation.issues],
        }
        node_id, expire, nodes = result.node_id, result.expire, result.nodes
        matches = [n for n in nodes if n.node_id == node_id and n.port == port]
    else:
        log = {
            'status': node_broker_pb2.TStatus.ECode.Name(response.Status.Code),
            'node_id_present': response.HasField('NodeId'),
        }
        node_id, expire, nodes = response.NodeId, response.Expire, response.Nodes
        matches = [n for n in nodes if n.NodeId == node_id and n.Port == port]
    log.update({
        'node_id_is_dynamic': node_id > static_node_id,
        'node_id_is_zero': node_id == 0,
        'expiry_positive': expire > 0,
        'registered_endpoint_present': bool(matches),
        'nodes_empty': not nodes,
    })
    return log
