#!/usr/bin/env python3
import logging
import os
import struct
import time

import grpc

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_cluster import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from ydb.public.api.grpc import ydb_actor_tracing_v1_pb2_grpc as actor_tracing_grpc
from ydb.public.api.protos import ydb_actor_tracing_pb2 as actor_tracing_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

logger = logging.getLogger(__name__)

TRACE_FILE_MAGIC = 0x41525459
EVENT_SIZE = 40
HEADER_FMT = "<IIIIq"
EVENT_FMT = "<QQQIHbbQ"

OUTPUT_DIR = os.path.join(os.getcwd(), "test_output")


def parse_trace_header(data):
    magic, version, node_id, header_size, event_count = struct.unpack_from(
        HEADER_FMT, data, 0)
    return magic, version, node_id, header_size, event_count


def parse_trace_events(data, header_size, event_count):
    events_offset = struct.calcsize(HEADER_FMT) + header_size
    events = []
    for i in range(event_count):
        offset = events_offset + i * EVENT_SIZE
        vals = struct.unpack_from(EVENT_FMT, data, offset)
        timestamp, actor1, actor2, aux, extra, ev_type, flags, handle_ptr = vals
        events.append({
            'timestamp': timestamp, 'actor1': actor1,
            'actor2': actor2, 'aux': aux, 'extra': extra,
            'type': ev_type,
            'handle_ptr': handle_ptr,
        })
    return events


def save_trace(data, name):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, name)
    with open(path, "wb") as f:
        f.write(data)
    logger.info("Saved trace to %s (%d bytes)", path, len(data))
    return path


def assert_trace_has_events(data, label=""):
    assert len(data) > 0, f"Trace data is empty {label}"
    magic, version, node_id, header_size, event_count = \
        parse_trace_header(data)
    assert magic == TRACE_FILE_MAGIC, f"Bad magic: {magic:#x}"
    assert version == 2
    assert node_id > 0
    assert event_count > 0, (
        f"No events {label}: header_size={header_size} "
        f"total_size={len(data)}")

    events = parse_trace_events(data, header_size, event_count)
    type_counts = {}
    for ev in events:
        t = ev['type']
        type_counts[t] = type_counts.get(t, 0) + 1
        assert ev['timestamp'] > 0

    names = {0: 'SendLocal', 1: 'ReceiveLocal', 2: 'New', 3: 'Die', 4: 'ForwardLocal'}
    for t, c in sorted(type_counts.items()):
        logger.info("  %s: %d", names.get(t, str(t)), c)

    assert type_counts.get(0, 0) > 0, "No SendLocal events"
    assert type_counts.get(1, 0) > 0, "No ReceiveLocal events"
    assert type_counts.get(2, 0) > 0, "No New events"
    return event_count


class ActorTracingTestBase:
    erasure = Erasure.NONE
    node_count = 1

    @classmethod
    def setup_class(cls):
        cls.configurator = KikimrConfigGenerator(
            erasure=cls.erasure,
            nodes=cls.node_count,
            use_in_memory_pdisks=True,
            extra_grpc_services=['actor_tracing'],
        )
        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def _get_stub(self, node_index=1):
        node = self.cluster.nodes[node_index]
        channel = grpc.insecure_channel(
            "%s:%d" % (node.host, node.grpc_port))
        return actor_tracing_grpc.ActorTracingServiceStub(channel)

    def _fetch_trace(self, stub):
        resp = stub.TraceFetch(actor_tracing_pb2.TraceFetchRequest())
        assert resp.operation.status == StatusIds.SUCCESS
        result = actor_tracing_pb2.TraceFetchResult()
        resp.operation.result.Unpack(result)
        return result.trace_data


class TestActorTracingSingleNode(ActorTracingTestBase):
    erasure = Erasure.NONE
    node_count = 1

    def test_start_stop(self):
        stub = self._get_stub()
        resp = stub.TraceStart(actor_tracing_pb2.TraceStartRequest())
        assert resp.operation.status == StatusIds.SUCCESS
        resp = stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        assert resp.operation.status == StatusIds.SUCCESS

    def test_fetch_returns_valid_data(self):
        stub = self._get_stub()

        stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        self._fetch_trace(stub)

        resp = stub.TraceStart(actor_tracing_pb2.TraceStartRequest())
        assert resp.operation.status == StatusIds.SUCCESS

        time.sleep(1)

        resp = stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        assert resp.operation.status == StatusIds.SUCCESS

        data = self._fetch_trace(stub)
        save_trace(data, "single_node_trace.bin")
        n = assert_trace_has_events(data, "single_node")
        logger.info("Single node: %d events", n)


class TestActorTracingMirror3DC(ActorTracingTestBase):
    erasure = Erasure.MIRROR_3_DC
    node_count = 3

    def test_start_stop(self):
        stub = self._get_stub(1)
        resp = stub.TraceStart(actor_tracing_pb2.TraceStartRequest())
        assert resp.operation.status == StatusIds.SUCCESS
        resp = stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        assert resp.operation.status == StatusIds.SUCCESS

    def test_fetch_returns_valid_data(self):
        stub = self._get_stub(1)

        stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        self._fetch_trace(stub)

        resp = stub.TraceStart(actor_tracing_pb2.TraceStartRequest())
        assert resp.operation.status == StatusIds.SUCCESS

        time.sleep(1)

        resp = stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        assert resp.operation.status == StatusIds.SUCCESS

        data = self._fetch_trace(stub)
        save_trace(data, "mirror3dc_node1_trace.bin")
        n = assert_trace_has_events(data, "mirror3dc_node1")
        logger.info("Mirror-3-DC node 1: %d events", n)

    def test_fetch_each_node(self):
        for node_idx in range(1, self.node_count + 1):
            stub = self._get_stub(node_idx)

            stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
            self._fetch_trace(stub)

            stub.TraceStart(actor_tracing_pb2.TraceStartRequest())

        time.sleep(1)

        for node_idx in range(1, self.node_count + 1):
            stub = self._get_stub(node_idx)
            stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
            data = self._fetch_trace(stub)
            fname = "mirror3dc_node%d_trace.bin" % node_idx
            save_trace(data, fname)
            n = assert_trace_has_events(data, "node_%d" % node_idx)
            logger.info("Node %d: %d events", node_idx, n)

    def test_tree_broadcast(self):
        stub = self._get_stub(1)

        for node_idx in range(1, self.node_count + 1):
            node_stub = self._get_stub(node_idx)
            node_stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
            self._fetch_trace(node_stub)

        resp = stub.TraceStart(actor_tracing_pb2.TraceStartRequest())
        assert resp.operation.status == StatusIds.SUCCESS

        time.sleep(1)

        resp = stub.TraceStop(actor_tracing_pb2.TraceStopRequest())
        assert resp.operation.status == StatusIds.SUCCESS

        time.sleep(0.5)

        data = self._fetch_trace(stub)
        save_trace(data, "tree_broadcast_trace.bin")
        n = assert_trace_has_events(data, "tree_broadcast")
        logger.info("Tree broadcast fetch returned %d events (from %d-node cluster)", n, self.node_count)
