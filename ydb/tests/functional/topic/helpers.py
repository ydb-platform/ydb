# -*- coding: utf-8 -*-
import json
import os
import subprocess
import time

import yatest
from google.protobuf import json_format

from ydb.core.persqueue.public.cloud_events.proto import topics_pb2
from ydb.tests.library.common.helpers import plain_or_under_sanitizer


NO_RECORDS_TIMEOUT = plain_or_under_sanitizer(2, 30)


def _decode_varint(data, pos):
    """Decode varint from data at pos. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
        if shift >= 64:
            raise ValueError("Varint too long")
    return result, pos


def _parse_length_delimited_messages(data):
    """Parse length-delimited protobuf messages. Returns list of raw message bytes."""
    messages = []
    pos = 0
    while pos < len(data):
        try:
            length, pos = _decode_varint(data, pos)
        except (ValueError, IndexError):
            break
        if pos + length > len(data):
            break
        messages.append(bytes(data[pos:pos + length]))
        pos += length
    return messages


def _protobuf_to_dict(msg):
    """Convert protobuf message to dict for canonization (preserving field names)."""
    return json_format.MessageToDict(msg, preserving_proto_field_name=True)


def _parse_protobuf_event(blob):
    """Parse a single blob as CreateTopic, AlterTopic, or DeleteTopic. Supports protobuf and JSON. Returns dict or None."""
    for msg_cls in (topics_pb2.CreateTopic, topics_pb2.AlterTopic, topics_pb2.DeleteTopic):
        try:
            msg = msg_cls()
            msg.ParseFromString(blob)
            if msg.event_metadata.event_type in TOPIC_CLOUD_EVENT_TYPES:
                return _protobuf_to_dict(msg)
        except Exception:
            continue
    # Fallback: C++ TFileEventsWriter now outputs JSON (length-delimited)
    try:
        event = json.loads(blob.decode('utf-8'))
        if isinstance(event, dict):
            meta = event.get('event_metadata') or {}
            if meta.get('event_type') in TOPIC_CLOUD_EVENT_TYPES:
                return event
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    return None


def make_test_file_with_content(human_readable_file_name, content):
    file_path = os.path.join(yatest.common.output_path(), human_readable_file_name)

    if os.path.exists(file_path):
        name, ext = os.path.splitext(human_readable_file_name)
        name, num = os.path.splitext(name)
        if num:
            num = str(int(num[1:]) + 1)  # '.1' -> '2'
        else:
            num = '1'
        return make_test_file_with_content(f'{name}.{num}{ext}', content)

    with open(file_path, 'w') as w:
        w.write(content)
    return file_path


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


# Topic cloud event types for audit
TOPIC_CLOUD_EVENT_TYPES = (
    'yandex.cloud.events.ydb.topics.CreateTopic',
    'yandex.cloud.events.ydb.topics.AlterTopic',
    'yandex.cloud.events.ydb.topics.DeleteTopic',
)


class CanonicalCaptureCloudEventOutput:
    """
    Captures topic cloud events from file (audit log or topic_cloud_events.json),
    normalizes variable fields, outputs one JSON line per event.
    Use with yatest.common.canonical_file via canonize().
    Supports: length-delimited protobuf from TFileEventsWriter, and audit log (cloud_event_json).
    """

    def __init__(self, filename, database_path):
        self.filename = filename
        self.database_path = database_path
        self.captured = ''
        self.read_lines = 0

    def __enter__(self):
        if not os.path.exists(self.filename):
            open(self.filename, 'a').close()
        size = os.path.getsize(self.filename)
        last_read_time = time.time()
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(size)
            while time.time() - last_read_time <= NO_RECORDS_TIMEOUT:
                time.sleep(0.1)
                line = f.readline()
                if len(line) > 0:
                    last_read_time = time.time()

        self.saved_pos = os.path.getsize(self.filename)
        return self

    def _canonize_event(self, event):
        """Normalize variable fields in cloud event for canonical comparison."""
        # event_metadata
        meta = event.get('event_metadata') or {}
        if meta.get('event_id'):
            meta['event_id'] = '<canonized_event_id>'
        if meta.get('created_at'):
            meta['created_at'] = '<canonized_created_at>'
        if 'cloud_id' in meta:
            meta['cloud_id'] = '<canonized_cloud_id>'
        if 'folder_id' in meta:
            meta['folder_id'] = '<canonized_folder_id>'

        # details.path and request_parameters.path
        for section in ('details', 'request_parameters'):
            obj = event.get(section) or {}
            path = obj.get('path')
            if path and isinstance(path, str) and path.startswith(self.database_path):
                topic_name = path[len(self.database_path):].lstrip('/')
                obj['path'] = f'<database_path>/{topic_name}'

        # authorization.permissions[].resource_id (contains database path + topic)
        auth = event.get('authorization') or {}
        for perm in auth.get('permissions') or []:
            if isinstance(perm, dict):
                rid = perm.get('resource_id')
                if rid and isinstance(rid, str) and self.database_path and rid.startswith(self.database_path):
                    topic_name = rid[len(self.database_path):].lstrip('/')
                    perm['resource_id'] = f'<database_path>/{topic_name}'

        # request_metadata.remote_address (port varies per run)
        req_meta = event.get('request_metadata') or {}
        if 'remote_address' in req_meta:
            req_meta['remote_address'] = '<canonized_remote_address>'
        if req_meta.get('user_agent'):
            req_meta['user_agent'] = '<canonized_user_agent>'

        return event

    def _process_line(self, line_str):
        if not any(t in line_str for t in TOPIC_CLOUD_EVENT_TYPES):
            return None

        json_start = line_str.find('{')
        if json_start == -1:
            return None

        # Format 1: audit log with cloud_event_json wrapper
        if 'cloud_event_json' in line_str:
            try:
                outer = json.loads(line_str[json_start:])
            except json.JSONDecodeError:
                return None
            inner_json = outer.get('cloud_event_json')
            if not inner_json:
                return None
            try:
                event = json.loads(inner_json)
            except json.JSONDecodeError:
                return None
        else:
            # Format 2: raw JSON from TFileEventsWriter (topic_cloud_events.json)
            try:
                event = json.loads(line_str[json_start:])
            except json.JSONDecodeError:
                return None
            if not isinstance(event, dict):
                return None
            meta = event.get('event_metadata') or {}
            if meta.get('event_type') not in TOPIC_CLOUD_EVENT_TYPES:
                return None

        event = self._canonize_event(event)
        return json.dumps(event, sort_keys=True) + '\n'

    def __exit__(self, *exc):
        last_read_time = time.time()
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            while time.time() - last_read_time <= NO_RECORDS_TIMEOUT:
                time.sleep(0.1)
                chunk = f.readline()
                if len(chunk) == 0:
                    continue
                last_read_time = time.time()
                # Format: length-delimited protobuf (TFileEventsWriter)
                messages = _parse_length_delimited_messages(chunk)
                if messages:
                    for blob in messages:
                        event = _parse_protobuf_event(blob)
                        if event:
                            event = self._canonize_event(event)
                            self.captured += json.dumps(event, sort_keys=True) + '\n'
                            self.read_lines += 1
                else:
                    # Fallback: audit log (JSON with cloud_event_json) or legacy JSON
                    try:
                        canonized = self._process_line(chunk.decode('utf-8'))
                        if canonized:
                            self.captured += canonized
                            self.read_lines += 1
                    except UnicodeDecodeError:
                        pass

    def canonize(self):
        # Use only the last captured event: tests that run setup (e.g. create_topic) before
        # the capture block may receive setup's event asynchronously; we want only the event
        # from the operation inside the capture block.
        lines = [s for s in self.captured.strip().split('\n') if s]
        content = (lines[-1] + '\n') if lines else ''
        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=make_test_file_with_content('topic_cloud_events.json', content)
        )


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
