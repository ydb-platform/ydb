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
    """Convert protobuf message to dict for canonization (preserving field names).
    Include default/empty fields to match canonical structure."""
    return json_format.MessageToDict(
        msg,
        preserving_proto_field_name=True,
        including_default_value_fields=True,
    )


def _parse_protobuf_event(blob):
    """Parse a single protobuf blob as CreateTopic, AlterTopic, or DeleteTopic. Returns dict or None."""
    for msg_cls in (topics_pb2.CreateTopic, topics_pb2.AlterTopic, topics_pb2.DeleteTopic):
        try:
            msg = msg_cls()
            msg.ParseFromString(blob)
            if msg.event_metadata.event_type in TOPIC_CLOUD_EVENT_TYPES:
                return _protobuf_to_dict(msg)
        except Exception:
            continue
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
        # Wait for file size to stabilize (no new writes for 0.5s), avoid long NO_RECORDS_TIMEOUT wait
        last_size = -1
        last_stable = time.time()
        while time.time() - last_stable <= min(2.0, NO_RECORDS_TIMEOUT):
            time.sleep(0.05)
            size = os.path.getsize(self.filename)
            if size != last_size:
                last_size = size
                last_stable = time.time()
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def _canonize_event(self, event):
        """Normalize variable fields in cloud event for canonical comparison.
        Ensure all canonical keys exist (protobuf may omit empty fields)."""
        # authentication: ensure subject_id exists
        authn = event.get('authentication') or {}
        if 'subject_id' not in authn:
            authn['subject_id'] = ''
        event['authentication'] = authn

        # event_metadata: canonize and ensure cloud_id, folder_id exist
        meta = event.get('event_metadata') or {}
        if meta.get('event_id'):
            meta['event_id'] = '<canonized_event_id>'
        if meta.get('created_at'):
            meta['created_at'] = '<canonized_created_at>'
        meta['cloud_id'] = '<canonized_cloud_id>'
        meta['folder_id'] = '<canonized_folder_id>'
        event['event_metadata'] = meta

        # details.path and request_parameters.path
        event_type = meta.get('event_type', '')
        for section in ('details', 'request_parameters'):
            obj = event.get(section) or {}
            path = obj.get('path')
            if path and isinstance(path, str) and path.startswith(self.database_path):
                topic_name = path[len(self.database_path):].lstrip('/')
                obj['path'] = f'<database_path>/{topic_name}'
            # DeleteTopic canonical: details and request_parameters only have path
            if 'DeleteTopic' in event_type:
                obj = {'path': obj.get('path', '')}
            event[section] = obj

        # DeleteTopic: details must have only path (protobuf may add default fields)
        if 'DeleteTopic' in event_type:
            event['details'] = {'path': (event.get('details') or {}).get('path', '')}

        # request_parameters: canonical has different structure per event type
        event_type = (event.get('event_metadata') or {}).get('event_type', '')
        rp = event.get('request_parameters') or {}
        if 'DeleteTopic' in event_type:
            event['request_parameters'] = {'path': rp.get('path', '')}
        elif 'AlterTopic' in event_type:
            # Canonical has no partition_write_speed_bytes_per_second, retention_storage_mb
            allowed = {'attributes', 'consumers', 'metering_mode', 'path', 'retention_period'}
            event['request_parameters'] = {k: rp[k] for k in allowed if k in rp}

        # Trim request_parameters to match canonical structure per event type.
        # CreateTopic: full set; AlterTopic: no partition_write_speed_bytes_per_second,
        # retention_storage_mb; DeleteTopic: only path.
        req_params = event.get('request_parameters') or {}
        event_type = meta.get('event_type', '')
        if 'DeleteTopic' in event_type:
            event['request_parameters'] = {k: v for k, v in req_params.items() if k == 'path'}
        elif 'AlterTopic' in event_type:
            for key in ('partition_write_speed_bytes_per_second', 'retention_storage_mb'):
                req_params.pop(key, None)
            event['request_parameters'] = req_params

        # DeleteTopic canonical: details only has path (protobuf may include extra fields)
        if 'DeleteTopic' in event_type:
            details = event.get('details') or {}
            event['details'] = {'path': details.get('path', '')}

        # authorization.permissions[].resource_id (contains database path + topic)
        auth = event.get('authorization') or {}
        for perm in auth.get('permissions') or []:
            if isinstance(perm, dict):
                rid = perm.get('resource_id')
                if rid and isinstance(rid, str) and self.database_path and rid.startswith(self.database_path):
                    topic_name = rid[len(self.database_path):].lstrip('/')
                    perm['resource_id'] = f'<database_path>/{topic_name}'

        # request_metadata: ensure full canonical structure
        req_meta = event.get('request_metadata') or {}
        req_meta['idempotency_id'] = req_meta.get('idempotency_id', '')
        req_meta['remote_address'] = '<canonized_remote_address>'
        req_meta['request_id'] = req_meta.get('request_id', '')
        req_meta['user_agent'] = req_meta.get('user_agent', '')
        if req_meta.get('user_agent'):
            req_meta['user_agent'] = '<canonized_user_agent>'
        event['request_metadata'] = req_meta

        # DeleteTopic canonical: details only has path (protobuf may include extra default fields)
        if 'DeleteTopic' in event_type:
            event['details'] = {'path': (event.get('details') or {}).get('path', '')}

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
            while time.time() - last_read_time <= NO_RECORDS_TIMEOUT:
                time.sleep(0.1)
                f.seek(self.saved_pos)
                chunk = f.read()  # read() not readline(): binary protobuf may contain 0x0a
                if len(chunk) == 0:
                    continue
                last_read_time = time.time()
                messages = _parse_length_delimited_messages(chunk)
                if messages:
                    for blob in messages:
                        event = _parse_protobuf_event(blob)
                        if event:
                            event = self._canonize_event(event)
                            self.captured += json.dumps(event, sort_keys=True) + '\n'
                            self.read_lines += 1
                    break  # got data, no need to re-read same chunk
                else:
                    # Fallback: audit log (JSON with cloud_event_json) or legacy JSON
                    try:
                        canonized = self._process_line(chunk.decode('utf-8'))
                        if canonized:
                            self.captured += canonized
                            self.read_lines += 1
                    except UnicodeDecodeError:
                        pass
                    break  # consumed chunk, avoid infinite loop on re-read

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
