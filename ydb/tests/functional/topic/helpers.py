# -*- coding: utf-8 -*-
import json
import os
import subprocess
import time

import yatest

from ydb.tests.library.common.helpers import plain_or_under_sanitizer


NO_RECORDS_TIMEOUT = plain_or_under_sanitizer(2, 30)


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
    Captures topic cloud events from audit log, normalizes variable fields,
    outputs one JSON line per event. Use with yatest.common.canonical_file via canonize().
    """

    def __init__(self, filename, database_path):
        self.filename = filename
        self.database_path = database_path
        self.captured = ''
        self.read_lines = 0

    def __enter__(self):
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

        # request_metadata.remote_address (port varies per run)
        req_meta = event.get('request_metadata') or {}
        if req_meta.get('remote_address'):
            req_meta['remote_address'] = '<canonized_remote_address>'
        if req_meta.get('user_agent'):
            req_meta['user_agent'] = '<canonized_user_agent>'

        return event

    def _process_line(self, line_str):
        if 'cloud_event_json' not in line_str:
            return None
        if not any(t in line_str for t in TOPIC_CLOUD_EVENT_TYPES):
            return None

        json_start = line_str.find('{')
        if json_start == -1:
            return None
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
        event = self._canonize_event(event)
        return json.dumps(event, sort_keys=True) + '\n'

    def __exit__(self, *exc):
        last_read_time = time.time()
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            while time.time() - last_read_time <= NO_RECORDS_TIMEOUT:
                time.sleep(0.1)
                line = f.readline()
                if len(line) > 0:
                    canonized = self._process_line(line.decode('utf-8'))
                    if canonized:
                        self.captured += canonized
                        self.read_lines += 1
                    last_read_time = time.time()

    def canonize(self):
        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=make_test_file_with_content('topic_cloud_events.json', self.captured)
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
