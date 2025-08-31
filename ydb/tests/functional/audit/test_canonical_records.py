# -*- coding: utf-8 -*-
import json
import os
import time

import yatest

from ydb.tests.library.harness.util import LogLevels

# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'GRPC_PROXY': LogLevels.DEBUG,
        'GRPC_SERVER': LogLevels.DEBUG,
        'FLAT_TX_SCHEMESHARD': LogLevels.TRACE,
        # less logs
        'KQP_PROXY': LogLevels.DEBUG,
        'KQP_GATEWAY': LogLevels.DEBUG,
        'KQP_WORKER': LogLevels.ERROR,
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.ERROR,
        'KQP_COMPILE_ACTOR': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'HIVE': LogLevels.ERROR,
        'CMS_TENANTS': LogLevels.ERROR,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
        'TX_PROXY_SCHEME_CACHE': LogLevels.CRIT,
        'TX_PROXY': LogLevels.CRIT,
    },
    enable_audit_log=True,
    audit_log_config={
        'file_backend': {
            'format': 'json',
            # File path will be generated automatically
        },
        'log_class_config': [
            {
                'log_class': 'default',
                'enable_logging': 'true',
                'log_phase': ['received', 'completed'],
            }
        ]
    },
    # extra_feature_flags=['enable_grpc_audit'],
)


class CanonicalCaptureAuditFileOutput:
    def __init__(self, filename, expected_lines):
        self.filename = filename
        self.captured = ''
        self.expected_lines = expected_lines
        self.read_lines = 0

    def __enter__(self):
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __canonize_field(self, json_record, field_name, placeholder_value=None):
        value = json_record.get(field_name)
        if value and value != '{none}':
            json_record[field_name] = placeholder_value if placeholder_value else f'<{field_name}>'

    def __canonize_audit_line(self, output):
        # Audit log has the following format: "<time>: {"k1": "v1", "k2": "v2", ...}"
        # where <time> is ISO 8601 format time string, k1, k2, ..., kn - fields of audit log message
        # and v1, v2, ..., vn are their values
        record_start = output.find('{')
        if record_start == -1:  # error, wrong format
            return output

        json_record = json.loads(output[record_start:])
        self.__canonize_field(json_record, 'start_time')
        self.__canonize_field(json_record, 'end_time')
        self.__canonize_field(json_record, 'remote_address')
        self.__canonize_field(json_record, 'tx_id')
        return json.dumps(json_record, sort_keys=True) + '\n'

    def __captured_all(self):
        self.captured.count('\n') >= self.expected_lines

    def __exit__(self, *exc):
        start_time = time.time()
        timeout = 10
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            while time.time() - start_time <= timeout and not self.__captured_all():
                # unreliable way to get all due audit records into the file
                time.sleep(0.1)
                line = f.readline()
                if len(line) > 0:
                    self.read_lines += 1
                    self.captured += self.__canonize_audit_line(line.decode('utf-8'))

    def canonize(self):
        file_path = os.path.join(yatest.common.output_path(), 'audit_log.json')
        with open(file_path, 'w') as w:
            w.write(self.captured)

        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=file_path
        )


def test_create_and_drop_database(ydb_cluster):
    capture_audit = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path, expected_lines=11)
    with capture_audit:
        database = '/Root/users/database'
        ydb_cluster.create_database(
            database,
            storage_pool_units_count={'hdd': 1}
        )
        database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
        ydb_cluster.wait_tenant_up(database)

        ydb_cluster.remove_database(database)
        ydb_cluster.unregister_and_stop_slots(database_nodes)
    return capture_audit.canonize()
