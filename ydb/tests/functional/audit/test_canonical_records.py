# -*- coding: utf-8 -*-
import json
import os
import re
import time

import yatest

from ydb.tests.library.harness.util import LogLevels


def test_file_with_content(human_readable_name, content):
    file_path = os.path.join(yatest.common.output_path(), human_readable_name)

    if os.path.exists(file_path):
        name, ext = os.path.splitext(human_readable_name)
        name, num = os.path.splitext(name)
        if num:
            num = str(int(num[1:]) + 1)  # '.1' -> '2'
        else:
            num = '1'
        return test_file_with_content(f'{name}.{num}{ext}', content)

    with open(file_path, 'w') as w:
        w.write(content)
    return file_path


TOKEN='root@builtin'

AUTH_CONFIG=f'staff_api_user_token: {TOKEN}'

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
                'enable_logging': True,
                'log_phase': ['received', 'completed'],
            }
        ]
    },
    enforce_user_token_requirement=True,
    default_clusteradmin=TOKEN,
    auth_config_path=test_file_with_content('auth_config.yaml', AUTH_CONFIG),
    # extra_feature_flags=['enable_grpc_audit'],
)


class CanonicalCaptureAuditFileOutput:
    def __init__(self, filename):
        self.filename = filename
        self.captured = ''
        self.read_lines = 0

    def __enter__(self):
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __canonize_field(self, json_record, field_name, placeholder_value=None):
        value = json_record.get(field_name)
        if value and value != '{none}':
            json_record[field_name] = placeholder_value if placeholder_value else f'<canonized_{field_name}>'

    def __canonize_regex(self, input_str):
        # Replace txid=123 or txid=any_number with txid=<canonized_tx_id>
        return re.sub(r'txid=\d+', 'txid=<canonized_tx_id>', input_str)

    def __canonize_regex(self, json_record, regex_str, replace_str):
        for k, v in json_record.items():
            replace_result = re.sub(regex_str, replace_str, v)
            if replace_result != v:
                json_record[k] = replace_result

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
        self.__canonize_regex(json_record, r'txid=\d+', 'txid=<canonized_txid>')
        self.__canonize_regex(json_record, r'cmstid=\d+', 'txid=<canonized_cmstid>')
        self.__canonize_regex(json_record, r'\.cpp:\d+', '.cpp:<canonized_line>')
        self.__canonize_regex(json_record, r'OwnerId: \d+', 'OwnerId: <canonized_owner_id>')
        self.__canonize_regex(json_record, r'LocalPathId: \d+', 'LocalPathId: <canonized_local_path_id>')
        self.__canonize_regex(json_record, r'Host: \"[^\"]+\"', 'Host: \"<canonized_host_name>\"')
        return json.dumps(json_record, sort_keys=True) + '\n'

    def __exit__(self, *exc):
        timeout = 2
        last_read_time = time.time()
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            while time.time() - last_read_time <= timeout:
                # unreliable way to get all due audit records into the file
                time.sleep(0.1)
                line = f.readline()
                if len(line) > 0:
                    self.read_lines += 1
                    self.captured += self.__canonize_audit_line(line.decode('utf-8'))
                    last_read_time = time.time()

    def canonize(self):
        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=test_file_with_content('audit_log.json', self.captured)
        )


def test_create_and_drop_database(ydb_cluster):
    capture_audit_create = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    capture_audit_drop = CanonicalCaptureAuditFileOutput(ydb_cluster.config.audit_file_path)
    with capture_audit_create:
        database = '/Root/Database'
        ydb_cluster.create_database(
            database,
            storage_pool_units_count={'hdd': 1},
            token=TOKEN
        )
        database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database, token=TOKEN)

    with capture_audit_drop:
        ydb_cluster.remove_database(database, token=TOKEN)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
    return (capture_audit_create.canonize(), capture_audit_drop.canonize())
