# -*- coding: utf-8 -*-
import json
import os
import re
import time

import yatest

from ydb.tests.library.common.yatest_common import plain_or_under_sanitizer


NO_RECORDS_TIMEOUT = plain_or_under_sanitizer(2, 30)


def cluster_endpoint(cluster):
    return f'{cluster.nodes[1].host}:{cluster.nodes[1].grpc_port}'


def cluster_http_endpoint(cluster):
    return f'{cluster.nodes[1].host}:{cluster.nodes[1].mon_port}'


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


def execute_ydbd(cluster, token, cmd, check_exit_code=True):
    ydbd_binary_path = cluster.nodes[1].binary_path
    full_cmd = [ydbd_binary_path, '-s', f'grpc://{cluster_endpoint(cluster)}']
    full_cmd += cmd

    proc_result = yatest.common.process.execute(full_cmd, check_exit_code=False, env={'YDB_TOKEN': token})
    if check_exit_code and proc_result.exit_code != 0:
        assert False, f'Command\n{full_cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err.decode("utf-8")}\n\nstdout:\n{proc_result.std_out.decode("utf-8")}'


def execute_dstool_grpc(cluster, token, cmd, check_exit_code=True):
    dstool_binary_path = yatest.common.build_path('ydb/apps/dstool/ydb-dstool')
    full_cmd = [dstool_binary_path, '--endpoint', f'grpc://{cluster_endpoint(cluster)}', '--grpc-port', str(cluster.nodes[1].grpc_port)]
    full_cmd += cmd

    proc_result = yatest.common.process.execute(full_cmd, check_exit_code=False, env={'YDB_TOKEN': token})
    if check_exit_code and proc_result.exit_code != 0:
        assert False, f'Command\n{full_cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err.decode("utf-8")}\n\nstdout:\n{proc_result.std_out.decode("utf-8")}'
    return proc_result.std_out


def execute_dstool_http(cluster, token, cmd, check_exit_code=True):
    dstool_binary_path = yatest.common.build_path('ydb/apps/dstool/ydb-dstool')
    full_cmd = [dstool_binary_path, '--endpoint', f'http://{cluster_http_endpoint(cluster)}', '--mon-port', str(cluster.nodes[1].mon_port), '--http']
    full_cmd += cmd

    proc_result = yatest.common.process.execute(full_cmd, check_exit_code=False, env={'YDB_TOKEN': token})
    if check_exit_code and proc_result.exit_code != 0:
        assert False, f'Command\n{full_cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err.decode("utf-8")}\n\nstdout:\n{proc_result.std_out.decode("utf-8")}'
    return proc_result.std_out


class CaptureFileOutput:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __exit__(self, *exc):
        # unreliable way to get all due audit records into the file
        time.sleep(0.1)
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            self.captured = f.read().decode('utf-8')


class CanonicalCaptureAuditFileOutput:
    def __init__(self, filename, components=[]):
        self.filename = filename
        self.captured = ''
        self.read_lines = 0
        self.components = components

    def __enter__(self):
        size = os.path.getsize(self.filename)
        last_read_time = time.time()
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(size)
            # Wait until we stop getting new asyncronous messages from log
            while time.time() - last_read_time <= NO_RECORDS_TIMEOUT:
                time.sleep(0.1)
                line = f.readline()
                if len(line) > 0:
                    last_read_time = time.time()

        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __canonize_field(self, json_record, field_name, placeholder_value=None):
        value = json_record.get(field_name)
        if value and value != '{none}':
            json_record[field_name] = placeholder_value if placeholder_value else f'<canonized_{field_name}>'

    def __canonize_apply_regex(self, json_record, regex_str, replace_str):
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
        if len(self.components) > 0:
            component = json_record.get('component')
            if component not in self.components:
                return None
        self.__validate_audit_record(json_record)
        self.__canonize_field(json_record, 'start_time')
        self.__canonize_field(json_record, 'end_time')
        self.__canonize_field(json_record, 'remote_address')
        self.__canonize_field(json_record, 'tx_id')
        self.__canonize_apply_regex(json_record, r'txid=\d+', 'txid=<canonized_txid>')
        self.__canonize_apply_regex(json_record, r'cmstid=\d+', 'txid=<canonized_cmstid>')
        self.__canonize_apply_regex(json_record, r'OwnerId: \d+', 'OwnerId: <canonized_owner_id>')
        self.__canonize_apply_regex(json_record, r'LocalPathId: \d+', 'LocalPathId: <canonized_local_path_id>')
        self.__canonize_apply_regex(json_record, r'Host: \"[^\"]+\"', 'Host: \"<canonized_host_name>\"')
        self.__canonize_apply_regex(json_record, r'RestartTabletID=\d+', 'RestartTabletID=<canonized_tablet_id>')
        # source_location is used only in debug builds like relwithdebinfo and debug
        self.__canonize_apply_regex(json_record, r', source_location: [A-Za-z0-9_/.]+\.(cpp|h):\d+', '')
        return json.dumps(json_record, sort_keys=True) + '\n'

    def __validate_field(self, json_record, field_name):
        value = json_record.get(field_name)
        assert value is not None, f'Field "{field_name}" is expected to be in audit log. Line: {json.dumps(json_record, sort_keys=True)}'
        assert isinstance(value, str), f'Field "{field_name}" is expected to be string. Line: {json.dumps(json_record, sort_keys=True)}'
        assert value, f'Field "{field_name}" is empty. Line: {json.dumps(json_record, sort_keys=True)}'
        return value

    def __validate_field_exists_and_not_empty(self, json_record, field_name):
        value = self.__validate_field(json_record, field_name)
        assert value != '{none}', f'Field "{field_name}" is expected to have nontrivial value. Line: {json.dumps(json_record, sort_keys=True)}'

    def __validate_field_has_value(self, json_record, field_name, values):
        value = self.__validate_field(json_record, field_name)
        assert value in values, f'Field "{field_name}" is expected to have one of the following values: {values}, but has value "{value}". Line: {json.dumps(json_record, sort_keys=True)}'

    def __validate_audit_record(self, json_record):
        self.__validate_field_exists_and_not_empty(json_record, 'component')
        self.__validate_field_exists_and_not_empty(json_record, 'operation')
        self.__validate_field_exists_and_not_empty(json_record, 'subject')
        self.__validate_field_has_value(json_record, 'status', ['SUCCESS', 'ERROR', 'IN-PROCESS'])
        if json_record.get('subject') != 'metadata@system':
            self.__validate_field_exists_and_not_empty(json_record, 'sanitized_token')
            # We don't have remote_address field in console requests
            # self.__validate_field_exists_and_not_empty(json_record, 'remote_address')

    def __exit__(self, *exc):
        last_read_time = time.time()
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            while time.time() - last_read_time <= NO_RECORDS_TIMEOUT:
                # Unreliable way to get all due audit records into the file
                time.sleep(0.1)
                line = f.readline()
                if len(line) > 0:
                    canonized_line = self.__canonize_audit_line(line.decode('utf-8'))
                    if canonized_line:
                        self.captured += canonized_line
                        self.read_lines += 1
                    last_read_time = time.time()

    def canonize(self):
        return yatest.common.canonical_file(
            local=True,
            universal_lines=True,
            path=make_test_file_with_content('audit_log.json', self.captured)
        )
