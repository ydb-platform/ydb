# -*- coding: utf-8 -*-
#
import yatest
import pytest
from ydb.tests.library.common import yatest_common

import json
import logging
import os
import re


logger = logging.getLogger(__name__)


def bin_from_env(name):
    if os.getenv(name):
        return yatest_common.binary_path(os.getenv(name))
    raise RuntimeError(f'{name} enviroment variable is not specified')


def dump_bin():
    return bin_from_env("DUMP_BINARY")


def dump_ds_init_bin():
    return bin_from_env("DUMP_DS_INIT_BINARY")


def json_diff_bin():
    return bin_from_env("JSON_DIFF_BINARY")


class TestYamlConfigTransformations(object):
    @classmethod
    def execute(cls, binary, stdin=None, args=[]):
        try:
            execution = yatest_common.execute(
                [binary] + args,
                stdin=stdin,
            )

            return True, execution.std_out.decode('utf-8')
        except yatest.common.process.ExecutionError as ex:
            return False, ex.execution_result.std_err.decode('utf-8')

    @staticmethod
    def canonical_result(output_result, filename, out_path):
        result_filename = filename + ".result.json"
        result_path = os.path.join(out_path, result_filename)
        with open(result_path, "w") as f:
            f.write(output_result)
        return yatest_common.canonical_file(str(result_path), diff_tool=json_diff_bin(), local=True, universal_lines=True)

    def cleanup_errors(self, errors):
        return re.sub(r'address -> 0x[a-zA-Z0-9]+', 'address -> REDACTED', errors)

    def execute_test(self, data, binary, args=[]):
        results = {}
        configs = yatest.common.source_path(data)
        with os.scandir(configs) as it:
            for entry in it:
                if entry.name.endswith(".yaml") and entry.is_file():
                    with open(entry, "r") as f:
                        success, result = self.execute(stdin=f, binary=binary, args=args)
                        if not success:
                            result = json.dumps({"error": True, "stderr": self.cleanup_errors(result)})
                        results[entry.name] = self.canonical_result(result, entry.name, yatest_common.output_path())
        return [results[key] for key in sorted(results.keys(), reverse=True)]

    @pytest.mark.parametrize('binary', [('dump', dump_bin()), ('dump_ds_init', dump_ds_init_bin())], ids=lambda binary: binary[0])
    @pytest.mark.parametrize('args', [[], ["--deprecated"]])
    def test_basic(self, binary, args):
        return self.execute_test("ydb/library/yaml_config/ut_transform/configs", binary[1], args)

    @pytest.mark.parametrize('binary', [('dump', dump_bin()), ('dump_ds_init', dump_ds_init_bin())], ids=lambda binary: binary[0])
    def test_simplified(self, binary):
        return self.execute_test("ydb/library/yaml_config/ut_transform/simplified_configs", binary[1])
