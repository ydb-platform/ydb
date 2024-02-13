# -*- coding: utf-8 -*-
#
import yatest
from ydb.tests.library.common import yatest_common

import logging
import os


logger = logging.getLogger(__name__)


def dump_bin():
    if os.getenv("DUMP_BINARY"):
        return yatest_common.binary_path(os.getenv("DUMP_BINARY"))
    raise RuntimeError("DUMP_BINARY enviroment variable is not specified")


def dump_ds_init_bin():
    if os.getenv("DUMP_DS_INIT_BINARY"):
        return yatest_common.binary_path(os.getenv("DUMP_DS_INIT_BINARY"))
    raise RuntimeError("DUMP_DS_INIT_BINARY enviroment variable is not specified")


def json_diff_bin():
    if os.getenv("JSON_DIFF_BINARY"):
        return yatest_common.binary_path(os.getenv("JSON_DIFF_BINARY"))
    raise RuntimeError("JSON_DIFF_BINARY enviroment variable is not specified")


class TestYamlConfigTransformations(object):
    @classmethod
    def execute_dump(cls, stdin=None, args=[]):
        execution = yatest_common.execute(
            [dump_bin()] + args,
            stdin=stdin,
        )

        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result

    @classmethod
    def execute_dump_ds_init(cls, stdin=None, args=[]):
        execution = yatest_common.execute(
            [dump_ds_init_bin()] + args,
            stdin=stdin,
        )

        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result

    @staticmethod
    def canonical_result(output_result, filename, out_path, suffix=""):
        result_filename = filename + suffix + ".result.json"
        result_path = os.path.join(out_path, result_filename)
        with open(result_path, "w") as f:
            f.write(output_result.decode('utf-8'))
        return yatest_common.canonical_file(str(result_path), diff_tool=json_diff_bin(), local=True, universal_lines=True)

    def test_basic(self):
        results = []
        configs = yatest.common.source_path("ydb/library/yaml_config/ut_transform/configs")
        with os.scandir(configs) as it:
            for entry in it:
                if entry.name.endswith(".yaml") and entry.is_file():
                    with open(entry, "r") as f:
                        result = self.execute_dump(stdin=f)
                        results.append(self.canonical_result(result, entry.name, yatest_common.output_path()))
        return results

    def test_deprecated(self):
        results = []
        configs = yatest.common.source_path("ydb/library/yaml_config/ut_transform/configs")
        with os.scandir(configs) as it:
            for entry in it:
                if entry.name.endswith(".yaml") and entry.is_file():
                    with open(entry, "r") as f:
                        result = self.execute_dump(stdin=f, args=["--deprecated"])
                        results.append(self.canonical_result(result, entry.name, yatest_common.output_path()))
        return results

    def test_ds_init_basic(self):
        results = []
        configs = yatest.common.source_path("ydb/library/yaml_config/ut_transform/configs")
        with os.scandir(configs) as it:
            for entry in it:
                if entry.name.endswith(".yaml") and entry.is_file():
                    with open(entry, "r") as f:
                        result = self.execute_dump(stdin=f)
                        results.append(self.canonical_result(result, entry.name, yatest_common.output_path(), suffix=".ds_init"))
        return results

    def test_ds_init_deprecated(self):
        results = []
        configs = yatest.common.source_path("ydb/library/yaml_config/ut_transform/configs")
        with os.scandir(configs) as it:
            for entry in it:
                if entry.name.endswith(".yaml") and entry.is_file():
                    with open(entry, "r") as f:
                        result = self.execute_dump(stdin=f, args=["--deprecated"])
                        results.append(self.canonical_result(result, entry.name, yatest_common.output_path(), suffix=".ds_init"))
        return results
