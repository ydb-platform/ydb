# -*- coding: utf-8 -*-
#
import yatest
import pytest

import json
import logging
import os
import re


logger = logging.getLogger(__name__)


def bin_from_env(name):
    if os.getenv(name):
        return yatest.common.binary_path(os.getenv(name))
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
            execution = yatest.common.execute(
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
        return yatest.common.canonical_file(str(result_path), diff_tool=json_diff_bin(), local=True, universal_lines=True)

    def cleanup_errors(self, errors):
        errors = re.sub(r'address -> 0x[a-zA-Z0-9]+', 'address -> REDACTED', errors)
        errors = re.sub(r'uncaught exception 0x[a-fA-F0-9]+', 'uncaught exception REDACTED', errors)
        return errors

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
                        results[entry.name] = self.canonical_result(result, entry.name, yatest.common.output_path())
        return [results[key] for key in sorted(results.keys(), reverse=True)]

    @pytest.mark.parametrize('binary', [('dump', dump_bin()), ('dump_ds_init', dump_ds_init_bin())], ids=lambda binary: binary[0])
    @pytest.mark.parametrize('args', [[], ["--deprecated"]])
    def test_basic(self, binary, args):
        return self.execute_test("ydb/library/yaml_config/ut_transform/configs", binary[1], args)

    @pytest.mark.parametrize('binary', [('dump', dump_bin()), ('dump_ds_init', dump_ds_init_bin())], ids=lambda binary: binary[0])
    def test_simplified(self, binary):
        return self.execute_test("ydb/library/yaml_config/ut_transform/simplified_configs", binary[1])

    @pytest.mark.parametrize('binary', [('dump', dump_bin()), ('dump_ds_init', dump_ds_init_bin())], ids=lambda binary: binary[0])
    def test_domains_config(self, binary):
        return self.execute_test("ydb/library/yaml_config/ut_transform/domains_configs", binary[1])


@pytest.mark.parametrize("directory", ["configs", "simplified_configs"])
def test_block82_geometry(directory):
    filename = yatest.common.source_path("ydb/library/yaml_config/ut_transform/%s/block-8-2.yaml" % directory)
    with open(filename) as config:
        result = yatest.common.execute([dump_bin()], stdin=config)
    proto = json.loads(result.std_out)
    group = proto["BlobStorageConfig"]["ServiceSet"]["Groups"][0]
    assert group["ErasureSpecies"] == 19
    assert len(group["Rings"]) == 1
    domains = group["Rings"][0]["FailDomains"]
    assert len(domains) == 12
    assert len({domain["VDiskLocations"][0]["NodeID"] for domain in domains}) == 12


@pytest.mark.parametrize("top_level_pools", [False, True])
def test_block82_compose_config(top_level_pools):
    filename = yatest.common.source_path("ydb/deploy/local/block-8-2/config.yaml")
    with open(filename) as stream:
        config = json.load(stream)["config"]
    if top_level_pools:
        config["storage_pool_types"] = config.pop("domains_config")["domain"][0]["storage_pool_types"]
    input_path = yatest.common.output_path("compose-input.json")
    with open(input_path, "w") as stream:
        json.dump(config, stream)
    with open(input_path) as stream:
        result = yatest.common.execute([dump_bin()], stdin=stream)
    proto = json.loads(result.std_out)
    group = proto["BlobStorageConfig"]["ServiceSet"]["Groups"][0]
    assert group["GroupID"] == 0 and group["ErasureSpecies"] == 19
    assert len(group["Rings"]) == 1
    assert {d["VDiskLocations"][0]["NodeID"] for d in group["Rings"][0]["FailDomains"]} == set(range(1, 13))
    assert len(proto["NameserviceConfig"]["Node"]) == 13
    domain = proto["DomainsConfig"]["Domain"][0]
    assert {p["Kind"]: p["PoolConfig"]["ErasureSpecies"] for p in domain["StoragePoolTypes"]} == {
        "ssd-block42": "block-4-2", "ssd-block82": "block-8-2",
    }
