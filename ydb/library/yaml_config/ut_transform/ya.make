PY3TEST()

TEST_SRCS(
    test_transform.py
)

ENV(DUMP_BINARY="ydb/library/yaml_config/tools/dump/yaml-to-proto-dump")
ENV(DUMP_DS_INIT_BINARY="ydb/library/yaml_config/tools/dump_ds_init/yaml-to-proto-dump-ds-init")
ENV(JSON_DIFF_BINARY="ydb/library/yaml_config/tools/simple_json_diff/simple_json_diff")

DEPENDS(
    ydb/library/yaml_config/tools/dump
    ydb/library/yaml_config/tools/dump_ds_init
    ydb/library/yaml_config/tools/simple_json_diff
)

DATA(
    arcadia/ydb/library/yaml_config/ut_transform/configs
    arcadia/ydb/library/yaml_config/ut_transform/simplified_configs
)

PEERDIR(
    contrib/python/pytest
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/library
    ydb/tests/oss/canonical
)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

END()
