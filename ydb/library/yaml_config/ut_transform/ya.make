PY3TEST()

TEST_SRCS(
    test_transform.py
)

ENV(DUMP_BINARY="ydb/library/yaml_config/tools/dump/yaml-to-proto-dump")
ENV(DUMP_DS_INIT_BINARY="ydb/library/yaml_config/tools/dump/yaml-to-proto-dump-ds-init")

DEPENDS(
    ydb/library/yaml_config/tools/dump
    ydb/library/yaml_config/tools/dump_ds_init
)

DATA(
    arcadia/ydb/library/yaml_config/ut_transform/configs
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/canonical
    ydb/public/sdk/python/enable_v3_new_behavior
)

TIMEOUT(600)

SIZE(MEDIUM)

END()
