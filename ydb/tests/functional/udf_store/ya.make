RECURSE(
    upload_udf
)

PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(YDB_KV_VOLUME_TOOL_PATH="ydb/tests/stress/kv_volume_tool/kv_volume_tool")
ENV(YDB_DICTS_UDF_PATH="yql/essentials/udfs/examples/dicts/libdicts_udf.so")
ENV(YDB_UPLOAD_UDF_PATH="ydb/tests/functional/udf_store/upload_udf/upload_udf")

TEST_SRCS(
    test_udf_store.py
)

SPLIT_FACTOR(10)

DEPENDS(
    ydb/tests/stress/kv_volume_tool
    yql/essentials/udfs/examples/dicts
    ydb/tests/functional/udf_store/upload_udf
)

PEERDIR(
    ydb/tests/functional/udf_store/lib
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()

# WASM UDF examples / SDK (cross-compile with Arcadia/YT-compatible flags):
#   ya make --target-platform=clang20-emscripten-wasm64 --build profile \
#     ydb/tests/functional/udf_store/examples/sdk \
#     ydb/tests/functional/udf_store/examples/helpers \
#     ydb/tests/functional/udf_store/examples/add \
#     ydb/tests/functional/udf_store/examples/md5 \
#     ydb/tests/functional/udf_store/examples/throw \
#     ydb/tests/functional/udf_store/examples/with_helpers
#
# with_helpers needs upload order: library sdk → library helpers → UDF with_helpers
# (manifest required_libraries: ["sdk", "helpers"]).
RECURSE(
    examples
    lib
    upload_udf
)
