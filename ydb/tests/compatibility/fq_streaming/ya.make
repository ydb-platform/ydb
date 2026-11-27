PY3TEST()

STYLE_PYTHON()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/recipe.inc)

ENV(YDB_DEFAULT_CLUSTERADMIN=root@builtin)

ENV(YDB_ENABLE_SIGNAL_BACKTRACE=1)

TEST_SRCS(
    test_compatibility.py
)

PY_SRCS(
    conftest.py
)

FORK_SUBTESTS()

SIZE(MEDIUM)

REQUIREMENTS(cpu:1)

REQUIREMENTS(ram:16)

TAG(ya:not_autocheck)

PEERDIR(
    ydb/tests/fq/streaming_common
    ydb/tests/library/compatibility
    ydb/tests/library
    ydb/tests/tools/datastreams_helpers
)

DEPENDS(
    ydb/tests/library/compatibility/binaries
)

END()
