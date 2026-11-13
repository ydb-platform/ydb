UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

INCLUDE(${ARCADIA_ROOT}/ydb/core/kqp/ut/federated_query/datastreams/common.inc)

# These scenarios also use S3, Solomon or IAM emulators.
SRCS(
    datastreams_history_replay_ut.cpp
    datastreams_ut.cpp
    streaming_ddl_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/recipe.inc)

END()

RECURSE_FOR_TESTS(
    ydb_only
)
