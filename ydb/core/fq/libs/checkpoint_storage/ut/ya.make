UNITTEST_FOR(ydb/core/fq/libs/checkpoint_storage)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/retry
    library/cpp/testing/unittest
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/checkpoint_storage/events
    ydb/core/testlib/default
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_table
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

SRCS(
    gc_ut.cpp
    storage_service_ydb_ut.cpp
    ydb_state_storage_ut.cpp
    ydb_checkpoint_storage_ut.cpp
)

END()
