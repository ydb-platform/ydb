UNITTEST_FOR(ydb/core/tx/columnshard/backup/iscan)

FORK_SUBTESTS()

PEERDIR(
    ydb/apps/ydbd/export
    ydb/core/testlib/actors
    ydb/core/testlib/default
    ydb/core/wrappers
    ydb/library/testlib/s3_recipe_helper
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SRCS(
    s3_retry_ut.cpp
)

END()
