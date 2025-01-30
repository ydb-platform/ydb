UNITTEST_FOR(ydb/core/fq/libs/ydb)

FORK_SUBTESTS()

SRCS(
    ydb_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/library/security
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()
