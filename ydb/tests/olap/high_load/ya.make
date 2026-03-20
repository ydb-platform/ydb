UNITTEST_FOR(ydb/services/ydb)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SRCS(
    read_update_write.cpp
)

PEERDIR(
    ydb/core/testlib/pg
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()