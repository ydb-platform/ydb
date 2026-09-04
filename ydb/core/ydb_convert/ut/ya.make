UNITTEST_FOR(ydb/core/ydb_convert)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    compression_ut.cpp
    table_description_ut.cpp
    ydb_convert_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    library/cpp/testing/unittest
    ydb/core/testlib/pg
)

END()
