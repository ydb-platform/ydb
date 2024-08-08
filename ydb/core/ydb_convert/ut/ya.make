UNITTEST_FOR(ydb/core/ydb_convert)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    compression_ut.cpp
    table_description_ut.cpp
    ydb_convert_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

END()
