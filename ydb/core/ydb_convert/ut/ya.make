UNITTEST_FOR(ydb/core/ydb_convert)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    compression_ut.cpp
    dictionary_feature_flag_ut.cpp
    table_description_ut.cpp
    ydb_convert_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/pg
)

END()
