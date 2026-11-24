UNITTEST_FOR(ydb/core/keyvalue)

FORK_SUBTESTS()

SPLIT_FACTOR(30)
IF (SANITIZER_TYPE  == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/util/actorsys_test
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(keyvalue_test_enums.h)

SRCS(
    keyvalue_ut.cpp
    keyvalue_collector_ut.cpp
    keyvalue_storage_read_request_ut.cpp
    keyvalue_move_data_ut.cpp
)

GENERATE_ENUM_SERIALIZATION(keyvalue_move_data_test_enums.h)

END()
