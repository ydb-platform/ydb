UNITTEST_FOR(ydb/core/tx/tx_proxy)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/core/tx
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    encrypted_storage_ut.cpp
    proxy_ut_helpers.h
    proxy_ut_helpers.cpp
)

END()
