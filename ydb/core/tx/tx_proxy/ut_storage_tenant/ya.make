UNITTEST_FOR(ydb/core/tx/tx_proxy)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/testing/unittest
    ydb/core/blobstorage
    ydb/core/testlib/default
    ydb/core/tx
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    storage_tenant_ut.cpp
    proxy_ut_helpers.h
    proxy_ut_helpers.cpp
)

REQUIREMENTS(ram:32)

END()
