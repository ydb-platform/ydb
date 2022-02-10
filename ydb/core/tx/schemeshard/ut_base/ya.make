UNITTEST_FOR(ydb/core/tx/schemeshard)

OWNER(
    vvvv
    g:kikimr
)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SPLIT_FACTOR(40)
ENDIF()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()
 
SRCS(
    ut_base.cpp
    ut_info_types.cpp
)

END()
