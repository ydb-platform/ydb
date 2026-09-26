UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/pg
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_system_names.cpp
    # ../schemeshard_system_names.cpp
    # ../schemeshard_system_names.h
)

END()
