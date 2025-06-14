UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/login
    ydb/library/testlib/service_mocks/ldap_mock
    yql/essentials/public/udf/service/exception_policy
    ydb/core/testlib/audit_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_login.cpp
)

END()
