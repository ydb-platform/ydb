UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

# Runs every op in this suite through the scheme change outbox and fails the
# test if any of them cannot resolve a target path.
ENV(YDB_SCHEME_CHANGE_CORPUS=1)

SPLIT_FACTOR(2)

IF (SANITIZER_TYPE == "thread")
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
    ydb/core/tx
    ydb/core/tx/columnshard
    ydb/core/tx/datashard
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_sequence.cpp
)

END()
