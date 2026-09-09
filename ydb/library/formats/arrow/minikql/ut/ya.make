UNITTEST_FOR(ydb/library/formats/arrow/minikql)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ut_helpers.cpp
    minikql_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/parser/pg_wrapper
)

END()
