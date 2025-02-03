UNITTEST_FOR(ydb/library/yql/dq/actors/spilling)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    spilling_file_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/actors/testlib
    ydb/library/services
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()
