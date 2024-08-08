UNITTEST_FOR(ydb/library/yql/dq/actors/spilling)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
TIMEOUT(180)

SRCS(
    spilling_file_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/actors/testlib
    ydb/library/services
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/exception_policy
)

END()
