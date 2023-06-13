UNITTEST_FOR(ydb/core/kqp/runtime)

FORK_SUBTESTS()

SIZE(MEDIUM)
TIMEOUT(180)

SRCS(
    kqp_spilling_file_ut.cpp
    kqp_scan_data_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics/default
    ydb/library/yql/public/udf/service/exception_policy
)

END()
