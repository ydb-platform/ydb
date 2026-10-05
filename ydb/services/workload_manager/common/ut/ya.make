UNITTEST_FOR(ydb/services/workload_manager/common)

SRCS(
    cpu_quota_manager_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
