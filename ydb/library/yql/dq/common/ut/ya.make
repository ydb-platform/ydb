UNITTEST_FOR(ydb/library/yql/dq/common)

REQUIREMENTS(cpu:1)
SRCS(
    dq_resource_quoter_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
