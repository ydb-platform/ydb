UNITTEST_FOR(ydb/public/lib/json_value)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

SRCS(
    ydb_json_value_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_params
)

END()
