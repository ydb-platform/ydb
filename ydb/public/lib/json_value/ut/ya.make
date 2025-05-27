UNITTEST_FOR(ydb/public/lib/json_value)

SIZE(MEDIUM)

FORK_SUBTESTS()

SRCS(
    ydb_json_value_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/params
)

END()
