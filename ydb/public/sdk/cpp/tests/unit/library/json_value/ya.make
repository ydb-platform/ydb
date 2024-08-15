UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

TIMEOUT(600)

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
    ydb/public/sdk/cpp/src/library/json_value
)

END()
