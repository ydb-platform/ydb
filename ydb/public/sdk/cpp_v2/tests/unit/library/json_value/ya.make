UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

TIMEOUT(600)

SIZE(MEDIUM)

FORK_SUBTESTS()

SRCS(
    ydb_json_value_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    ydb/public/sdk/cpp_v2/src/client/proto
    ydb/public/sdk/cpp_v2/src/client/params
    ydb/public/sdk/cpp_v2/src/library/json_value
)

END()
