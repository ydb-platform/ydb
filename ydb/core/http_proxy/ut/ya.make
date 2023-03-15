UNITTEST_FOR(ydb/core/http_proxy)

SIZE(SMALL)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    ydb/core/http_proxy
    ydb/public/sdk/cpp/client/ydb_types
)

SRCS(
    json_proto_conversion_ut.cpp
)

END()
