UNITTEST_FOR(ydb/public/sdk/cpp/client/draft)

SIZE(SMALL)

PEERDIR(
    ydb/public/sdk/cpp/client/ut/helpers
)

SRCS(
    ydb_scripting_response_headers_ut.cpp
    ydb_view_ut.cpp
)

END()
