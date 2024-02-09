UNITTEST_FOR(ydb/mvp/ydbc)

SIZE(SMALL)

SRCS(
    mvp_ut.cpp
)

PEERDIR(
    ydb/core/testlib/actors
    ydb/public/sdk/cpp/client/draft
    ydb/public/lib/json_value
)

END()
