UNITTEST_FOR(ydb/apps/bridge_skipper/http)

SRCS(http_ut.cpp)

PEERDIR(
    library/cpp/json
    library/cpp/monlib/encode/prometheus
    ydb/library/actors/testlib
)

END()
