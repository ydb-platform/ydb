UNITTEST_FOR(ydb/core/security/util)

SRCS(
    counters_ut.cpp
    jwk_ut.cpp
    net_ut.cpp
)

PEERDIR(
    library/cpp/json
)

END()
