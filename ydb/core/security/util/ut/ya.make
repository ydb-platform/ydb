UNITTEST_FOR(ydb/core/security/util)

REQUIREMENTS(cpu:1)
SRCS(
    counters_ut.cpp
    jwk_ut.cpp
    net_ut.cpp
)

PEERDIR(
    library/cpp/json
)

END()
