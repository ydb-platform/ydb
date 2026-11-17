UNITTEST_FOR(ydb/core/base)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/testing/unittest
    ydb/core/base
)

SRCS(
    http_database_param_ut.cpp
)

END()
