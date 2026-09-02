UNITTEST_FOR(ydb/core/base)

FORK_SUBTESTS()

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
