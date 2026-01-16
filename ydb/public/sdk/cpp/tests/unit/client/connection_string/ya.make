GTEST()

SIZE(SMALL)

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/internal/common
)

SRCS(
    connection_string_ut.cpp
)

END()
