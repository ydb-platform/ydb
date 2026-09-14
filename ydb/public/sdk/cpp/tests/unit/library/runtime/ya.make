UNITTEST()

FORK_SUBTESTS()

SRCS(
    runtime_ut.cpp
    thread_lifetime_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/runtime
)

END()
