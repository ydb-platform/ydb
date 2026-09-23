UNITTEST()

SIZE(MEDIUM)
FORK_SUBTESTS()

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

SRCS(
    ../../../../src/client/topic/ut/write_session_size_ut.cpp
    write_size_ut.cpp
)

END()
