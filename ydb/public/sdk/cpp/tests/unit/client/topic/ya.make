UNITTEST()

SIZE(MEDIUM)
FORK_SUBTESTS()

SRCS(
    write_session_credentials_ut.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/persqueue_public
)

END()
