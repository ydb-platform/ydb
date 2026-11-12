UNITTEST()

SRCS(
    read_session_lifecycle_ut.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

END()
