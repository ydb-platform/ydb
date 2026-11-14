UNITTEST()

SRCS(
    read_session_lifecycle_ut.cpp
    read_session_reconnect_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

END()
