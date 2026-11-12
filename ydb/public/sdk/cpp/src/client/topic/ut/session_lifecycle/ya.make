UNITTEST()

SRCS(
    direct_read_lifecycle_ut.cpp
    read_session_lifecycle_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

END()
