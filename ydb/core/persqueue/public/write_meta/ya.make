LIBRARY()

SRCS(
    write_meta.cpp
)

PEERDIR(
    ydb/core/persqueue/public
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

END()

