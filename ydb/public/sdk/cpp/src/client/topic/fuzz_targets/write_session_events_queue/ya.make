FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
    ydb/public/sdk/cpp/src/client/persqueue_public/impl
    ydb/public/sdk/cpp/src/client/persqueue_public/include
    ydb/public/sdk/cpp/src/client/topic/impl
)

END()
