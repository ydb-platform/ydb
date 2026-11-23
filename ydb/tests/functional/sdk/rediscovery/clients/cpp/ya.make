PROGRAM(rediscovery-probe)

SRCS(main.cpp)

PEERDIR(
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
)

END()
