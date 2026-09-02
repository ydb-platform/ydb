LIBRARY()

SRCS(
    runtime.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/types/exceptions
    ydb/public/sdk/cpp/src/client/types/executor
)

END()
