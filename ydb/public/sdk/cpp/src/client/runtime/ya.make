LIBRARY()

SRCS(
    runtime.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/threading/task_scheduler
    ydb/public/sdk/cpp/src/client/types/exceptions
    ydb/public/sdk/cpp/src/client/types/executor
)

END()
