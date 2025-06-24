LIBRARY()

SRCS(
    describe.cpp
    local_partition.cpp
    managed_executor.cpp
    setup.cpp
    trace.cpp
)

PEERDIR(
    library/cpp/logger
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
