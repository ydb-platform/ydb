LIBRARY()

SRCS(
    describe.cpp
    local_partition.cpp
    managed_executor.cpp
    trace.cpp
    setup.h
)

PEERDIR(
    library/cpp/logger
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
