LIBRARY()

SRCS(
    counters.h
    executor_impl.h
    executor_impl.cpp
    executor.h
    retry_policy.h
    retry_policy.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_types

    library/cpp/monlib/dynamic_counters
    library/cpp/retry
)

END()
