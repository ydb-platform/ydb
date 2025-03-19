LIBRARY()

SRCS(
    callback_context.h
    executor_impl.h
    executor_impl.cpp
    log_lazy.h
    retry_policy.cpp
    trace_lazy.h
)

PEERDIR(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic

    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/types
    

    library/cpp/monlib/dynamic_counters
)

END()
