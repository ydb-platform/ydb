LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    callback_context.h
    executor_impl.h
    executor_impl.cpp
    log_lazy.h
    retry_policy.cpp
    trace_lazy.h
)

PEERDIR(
    ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/topic

    ydb/public/sdk/cpp_v2/src/client/common_client/impl
    ydb/public/sdk/cpp_v2/src/client/types

    library/cpp/monlib/dynamic_counters
    library/cpp/retry
)

END()
