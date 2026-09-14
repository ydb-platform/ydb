#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/threading/future/future.h>

#include <cstddef>
#include <functional>

namespace NYdb::inline Dev {

class TSdkRuntime final {
public:
    // The first driver configures the shared response executor. Only a different
    // explicitly supplied executor is rejected by subsequent drivers.
    IExecutor::TPtr GetExecutor(
        IExecutor::TPtr executor = {},
        std::size_t threadCount = 0,
        std::size_t maxQueueSize = 0);

    NYdbGrpc::TGRpcClientLow& GetNetwork(
        std::size_t threadCount = NYdbGrpc::DEFAULT_NUM_THREADS);

    // Internal timers preserve explicit operation cancellation and run their
    // callbacks on a network thread.
    void ScheduleCallback(
        TDuration timeout,
        std::function<void(bool)> callback,
        NYdbGrpc::IQueueClientContextPtr context = {});

    void ScheduleCallback(
        TDeadline deadline,
        std::function<void(bool)> callback,
        NYdbGrpc::IQueueClientContextPtr context = {});

    NThreading::TFuture<bool> ScheduleFuture(
        TDuration timeout,
        NYdbGrpc::IQueueClientContextPtr context = {});

private:
    TSdkRuntime() = default;
    TSdkRuntime(const TSdkRuntime&) = delete;
    TSdkRuntime& operator=(const TSdkRuntime&) = delete;

    friend TSdkRuntime& GetSdkRuntime();
};

TSdkRuntime& GetSdkRuntime();

} // namespace NYdb
