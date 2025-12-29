#pragma once

#include "runner.h"
#include "task_queue.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/threading/future/core/coroutine_traits.h>

#include <memory>

class TLog;

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

extern std::atomic<size_t> TransactionsInflight;

struct TTransactionInflightGuard {
    TTransactionInflightGuard() {
        TransactionsInflight.fetch_add(1, std::memory_order_relaxed);
    }

    ~TTransactionInflightGuard() {
        TransactionsInflight.fetch_sub(1, std::memory_order_relaxed);
    }
};

//-----------------------------------------------------------------------------

struct TTransactionContext {
    size_t TerminalID; // unrelated to TPC-C, part of implementation
    size_t WarehouseID;
    size_t WarehouseCount;
    ITaskQueue& TaskQueue;
    int SimulateTransactionMs;
    int SimulateTransactionSelect1;
    std::shared_ptr<NQuery::TQueryClient> Client;
    const TString Path;
    std::shared_ptr<TLog> Log;
    TRunConfig::ETxMode TxMode;
};

struct TUserAbortedException : public yexception {
};

//-----------------------------------------------------------------------------

inline NQuery::TTxSettings GetTxSettings(TRunConfig::ETxMode mode) {
    switch (mode) {
        case TRunConfig::ETxMode::SerializableRW:
            return NQuery::TTxSettings::SerializableRW();
        case TRunConfig::ETxMode::SnapshotRW:
            return NQuery::TTxSettings::SnapshotRW();
    }
}

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetNewOrderTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetDeliveryTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetOrderStatusTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetPaymentTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetStockLevelTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetSimulationTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session);

} // namespace NYdb::NTPCC
