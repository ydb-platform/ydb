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
    NQuery::TTxSettings TxMode;
};

struct TUserAbortedException : public yexception {
};

// Build TTxControl for an in-flight transaction, optionally committing with the query.
inline NQuery::TTxControl TxControl(const NQuery::TTransaction& tx, bool commit = false) {
    auto control = NQuery::TTxControl::Tx(tx);
    if (commit) {
        control.CommitTx(true);
    }
    return control;
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
