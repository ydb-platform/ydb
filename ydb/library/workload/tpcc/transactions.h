#pragma once

#include "task_queue.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/threading/future/core/coroutine_traits.h>

#include <memory>

class TLog;

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

struct TTransactionContext {
    size_t TerminalID; // unrelated to TPC-C, part of implementation
    size_t WarehouseID;
    size_t WarehouseCount;
    ITaskQueue& TaskQueue;
    std::shared_ptr<NQuery::TQueryClient> Client;
    const TString Path;
    std::shared_ptr<TLog> Log;
};

struct TUserAbortedException : public yexception {
};

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetNewOrderTask(
    TTransactionContext& context,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetDeliveryTask(
    TTransactionContext& context,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetOrderStatusTask(
    TTransactionContext& context,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetPaymentTask(
    TTransactionContext& context,
    NQuery::TSession session);

NThreading::TFuture<TStatus> GetStockLevelTask(
    TTransactionContext& context,
    NQuery::TSession session);

} // namespace NYdb::NTPCC
