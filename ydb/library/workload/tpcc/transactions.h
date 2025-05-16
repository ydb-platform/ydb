#pragma once

#include "task_queue.h"

#include <memory>

class TLog;

namespace NYdb::NTPCC {

struct TTransactionContext {
    size_t TerminalID;
    ITaskQueue& TaskQueue;
    std::shared_ptr<NYdb::NQuery::TQueryClient> Client;
    std::shared_ptr<TLog> Log;
};

TTransactionTask GetNewOrderTask(TTransactionContext& context);

} // namespace NYdb::NTPCC
