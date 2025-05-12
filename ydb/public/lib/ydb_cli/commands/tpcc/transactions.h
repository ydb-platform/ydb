#pragma once

#include "task_queue.h"

#include <memory>

class TLog;

namespace NYdb::NTPCC {

struct TTransactionContext {
    size_t TerminalID;
    std::shared_ptr<NYdb::NQuery::TQueryClient> Client;
    std::shared_ptr<TLog> Log;
};

TTransactionTask GetNewOrderTask(TTransactionContext& context);

} // namesapce NYdb::NTPCC

// TODO: can we elliminate this?
namespace std {
    template<>
    struct coroutine_traits<NYdb::NTPCC::TTransactionTask, NYdb::NTPCC::TTransactionContext&> {
        using promise_type = NYdb::NTPCC::TTransactionTask::TPromiseType;
    };
}
