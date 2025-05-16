#pragma once

#include "task_queue.h"

#include "transactions.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <atomic>
#include <stop_token>
#include <memory>

class TLog;

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

struct TTerminalStats {
    std::atomic<size_t> NewOrdersDone = 0;
};

//-----------------------------------------------------------------------------

class alignas(64) TTerminal {
public:
    TTerminal(
        size_t terminalID,
        ITaskQueue& taskQueue,
        TDriver& driver,
        std::stop_token stopToken,
        std::atomic<bool>& stopWarmup,
        std::shared_ptr<TLog>& log);

    TTerminal(TTerminal&&) = default;
    TTerminal& operator=(TTerminal&&) = default;

    TTerminalTask Run();

    bool IsDone() const;

private:
    ITaskQueue& TaskQueue;
    TDriver Driver;
    TTransactionContext Context;
    std::stop_token StopToken;
    std::atomic<bool>& StopWarmup;

    TTerminalTask Task;

    // TODO
    //TTerminalStats Stats;
};

} // namespace NYdb::NTPCC
