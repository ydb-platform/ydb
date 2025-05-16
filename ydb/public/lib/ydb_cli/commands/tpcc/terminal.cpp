#include "terminal.h"

#include "log.h"

namespace NYdb::NTPCC {

TTerminal::TTerminal(size_t terminalID,
                     ITaskQueue& taskQueue,
                     TDriver& driver,
                     std::stop_token stopToken,
                     std::atomic<bool>& stopWarmup,
                     std::shared_ptr<TLog>& log)
    : TaskQueue(taskQueue)
    , Driver(driver)
    , Context(terminalID, TaskQueue, std::make_shared<NQuery::TQueryClient>(Driver), log)
    , StopToken(stopToken)
    , StopWarmup(stopWarmup)
    , Task(Run())
{
    TaskQueue.TaskReady(Task.Handle, Context.TerminalID);
}

TTerminalTask TTerminal::Run() {
    auto& Log = Context.Log; // to make LOG_* macros working

    LOG_D("Terminal " << Context.TerminalID << " has started");

    Y_UNUSED(StopWarmup);

    while (!StopToken.stop_requested()) {
        try {
            auto result = co_await GetNewOrderTask(Context);
            (void) result;
        } catch (const std::exception& ex) {
            LOG_E("Terminal " << Context.TerminalID << " got exception while transaction execution: " << ex.what());
        }

        co_await TSuspend(TaskQueue, Context.TerminalID, std::chrono::milliseconds(50));
    }

    LOG_D("Terminal " << Context.TerminalID << " stopped");

    co_return;
}

bool TTerminal::IsDone() const {
    if (!Task.Handle) {
        return true;
    }

    return Task.Handle.done();
}

} // namespace NYdb::NTPCC
