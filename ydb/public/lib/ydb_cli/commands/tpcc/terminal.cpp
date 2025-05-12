#include "terminal.h"

#include "log.h"

#include <thread> // XXX

// TODO: can we elliminate this?
namespace std {
    template<>
    struct coroutine_traits<NYdb::NTPCC::TTerminalTask, NYdb::NTPCC::TTerminal&> {
        using promise_type = NYdb::NTPCC::TTerminalTask::TPromiseType;
    };
}

namespace NYdb::NTPCC {

TTerminal::TTerminal(size_t terminalID,
                     IReadyTaskQueue& taskQueue,
                     TDriver& driver,
                     std::stop_token stopToken,
                     std::atomic<bool>& stopWarmup,
                     std::shared_ptr<TLog>& log)
    : TaskQueue(taskQueue)
    , Driver(driver)
    , Context(terminalID, std::make_shared<NQuery::TQueryClient>(Driver), log)
    , StopToken(stopToken)
    , StopWarmup(stopWarmup)
    , Task(Run())
{
    TaskQueue.TerminalReady(Task.Handle, Context.TerminalID);
}

TTerminalTask TTerminal::Run() {
    auto& Log = Context.Log; // to make LOG_* macros working

    LOG_D("Terminal " << Context.TerminalID << " has started");

    Y_UNUSED(StopWarmup);

    while (!StopToken.stop_requested()) {
        auto result = co_await GetNewOrderTask(Context);
        (void) result;

        std::this_thread::sleep_for(std::chrono::milliseconds(200)); // XXX
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

} // namesapce NYdb::NTPCC
