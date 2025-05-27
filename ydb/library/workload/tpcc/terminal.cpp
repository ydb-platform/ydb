#include "terminal.h"
#include "log.h"
#include "transactions.h"
#include "util.h"
#include "constants.h"

#include <array>

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

namespace NYdb::NTPCC {

namespace {

struct TTerminalTransaction {
    using TTaskFunc = NThreading::TFuture<TStatus> (*)(TTransactionContext&, NQuery::TSession);

    TString Name;
    double Weight;
    TTaskFunc TaskFunc;

    // TPC-C timing parameters
    std::chrono::seconds KeyingTime;  // Time before executing transaction
    std::chrono::seconds ThinkTime;   // Time after executing transaction
};

// the order is as in TTerminalStats::ETransactionType
static std::array<TTerminalTransaction, 5> Transactions = {{
    {"NewOrder", NEW_ORDER_WEIGHT, &GetNewOrderTask, NEW_ORDER_KEYING_TIME, NEW_ORDER_THINK_TIME},
    {"Delivery", DELIVERY_WEIGHT, &GetDeliveryTask, DELIVERY_KEYING_TIME, DELIVERY_THINK_TIME},
    {"OrderStatus", ORDER_STATUS_WEIGHT, &GetOrderStatusTask, ORDER_STATUS_KEYING_TIME, ORDER_STATUS_THINK_TIME},
    {"Payment", PAYMENT_WEIGHT, &GetPaymentTask, PAYMENT_KEYING_TIME, PAYMENT_THINK_TIME},
    {"StockLevel", STOCK_LEVEL_WEIGHT, &GetStockLevelTask, STOCK_LEVEL_KEYING_TIME, STOCK_LEVEL_THINK_TIME}
}};

static size_t ChooseRandomTransactionIndex() {
    double totalWeight = 0.0;
    for (const auto& tx : Transactions) {
        totalWeight += tx.Weight;
    }

    double randomValue = RandomNumber(0, static_cast<size_t>(totalWeight * 100)) / 100.0;
    double cumulativeWeight = 0.0;

    for (size_t i = 0; i < Transactions.size(); ++i) {
        cumulativeWeight += Transactions[i].Weight;
        if (randomValue <= cumulativeWeight) {
            return i;
        }
    }

    return Transactions.size() - 1;
}

} // anonymous namespace

TTerminal::TTerminal(size_t terminalID,
                     size_t warehouseID,
                     size_t warehouseCount,
                     ITaskQueue& taskQueue,
                     TDriver& driver,
                     const TString& path,
                     bool noSleep,
                     std::stop_token stopToken,
                     std::atomic<bool>& stopWarmup,
                     std::shared_ptr<TTerminalStats>& stats,
                     std::shared_ptr<TLog>& log)
    : TaskQueue(taskQueue)
    , Driver(driver)
    , Context(terminalID, warehouseID, warehouseCount, TaskQueue, std::make_shared<NQuery::TQueryClient>(Driver), path, log)
    , NoSleep(noSleep)
    , StopToken(stopToken)
    , StopWarmup(stopWarmup)
    , Stats(stats)
    , Task(Run())
{
}

void TTerminal::Start() {
    TaskQueue.TaskReady(Task.Handle, Context.TerminalID);
}

TTerminalTask TTerminal::Run() {
    auto& Log = Context.Log; // to make LOG_* macros working

    LOG_D("Terminal " << Context.TerminalID << " has started");

    while (!StopToken.stop_requested()) {
        if (!WarmupWasStopped && StopWarmup.load(std::memory_order::relaxed)) {
            Stats->ClearOnce();
            WarmupWasStopped = true;
        }

        size_t txIndex = ChooseRandomTransactionIndex();
        auto& transaction = Transactions[txIndex];

        try {
            if (!NoSleep) {
                LOG_T("Terminal " << Context.TerminalID << " keying time for " << transaction.Name << ": "
                    << transaction.KeyingTime.count() << "s");
                co_await TSuspend(TaskQueue, Context.TerminalID, transaction.KeyingTime);
                if (StopToken.stop_requested()) {
                    break;
                }
            }

            co_await TTaskHasInflight(TaskQueue, Context.TerminalID);
            if (StopToken.stop_requested()) {
                TaskQueue.DecInflight();
                break;
            }

            LOG_T("Terminal " << Context.TerminalID << " starting " << transaction.Name << " transaction");

            size_t execCount = 0;
            auto startTime = std::chrono::steady_clock::now();

            auto future = Context.Client->RetryQuery([this, &transaction, &execCount](TSession session) mutable {
                auto& Log = Context.Log;
                LOG_T("Terminal " << Context.TerminalID << " started RetryQuery for " << transaction.Name);
                ++execCount;
                return transaction.TaskFunc(Context, session);
            });

            auto result = co_await TSuspendWithFuture(future, Context.TaskQueue, Context.TerminalID);
            auto endTime = std::chrono::steady_clock::now();
            auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

            if (result.IsSuccess()) {
                Stats->AddOK(static_cast<TTerminalStats::ETransactionType>(txIndex), latency);
                LOG_T("Terminal " << Context.TerminalID << " " << transaction.Name << " transaction finished in "
                    << execCount << " execution(s): " << result.GetStatus());
            } else {
                Stats->IncFailed(static_cast<TTerminalStats::ETransactionType>(txIndex));
                LOG_E("Terminal " << Context.TerminalID << " " << transaction.Name << " transaction failed in "
                    << execCount << " execution(s): " << result.GetStatus() << ", "
                    << result.GetIssues().ToOneLineString());
            }

            if (!NoSleep) {
                LOG_T("Terminal " << Context.TerminalID << " is going to sleep for "
                    << transaction.ThinkTime.count() << "s (think time)");
                co_await TSuspend(TaskQueue, Context.TerminalID, transaction.ThinkTime);
            }
        } catch (const TUserAbortedException& ex) {
            // it's OK, inc statistics and ignore
            Stats->IncUserAborted(static_cast<TTerminalStats::ETransactionType>(txIndex));
            LOG_T("Terminal " << Context.TerminalID << " " << transaction.Name << " transaction aborted by user");
        } catch (const yexception& ex) {
            TStringStream ss;
            ss << "Terminal " << Context.TerminalID << " got exception while transaction execution: "
                << ex.what();
            const auto* backtrace = ex.BackTrace();
            if (backtrace) {
                ss << ", backtrace: " << ex.BackTrace()->PrintToString();
            }
            LOG_E(ss.Str());
            std::quick_exit(1);
        }

        TaskQueue.DecInflight();
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
