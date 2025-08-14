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
    using TTaskFunc = NThreading::TFuture<TStatus> (*)(TTransactionContext&, TDuration&, NQuery::TSession);

    TString Name;
    double Weight;
    TTaskFunc TaskFunc;

    // TPC-C timing parameters
    std::chrono::seconds KeyingTime;  // Time before executing transaction
    std::chrono::seconds ThinkTime;   // Time after executing transaction
};

static std::array<TTerminalTransaction, GetEnumItemsCount<ETransactionType>()> CreateTransactions() {
    std::array<TTerminalTransaction, GetEnumItemsCount<ETransactionType>()> transactions{};

    transactions[static_cast<size_t>(ETransactionType::NewOrder)] =
        {"NewOrder", NEW_ORDER_WEIGHT, &GetNewOrderTask, NEW_ORDER_KEYING_TIME, NEW_ORDER_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::Delivery)] =
        {"Delivery", DELIVERY_WEIGHT, &GetDeliveryTask, DELIVERY_KEYING_TIME, DELIVERY_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::OrderStatus)] =
        {"OrderStatus", ORDER_STATUS_WEIGHT, &GetOrderStatusTask, ORDER_STATUS_KEYING_TIME, ORDER_STATUS_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::Payment)] =
        {"Payment", PAYMENT_WEIGHT, &GetPaymentTask, PAYMENT_KEYING_TIME, PAYMENT_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::StockLevel)] =
        {"StockLevel", STOCK_LEVEL_WEIGHT, &GetStockLevelTask, STOCK_LEVEL_KEYING_TIME, STOCK_LEVEL_THINK_TIME};

    return transactions;
}

static std::array<TTerminalTransaction, GetEnumItemsCount<ETransactionType>()> Transactions = CreateTransactions();

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
                     std::shared_ptr<NQuery::TQueryClient>& client,
                     const TString& path,
                     bool noDelays,
                     int simulateTransactionMs,
                     int simulateTransactionSelect1Count,
                     std::stop_token stopToken,
                     std::atomic<bool>& stopWarmup,
                     std::shared_ptr<TTerminalStats>& stats,
                     std::shared_ptr<TLog>& log)
    : TaskQueue(taskQueue)
    , Context(terminalID, warehouseID, warehouseCount, TaskQueue, simulateTransactionMs, simulateTransactionSelect1Count, client, path, log)
    , NoDelays(noDelays)
    , StopToken(stopToken)
    , StopWarmup(stopWarmup)
    , Stats(stats)
    , Task(Run())
{
}

void TTerminal::Start() {
    if (!Started) {
        TaskQueue.TaskReadyThreadSafe(Task.Handle, Context.TerminalID);
        Started = true;
    }
}

TTerminalTask TTerminal::Run() {
    auto& Log = Context.Log; // to make LOG_* macros working

    LOG_D("Terminal " << Context.TerminalID << " has started");

    while (!StopToken.stop_requested()) {
        if (!WarmupWasStopped && StopWarmup.load(std::memory_order::relaxed)) {
            // WarmupWasStopped is per terminal member, while Stats are shared
            // between multiple terminals. That's why we call ClearOnce().
            Stats->ClearOnce();
            WarmupWasStopped = true;
        }

        size_t txIndex = ChooseRandomTransactionIndex();
        auto& transaction = Transactions[txIndex];

        try {
            if (!NoDelays) {
                LOG_T("Terminal " << Context.TerminalID << " keying time for " << transaction.Name << ": "
                    << transaction.KeyingTime.count() << "s");
                co_await TSuspend(TaskQueue, Context.TerminalID, transaction.KeyingTime);
                if (StopToken.stop_requested()) {
                    break;
                }
            }

            auto startTime = std::chrono::steady_clock::now();
            co_await TTaskHasInflight(TaskQueue, Context.TerminalID);
            if (StopToken.stop_requested()) {
                TaskQueue.DecInflight();
                break;
            }

            LOG_T("Terminal " << Context.TerminalID << " starting " << transaction.Name << " transaction");

            size_t execCount = 0;
            auto startTimeTransaction = std::chrono::steady_clock::now();
            TDuration latencyPure;

            // the block helps to ensure, that session is destroyed before we sleep right after the block
            {
                bool real = Context.SimulateTransactionMs == 0 && Context.SimulateTransactionSelect1 == 0;
                auto future = Context.Client->RetryQuery(
                    [this, real, &transaction, &execCount, &latencyPure](TSession session) mutable {
                        auto& Log = Context.Log;
                        LOG_T("Terminal " << Context.TerminalID << " started RetryQuery for " << transaction.Name);
                        ++execCount;
                        if (real) {
                            return transaction.TaskFunc(Context, latencyPure, session);
                        } else {
                            return GetSimulationTask(Context, latencyPure, session);
                        }
                    });

                auto result = co_await TSuspendWithFuture(future, Context.TaskQueue, Context.TerminalID);
                auto endTime = std::chrono::steady_clock::now();
                auto latencyFull = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
                auto latencyTransaction = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTimeTransaction);

                if (result.IsSuccess()) {
                    Stats->AddOK(static_cast<ETransactionType>(txIndex), latencyTransaction, latencyFull, latencyPure);
                    LOG_T("Terminal " << Context.TerminalID << " " << transaction.Name << " transaction finished in "
                        << execCount << " execution(s): " << result.GetStatus());
                } else {
                    Stats->IncFailed(static_cast<ETransactionType>(txIndex));
                    LOG_E("Terminal " << Context.TerminalID << " " << transaction.Name << " transaction failed in "
                        << execCount << " execution(s): " << result.GetStatus() << ", "
                        << result.GetIssues().ToOneLineString());
                }
            }
            TaskQueue.DecInflight();
            if (!NoDelays) {
                LOG_T("Terminal " << Context.TerminalID << " is going to sleep for "
                    << transaction.ThinkTime.count() << "s (think time)");
                co_await TSuspend(TaskQueue, Context.TerminalID, transaction.ThinkTime);
            }
            continue;
        } catch (const TUserAbortedException& ex) {
            // it's OK, inc statistics and ignore
            Stats->IncUserAborted(static_cast<ETransactionType>(txIndex));
            LOG_T("Terminal " << Context.TerminalID << " " << transaction.Name << " transaction aborted by user");
        } catch (const yexception& ex) {
            TStringStream ss;
            ss << "Terminal " << Context.TerminalID << " got exception while " << transaction.Name << " execution: "
                << ex.what();
            const auto* backtrace = ex.BackTrace();
            if (backtrace) {
                ss << ", backtrace: " << ex.BackTrace()->PrintToString();
            }
            LOG_E(ss.Str());
            RequestStop();
            co_return;
        } catch (const std::exception& ex) {
            TStringStream ss;
            ss << "Terminal " << Context.TerminalID << " got exception while " << transaction.Name << " execution: "
                << ex.what();
            LOG_E(ss.Str());
            RequestStop();
            co_return;
        }

        // only here if exception caught

        TaskQueue.DecInflight();
    }

    LOG_D("Terminal " << Context.TerminalID << " stopped");

    co_return;
}

bool TTerminal::IsDone() const {
    if (!Started) {
        return true;
    }

    if (!Task.Handle) {
        return true;
    }

    return Task.Handle.done();
}

} // namespace NYdb::NTPCC
