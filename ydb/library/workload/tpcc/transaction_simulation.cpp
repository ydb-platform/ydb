#include "transactions.h"

#include "constants.h"
#include "log.h"
#include "util.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/singleton.h>
#include <util/string/printf.h>

#include <format>
#include <unordered_map>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

NYdb::NQuery::TAsyncExecuteQueryResult Select1(
    NQuery::TSession& session,
    const std::optional<NYdb::NQuery::TTransaction>& tx,
    TTransactionContext& context)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $count AS Int32;

        SELECT $count;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$count").Int32(1).Build()
        .Build();

    auto txControl = tx ? TTxControl::Tx(*tx) : TTxControl::BeginTx(TTxSettings::SerializableRW());
    auto result = session.ExecuteQuery(
        query,
        txControl,
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for select1");
    return result;
}

} // anonymous

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetSimulationTask(
    TTransactionContext& context,
    TDuration& latency,
    NQuery::TSession session)
{
    TMonotonic startTs = TMonotonic::Now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;

    LOG_T("Terminal " << context.TerminalID << " started simulated transaction, session: " << session.GetId());

    // just to test if we have problems with generator (we don't)
    for (size_t i = 0; i < 10; ++i) {
        RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    }

    if (context.SimulateTransactionMs != 0) {
        std::chrono::milliseconds delay(context.SimulateTransactionMs);
        co_await TSuspend(context.TaskQueue, context.TerminalID, delay);
        TMonotonic endTs = TMonotonic::Now();
        latency = endTs - startTs;
        co_return TStatus(EStatus::SUCCESS, NIssue::TIssues());
    }

    // sleep 1 sumulation

    std::optional<TTransaction> tx;
    for (int i = 0; i < context.SimulateTransactionSelect1; ++i) {
        auto future = Select1(session, tx, context);
        auto result = co_await TSuspendWithFuture(future, context.TaskQueue, context.TerminalID);
        if (!result.IsSuccess()) {
            co_return result;
        }

        if (!tx) {
            tx = *result.GetTransaction();
            LOG_T("Terminal " << context.TerminalID << " simulated transaction txId " << tx->GetId());
        }
    }

    auto commitFuture = tx->Commit();
    auto commitResult = co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);

    TMonotonic endTs = TMonotonic::Now();
    latency = endTs - startTs;

    co_return commitResult;
}

} // namespace NYdb::NTPCC
