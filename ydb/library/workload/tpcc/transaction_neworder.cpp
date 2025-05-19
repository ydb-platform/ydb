#include "transactions.h"

#include <util/string/printf.h>

#include "constants.h"
#include "log.h"

namespace NYdb::NTPCC {

namespace {

using namespace NQuery;

TAsyncExecuteQueryResult GetCustomer(TSession session, const TString& path, int warehouseID, int districtID, int customerID) {
    TString query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;

        SELECT C_DISCOUNT, C_LAST, C_CREDIT
          FROM customer
         WHERE C_W_ID = $c_w_id
           AND C_D_ID = $c_d_id
           AND C_ID = $c_id;
    )", path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(customerID).Build()
        .Build();

    return session.ExecuteQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()),
        std::move(params));
}

} // anonymous

TTransactionTask GetNewOrderTask(TTransactionContext& context)
{
    auto& Log = context.Log; // to make LOG_* macros working
    LOG_D("Terminal " << context.TerminalID << " started NewOrder transaction");

    // XXX
    TString path = "/Root/db1";
    int warehouseID = 0;
    int districtID = 0;
    int customerID = 10;

    auto clientFuture = context.Client->RetryQuery(
        [&path, warehouseID, districtID, customerID](TSession session) {
            return GetCustomer(session, path, warehouseID, districtID, customerID);
        }
    );

    LOG_D("Terminal " << context.TerminalID << " waiting for result");
    auto clientResult = co_await TSuspendWithFuture(clientFuture, context.TaskQueue, context.TerminalID);
    LOG_D("Terminal " << context.TerminalID << " got future");

    LOG_D("Terminal " << context.TerminalID << " finished NewOrder transaction, success: " << clientResult.IsSuccess());
    co_return TTransactionResult{};
}

} // namespace NYdb::NTPCC
