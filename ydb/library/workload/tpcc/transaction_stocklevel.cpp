#include "transactions.h"

#include <util/string/printf.h>

#include "constants.h"
#include "log.h"
#include "util.h"

#include <format>
#include <string>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetDistrictOrderId(
    TSession& session, TTransactionContext& context, int warehouseID, int districtID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $d_w_id AS Int32;
        DECLARE $d_id AS Int32;

        SELECT D_NEXT_O_ID
          FROM `district`
         WHERE D_W_ID = $d_w_id
           AND D_ID = $d_id;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$d_w_id").Int32(warehouseID).Build()
        .AddParam("$d_id").Int32(districtID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for district order ID result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetStockCount(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int orderID, int threshold)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $ol_w_id AS Int32;
        DECLARE $ol_d_id AS Int32;
        DECLARE $ol_o_id_high AS Int32;
        DECLARE $ol_o_id_low AS Int32;
        DECLARE $s_w_id AS Int32;
        DECLARE $s_quantity AS Int32;

        SELECT COUNT(DISTINCT (s.S_I_ID)) AS STOCK_COUNT
         FROM `order_line` as ol INNER JOIN `stock` as s ON s.S_I_ID = ol.OL_I_ID
         WHERE ol.OL_W_ID = $ol_w_id
         AND ol.OL_D_ID = $ol_d_id
         AND ol.OL_O_ID < $ol_o_id_high
         AND ol.OL_O_ID >= $ol_o_id_low
         AND s.S_W_ID = $s_w_id
         AND s.S_QUANTITY < $s_quantity;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$ol_w_id").Int32(warehouseID).Build()
        .AddParam("$ol_d_id").Int32(districtID).Build()
        .AddParam("$ol_o_id_high").Int32(orderID).Build()
        .AddParam("$ol_o_id_low").Int32(orderID - 20).Build()
        .AddParam("$s_w_id").Int32(warehouseID).Build()
        .AddParam("$s_quantity").Int32(threshold).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for stock count result");
    return result;
}

} // anonymous

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetStockLevelTask(TTransactionContext& context,
    TSession session)
{
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    const int threshold = RandomNumber(10, 20);

    LOG_T("Terminal " << context.TerminalID << " started StockLevel transaction in "
        << warehouseID << ", " << districtID);

    // Get next order ID from district
    auto districtFuture = GetDistrictOrderId(session, context, warehouseID, districtID);
    auto districtResult = co_await TSuspendWithFuture(districtFuture, context.TaskQueue, context.TerminalID);
    if (!districtResult.IsSuccess()) {
        LOG_E("Terminal " << context.TerminalID << " district query failed: "
            << districtResult.GetIssues().ToOneLineString());
        if (ShouldExit(districtResult)) {
            std::quick_exit(1);
        }
        co_return districtResult;
    }

    auto tx = *districtResult.GetTransaction();
    LOG_T("Terminal " << context.TerminalID << " StockLevel txId " << tx.GetId());

    TResultSetParser districtParser(districtResult.GetResultSet(0));
    if (!districtParser.TryNextRow()) {
        LOG_E("Terminal " << context.TerminalID
            << ", warehouseId " << warehouseID << ", districtId " << districtID << " not found");
        std::quick_exit(1);
    }

    int nextOrderID = *districtParser.ColumnParser("D_NEXT_O_ID").GetOptionalInt32();

    // Get stock count
    auto stockCountFuture = GetStockCount(session, tx, context, warehouseID, districtID, nextOrderID, threshold);
    auto stockCountResult = co_await TSuspendWithFuture(stockCountFuture, context.TaskQueue, context.TerminalID);
    if (!stockCountResult.IsSuccess()) {
        if (ShouldExit(stockCountResult)) {
            LOG_E("Terminal " << context.TerminalID << " stock count query failed: "
                << stockCountResult.GetIssues().ToOneLineString());
            std::quick_exit(1);
        }
        co_return stockCountResult;
    }

    LOG_T("Terminal " << context.TerminalID << " is committing StockLevel transaction");

    auto commitFuture = tx.Commit();
    co_return co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);
}

} // namespace NYdb::NTPCC
