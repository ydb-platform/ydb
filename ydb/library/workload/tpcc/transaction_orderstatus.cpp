#include "transactions.h"

#include <util/string/printf.h>

#include "common_queries.h"
#include "constants.h"
#include "log.h"
#include "util.h"

#include <format>
#include <string>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb;
using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

struct TOrderLine {
    int ol_supply_w_id;
    int ol_i_id;
    double ol_quantity;
    double ol_amount;
    TInstant ol_delivery_d;
};

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetOrderByCustomer(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int customerID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $o_w_id AS Int32;
        DECLARE $o_d_id AS Int32;
        DECLARE $o_c_id AS Int32;

        SELECT O_W_ID, O_D_ID, O_C_ID, O_ID, O_CARRIER_ID, O_ENTRY_D
          FROM `oorder` VIEW idx_order AS idx
         WHERE idx.O_W_ID = $o_w_id
           AND idx.O_D_ID = $o_d_id
           AND idx.O_C_ID = $o_c_id
         ORDER BY idx.O_W_ID DESC, idx.O_D_ID DESC, idx.O_C_ID DESC, idx.O_ID DESC
         LIMIT 1;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$o_w_id").Int32(warehouseID).Build()
        .AddParam("$o_d_id").Int32(districtID).Build()
        .AddParam("$o_c_id").Int32(customerID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for order by customer result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetOrderLines(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int orderID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $ol_w_id AS Int32;
        DECLARE $ol_d_id AS Int32;
        DECLARE $ol_o_id AS Int32;

        SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
          FROM `order_line`
         WHERE OL_O_ID = $ol_o_id
           AND OL_D_ID = $ol_d_id
           AND OL_W_ID = $ol_w_id;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$ol_w_id").Int32(warehouseID).Build()
        .AddParam("$ol_d_id").Int32(districtID).Build()
        .AddParam("$ol_o_id").Int32(orderID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for order lines result");
    return result;
}

} // anonymous

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetOrderStatusTask(TTransactionContext& context,
    TSession session)
{
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;
    LOG_T("Terminal " << context.TerminalID << " started OrderStatus transaction");

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);

    // Determine lookup method (60% by name, 40% by id)
    bool lookupByName = RandomNumber(1, 100) <= 60;

    TCustomer customer;
    std::optional<TTransaction> tx;

    // Get Customer

    if (lookupByName) {
        // by last name
        TString lastName = GetNonUniformRandomLastNameForRun();

        auto customersFuture = GetCustomersByLastName(session, std::nullopt, context, warehouseID, districtID, lastName);
        auto customersResult = co_await TSuspendWithFuture(customersFuture, context.TaskQueue, context.TerminalID);
        if (!customersResult.IsSuccess()) {
            if (ShouldExit(customersResult)) {
                LOG_E("Terminal " << context.TerminalID << " customers query failed: "
                    << customersResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return customersResult;
        }

        tx = *customersResult.GetTransaction();

        auto selectedCustomer = SelectCustomerFromResultSet(customersResult.GetResultSet(0));
        if (!selectedCustomer) {
            LOG_E("Terminal " << context.TerminalID << " no customer found by name: "
                << warehouseID << ", " << districtID << ", " << lastName);
            std::quick_exit(1);
        }
        customer = std::move(*selectedCustomer);
    } else {
        // by ID
        int customerID = GetRandomCustomerID();

        auto customerFuture = GetCustomerById(session, std::nullopt, context, warehouseID, districtID, customerID);
        auto customerResult = co_await TSuspendWithFuture(customerFuture, context.TaskQueue, context.TerminalID);
        if (!customerResult.IsSuccess()) {
            if (ShouldExit(customerResult)) {
                LOG_E("Terminal " << context.TerminalID << " customer query failed: "
                    << customerResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return customerResult;
        }

        tx = *customerResult.GetTransaction();

        TResultSetParser customerParser(customerResult.GetResultSet(0));
        if (!customerParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " no customer found by id: "
                << warehouseID << ", " << districtID << ", " << customerID);
        }

        customer = ParseCustomerFromResult(customerParser);
        customer.c_id = customerID;
    }

    // Get the newest order for this customer

    auto orderFuture = GetOrderByCustomer(session, *tx, context, warehouseID, districtID, customer.c_id);
    auto orderResult = co_await TSuspendWithFuture(orderFuture, context.TaskQueue, context.TerminalID);
    if (!orderResult.IsSuccess()) {
        if (ShouldExit(orderResult)) {
            LOG_E("Terminal " << context.TerminalID << " order query failed: "
                << orderResult.GetIssues().ToOneLineString());
            std::quick_exit(1);
        }
        co_return orderResult;
    }

    TResultSetParser orderParser(orderResult.GetResultSet(0));
    if (!orderParser.TryNextRow()) {
        LOG_T("Terminal " << context.TerminalID << " customer has no orders");
        co_return orderResult;
    }
    int orderID = orderParser.ColumnParser("O_ID").GetInt32();

    // Get the order lines for this order

    auto orderLinesFuture = GetOrderLines(session, *tx, context, warehouseID, districtID, orderID);
    auto orderLinesResult = co_await TSuspendWithFuture(orderLinesFuture, context.TaskQueue, context.TerminalID);
    if (!orderLinesResult.IsSuccess()) {
        if (ShouldExit(orderLinesResult)) {
            LOG_E("Terminal " << context.TerminalID << " order lines query failed: "
                << orderLinesResult.GetIssues().ToOneLineString());
            std::quick_exit(1);
        }
        co_return orderLinesResult;
    }

    LOG_T("Terminal " << context.TerminalID << " is committing OrderStatus transaction: "
        << "customer " << customer.c_id << ", order " << orderID
        << ", lines " << orderLinesResult.GetResultSet(0).RowsCount());

    auto commitFuture = tx->Commit();
    co_return co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);
}

} // namespace NYdb::NTPCC
