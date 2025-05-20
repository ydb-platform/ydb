#include "transactions.h"

#include <util/string/printf.h>

#include "constants.h"
#include "log.h"
#include "util.h"

#include <format>
#include <string>
#include <vector>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

struct TOrderData {
    TOrderData() {
        OrderLineNumbers.reserve(MAX_ITEMS);
    }

    int OrderID;
    int CustomerId;
    std::vector<int> OrderLineNumbers;
    double TotalAmount;
};

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetNewOrder(
    TSession& session, const std::optional<TTransaction>& tx, TTransactionContext& context, int warehouseID, int districtID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $no_d_id AS Int32;
        DECLARE $no_w_id AS Int32;

        SELECT NO_W_ID, NO_D_ID, NO_O_ID FROM `new_order`
         WHERE NO_D_ID = $no_d_id
           AND NO_W_ID = $no_w_id
         ORDER BY NO_W_ID ASC, NO_D_ID ASC, NO_O_ID ASC
         LIMIT 1;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$no_d_id").Int32(districtID).Build()
        .AddParam("$no_w_id").Int32(warehouseID).Build()
        .Build();

    auto txControl = tx ? TTxControl::Tx(*tx) : TTxControl::BeginTx(TTxSettings::SerializableRW());
    auto result = session.ExecuteQuery(
        query,
        txControl,
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for new order result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult DeleteNewOrder(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int orderID, int districtID, int warehouseID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $no_o_id AS Int32;
        DECLARE $no_d_id AS Int32;
        DECLARE $no_w_id AS Int32;

        DELETE FROM `new_order`
         WHERE NO_O_ID = $no_o_id
           AND NO_D_ID = $no_d_id
           AND NO_W_ID = $no_w_id;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$no_o_id").Int32(orderID).Build()
        .AddParam("$no_d_id").Int32(districtID).Build()
        .AddParam("$no_w_id").Int32(warehouseID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for new order delete result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetCustomerID(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int orderID, int districtID, int warehouseID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $o_id AS Int32;
        DECLARE $o_d_id AS Int32;
        DECLARE $o_w_id AS Int32;

        SELECT O_C_ID
          FROM `oorder`
         WHERE O_ID = $o_id
           AND O_D_ID = $o_d_id
           AND O_W_ID = $o_w_id;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$o_id").Int32(orderID).Build()
        .AddParam("$o_d_id").Int32(districtID).Build()
        .AddParam("$o_w_id").Int32(warehouseID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer ID result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateCarrierID(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int orderID, int districtID, int warehouseID, int carrierID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $o_id AS Int32;
        DECLARE $o_d_id AS Int32;
        DECLARE $o_w_id AS Int32;
        DECLARE $o_carrier_id AS Int32;

        UPSERT INTO `oorder` (O_ID, O_D_ID, O_W_ID, O_CARRIER_ID)
         VALUES ($o_id, $o_d_id, $o_w_id, $o_carrier_id);
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$o_id").Int32(orderID).Build()
        .AddParam("$o_d_id").Int32(districtID).Build()
        .AddParam("$o_w_id").Int32(warehouseID).Build()
        .AddParam("$o_carrier_id").Int32(carrierID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for carrier ID update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetOrderLines(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int orderID, int districtID, int warehouseID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $ol_o_id AS Int32;
        DECLARE $ol_d_id AS Int32;
        DECLARE $ol_w_id AS Int32;

        SELECT OL_NUMBER, OL_AMOUNT
          FROM `order_line`
         WHERE OL_O_ID = $ol_o_id
           AND OL_D_ID = $ol_d_id
           AND OL_W_ID = $ol_w_id;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$ol_o_id").Int32(orderID).Build()
        .AddParam("$ol_d_id").Int32(districtID).Build()
        .AddParam("$ol_w_id").Int32(warehouseID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for order lines result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateOrderLineDeliveryDate(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int orderID, int lineNumber, TInstant deliveryDate)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $ol_w_id AS Int32;
        DECLARE $ol_d_id AS Int32;
        DECLARE $ol_o_id AS Int32;
        DECLARE $ol_number AS Int32;
        DECLARE $ol_delivery_d AS Timestamp;

        UPSERT INTO `order_line` (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_DELIVERY_D)
         VALUES ($ol_w_id, $ol_d_id, $ol_o_id, $ol_number, $ol_delivery_d);
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$ol_w_id").Int32(warehouseID).Build()
        .AddParam("$ol_d_id").Int32(districtID).Build()
        .AddParam("$ol_o_id").Int32(orderID).Build()
        .AddParam("$ol_number").Int32(lineNumber).Build()
        .AddParam("$ol_delivery_d").Timestamp(deliveryDate).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for order line delivery date update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetCustomerData(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int customerID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;

        SELECT C_BALANCE, C_DELIVERY_CNT
          FROM `customer`
         WHERE C_W_ID = $c_w_id
           AND C_D_ID = $c_d_id
           AND C_ID = $c_id;
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(customerID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer data result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateCustomerBalanceAndDeliveryCount(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int warehouseID, int districtID, int customerID, double balance, int deliveryCount)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;
        DECLARE $c_balance AS Double;
        DECLARE $c_delivery_cnt AS Int32;

        UPSERT INTO `customer` (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_DELIVERY_CNT)
         VALUES ($c_w_id, $c_d_id, $c_id, $c_balance, $c_delivery_cnt);
    )", context.Path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(customerID).Build()
        .AddParam("$c_balance").Double(balance).Build()
        .AddParam("$c_delivery_cnt").Int32(deliveryCount).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer balance and delivery count update result");
    return result;
}

} // anonymous

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetDeliveryTask(TTransactionContext& context,
    TSession session)
{
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;
    LOG_T("Terminal " << context.TerminalID << " started Delivery transaction");

    const int warehouseID = context.WarehouseID;
    const int carrierID = RandomNumber(1, 10);

    size_t processedOrderCount = 0;
    std::optional<TTransaction> tx;

    for (int districtID = DISTRICT_LOW_ID; districtID <= DISTRICT_HIGH_ID; ++districtID) {
        // Get the oldest new order for this district
        auto newOrderFuture = GetNewOrder(session, tx, context, warehouseID, districtID);
        auto newOrderResult = co_await TSuspendWithFuture(newOrderFuture, context.TaskQueue, context.TerminalID);
        if (!newOrderResult.IsSuccess()) {
            if (ShouldExit(newOrderResult)) {
                LOG_E("Terminal " << context.TerminalID << " new order query failed: "
                    << newOrderResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return newOrderResult;
        }

        if (!tx) {
            tx = *newOrderResult.GetTransaction();
            LOG_T("Terminal " << context.TerminalID << " Delivery txId " << tx->GetId());
        }

        // Check if there's a new order for this district
        TResultSetParser orderParser(newOrderResult.GetResultSet(0));
        if (!orderParser.TryNextRow()) {
            LOG_T("Terminal " << context.TerminalID << " no new orders for district " << districtID);
            continue;
        }
        int orderID = orderParser.ColumnParser("NO_O_ID").GetInt32();

        // Get the customer ID from the order
        auto customerIdFuture = GetCustomerID(session, *tx, context, orderID, districtID, warehouseID);
        auto customerIdResult = co_await TSuspendWithFuture(customerIdFuture, context.TaskQueue, context.TerminalID);
        if (!customerIdResult.IsSuccess()) {
            if (ShouldExit(customerIdResult)) {
                LOG_E("Terminal " << context.TerminalID << " get customer ID failed: "
                    << customerIdResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return customerIdResult;
        }

        TResultSetParser customerParser(customerIdResult.GetResultSet(0));
        if (!customerParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " failed to get customerID "
                << warehouseID << ", " <<  districtID << ", " << ", " << orderID);
            std::quick_exit(1);
        }
        int customerID = *customerParser.ColumnParser("O_C_ID").GetOptionalInt32();

        // Get customer data
        auto customerDataFuture = GetCustomerData(session, *tx, context, warehouseID, districtID, customerID);
        auto customerDataResult = co_await TSuspendWithFuture(customerDataFuture, context.TaskQueue, context.TerminalID);
        if (!customerDataResult.IsSuccess()) {
            if (ShouldExit(customerDataResult)) {
                LOG_E("Terminal " << context.TerminalID << " get customer data failed: "
                    << customerDataResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return customerDataResult;
        }

        TResultSetParser customerDataParser(customerDataResult.GetResultSet(0));
        if (!customerDataParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " failed to get customer data for "
                << warehouseID << ", " <<  districtID << ", " << ", " << customerID);
            std::quick_exit(1);
        }

        double customerBalance = *customerDataParser.ColumnParser("C_BALANCE").GetOptionalDouble();
        double deliveryCount = *customerDataParser.ColumnParser("C_DELIVERY_CNT").GetOptionalInt32();

        // Get information about the order lines
        auto orderLinesFuture = GetOrderLines(session, *tx, context, orderID, districtID, warehouseID);
        auto orderLinesResult = co_await TSuspendWithFuture(orderLinesFuture, context.TaskQueue, context.TerminalID);
        if (!orderLinesResult.IsSuccess()) {
            if (ShouldExit(orderLinesResult)) {
                LOG_E("Terminal " << context.TerminalID << " get order lines failed: "
                    << orderLinesResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return orderLinesResult;
        }

        // Process order lines and calculate total amount
        TOrderData orderData;
        orderData.OrderID = orderID;
        orderData.CustomerId = customerID;
        orderData.TotalAmount = 0.0;

        TResultSetParser orderLinesParser(orderLinesResult.GetResultSet(0));
        while (orderLinesParser.TryNextRow()) {
            int lineNumber = orderLinesParser.ColumnParser("OL_NUMBER").GetInt32();
            double amount = *orderLinesParser.ColumnParser("OL_AMOUNT").GetOptionalDouble();

            orderData.OrderLineNumbers.push_back(lineNumber);
            orderData.TotalAmount += amount;
        }

        if (orderData.OrderLineNumbers.empty()) {
            LOG_E("Terminal " << context.TerminalID << " failed to get order lines for "
                << warehouseID << ", " <<  districtID << ", " << ", " << orderID);
            std::quick_exit(1);
        }

        // Update customer balance and delivery count
        customerBalance += orderData.TotalAmount;
        deliveryCount += 1;

        // Delete the entry from the new order table
        auto deleteOrderFuture = DeleteNewOrder(session, *tx, context, orderID, districtID, warehouseID);
        auto deleteOrderResult = co_await TSuspendWithFuture(deleteOrderFuture, context.TaskQueue, context.TerminalID);
        if (!deleteOrderResult.IsSuccess()) {
            if (ShouldExit(deleteOrderResult)) {
                LOG_E("Terminal " << context.TerminalID << " delete order failed: "
                    << deleteOrderResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return deleteOrderResult;
        }

        // Update the carrier ID in the order
        auto updateCarrierFuture = UpdateCarrierID(session, *tx, context, orderID, districtID, warehouseID, carrierID);
        auto updateCarrierResult = co_await TSuspendWithFuture(updateCarrierFuture, context.TaskQueue, context.TerminalID);
        if (!updateCarrierResult.IsSuccess()) {
            if (ShouldExit(updateCarrierResult)) {
                LOG_E("Terminal " << context.TerminalID << " update carrier ID failed: "
                    << updateCarrierResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return updateCarrierResult;
        }

        // Update all the order lines with the delivery date
        TInstant deliveryDate = TInstant::Now();
        for (int lineNumber : orderData.OrderLineNumbers) {
            auto updateDeliveryDateFuture = UpdateOrderLineDeliveryDate(
                session, *tx, context, warehouseID, districtID, orderID, lineNumber, deliveryDate);

            auto updateDeliveryDateResult = co_await TSuspendWithFuture(
                updateDeliveryDateFuture, context.TaskQueue, context.TerminalID);

            if (!updateDeliveryDateResult.IsSuccess()) {
                if (ShouldExit(updateDeliveryDateResult)) {
                    LOG_E("Terminal " << context.TerminalID << " update delivery date failed: "
                        << updateDeliveryDateResult.GetIssues().ToOneLineString());
                    std::quick_exit(1);
                }
                co_return updateDeliveryDateResult;
            }
        }

       auto updateCustomerFuture = UpdateCustomerBalanceAndDeliveryCount(
            session, *tx, context, warehouseID, districtID, customerID, customerBalance, deliveryCount);

        auto updateCustomerResult = co_await TSuspendWithFuture(updateCustomerFuture, context.TaskQueue, context.TerminalID);
        if (!updateCustomerResult.IsSuccess()) {
            if (ShouldExit(updateCustomerResult)) {
                LOG_E("Terminal " << context.TerminalID << " update customer failed: "
                    << updateCustomerResult.GetIssues().ToOneLineString());
                std::quick_exit(1);
            }
            co_return updateCustomerResult;
        }

        // Add this order to the processed orders
        ++processedOrderCount;
    }

    LOG_T("Terminal " << context.TerminalID
        << " is committing Delivery transaction, processed " << processedOrderCount << " districts");

    auto commitFuture = tx->Commit();
    co_return co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);
}

} // namespace NYdb::NTPCC
