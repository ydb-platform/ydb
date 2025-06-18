#include "transactions.h"

#include "constants.h"
#include "log.h"
#include "util.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/string/printf.h>

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

    int OrderID = 0;
    int CustomerId = 0;
    double TotalAmount = 0;
    double DeliveryCount = 0;
    double CustomerBalance = 0;

    std::vector<int> OrderLineNumbers;
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

        SELECT NO_W_ID, NO_D_ID, NO_O_ID FROM `{}`
         WHERE NO_D_ID = $no_d_id
           AND NO_W_ID = $no_w_id
         ORDER BY NO_W_ID ASC, NO_D_ID ASC, NO_O_ID ASC
         LIMIT 1;
    )", context.Path.c_str(), TABLE_NEW_ORDER);

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

        DELETE FROM `{}`
         WHERE NO_O_ID = $no_o_id
           AND NO_D_ID = $no_d_id
           AND NO_W_ID = $no_w_id;
    )", context.Path.c_str(), TABLE_NEW_ORDER);

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
          FROM `{}`
         WHERE O_ID = $o_id
           AND O_D_ID = $o_d_id
           AND O_W_ID = $o_w_id;
    )", context.Path.c_str(), TABLE_OORDER);

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

        UPSERT INTO `{}` (O_W_ID, O_D_ID, O_ID, O_CARRIER_ID)
         VALUES ($o_w_id, $o_d_id, $o_id, $o_carrier_id);
    )", context.Path.c_str(), TABLE_OORDER);

    auto params = TParamsBuilder()
        .AddParam("$o_w_id").Int32(warehouseID).Build()
        .AddParam("$o_d_id").Int32(districtID).Build()
        .AddParam("$o_id").Int32(orderID).Build()
        .AddParam("$o_carrier_id").Int32(carrierID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for carrier ID update result");
    return result;
}

TAsyncExecuteQueryResult UpdateDeliveryDate(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    int districtID, int warehouseID, const TOrderData& orderData, TInstant timestamp)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $values as List<Struct<p1:Int,p2:Int32,p3:Int32,p4:Int32,p5:Timestamp>>;
        $mapper = ($row) -> (AsStruct(
            $row.p1 as OL_W_ID, $row.p2 as OL_D_ID, $row.p3 as OL_O_ID,
            $row.p4 as OL_NUMBER, $row.p5 as OL_DELIVERY_D));
        UPSERT INTO `{}` SELECT * FROM as_table(ListMap($values, $mapper));
    )", context.Path.c_str(), TABLE_ORDER_LINE);

    auto paramsBuilder = TParamsBuilder();
    auto& listBuilder = paramsBuilder.AddParam("$values").BeginList();
    for (const auto& lineNum : orderData.OrderLineNumbers) {
        listBuilder.AddListItem().BeginStruct()
            .AddMember("p1").Int32(warehouseID)
            .AddMember("p2").Int32(districtID)
            .AddMember("p3").Int32(orderData.OrderID)
            .AddMember("p4").Int32(lineNum)
            .AddMember("p5").Timestamp(timestamp)
        .EndStruct();
    }

    auto params = listBuilder.EndList().Build().Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for delivery date update result");
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
          FROM `{}`
         WHERE OL_O_ID = $ol_o_id
           AND OL_D_ID = $ol_d_id
           AND OL_W_ID = $ol_w_id;
    )", context.Path.c_str(), TABLE_ORDER_LINE);

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
          FROM `{}`
         WHERE C_W_ID = $c_w_id
           AND C_D_ID = $c_d_id
           AND C_ID = $c_id;
    )", context.Path.c_str(), TABLE_CUSTOMER);

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
    int warehouseID, int districtID, const TOrderData& orderData)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;
        DECLARE $c_balance AS Double;
        DECLARE $c_delivery_cnt AS Int32;

        UPSERT INTO `{}` (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_DELIVERY_CNT)
         VALUES ($c_w_id, $c_d_id, $c_id, $c_balance, $c_delivery_cnt);
    )", context.Path.c_str(), TABLE_CUSTOMER);

    auto params = TParamsBuilder()
        .AddParam("$c_w_id").Int32(warehouseID).Build()
        .AddParam("$c_d_id").Int32(districtID).Build()
        .AddParam("$c_id").Int32(orderData.CustomerId).Build()
        .AddParam("$c_balance").Double(orderData.CustomerBalance).Build()
        .AddParam("$c_delivery_cnt").Int32(orderData.DeliveryCount).Build()
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

NThreading::TFuture<TStatus> GetDeliveryTask(
    TTransactionContext& context,
    TDuration& latency,
    TSession session)
{
    TMonotonic startTs = TMonotonic::Now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;

    const int warehouseID = context.WarehouseID;
    const int carrierID = RandomNumber(1, 10);

    LOG_T("Terminal " << context.TerminalID << " started Delivery transaction in " << warehouseID
        << ", session: " << session.GetId());

    size_t processedOrderCount = 0;
    std::optional<TTransaction> tx;

    std::array<std::optional<TOrderData>, DISTRICT_HIGH_ID - DISTRICT_LOW_ID + 1> orders;

    for (int districtID = DISTRICT_LOW_ID; districtID <= DISTRICT_HIGH_ID; ++districtID) {
        // Get the oldest new order for this district
        auto newOrderFuture = GetNewOrder(session, tx, context, warehouseID, districtID);
        auto newOrderResult = co_await TSuspendWithFuture(newOrderFuture, context.TaskQueue, context.TerminalID);
        if (!newOrderResult.IsSuccess()) {
            if (ShouldExit(newOrderResult)) {
                LOG_E("Terminal " << context.TerminalID << " new order query failed: "
                    << newOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " new order query failed: "
                << newOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
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
        orders[districtID - 1] = TOrderData();
        auto& currentOrder = *orders[districtID - 1];
        currentOrder.OrderID = orderParser.ColumnParser("NO_O_ID").GetInt32();

        // Get the customer ID from the order
        auto customerIdFuture = GetCustomerID(session, *tx, context, currentOrder.OrderID, districtID, warehouseID);
        auto customerIdResult = co_await TSuspendWithFuture(customerIdFuture, context.TaskQueue, context.TerminalID);
        if (!customerIdResult.IsSuccess()) {
            if (ShouldExit(customerIdResult)) {
                LOG_E("Terminal " << context.TerminalID << " get customer ID failed: "
                    << customerIdResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " get customer ID failed: "
                << customerIdResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return customerIdResult;
        }

        TResultSetParser customerParser(customerIdResult.GetResultSet(0));
        if (!customerParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " failed to get customerID "
                << warehouseID << ", " <<  districtID << ", " << ", " << currentOrder.OrderID);
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        currentOrder.CustomerId = *customerParser.ColumnParser("O_C_ID").GetOptionalInt32();

        // Get customer data
        auto customerDataFuture = GetCustomerData(session, *tx, context, warehouseID, districtID, currentOrder.CustomerId);
        auto customerDataResult = co_await TSuspendWithFuture(customerDataFuture, context.TaskQueue, context.TerminalID);
        if (!customerDataResult.IsSuccess()) {
            if (ShouldExit(customerDataResult)) {
                LOG_E("Terminal " << context.TerminalID << " get customer data failed: "
                    << customerDataResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " get customer data failed: "
                << customerDataResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return customerDataResult;
        }

        TResultSetParser customerDataParser(customerDataResult.GetResultSet(0));
        if (!customerDataParser.TryNextRow()) {
            LOG_E("Terminal " << context.TerminalID << " failed to get customer data for "
                << warehouseID << ", " <<  districtID << ", " << ", " << currentOrder.CustomerId);
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }

        currentOrder.CustomerBalance = *customerDataParser.ColumnParser("C_BALANCE").GetOptionalDouble();
        currentOrder.DeliveryCount = *customerDataParser.ColumnParser("C_DELIVERY_CNT").GetOptionalInt32();

        // Get information about the order lines
        auto orderLinesFuture = GetOrderLines(session, *tx, context, currentOrder.OrderID, districtID, warehouseID);
        auto orderLinesResult = co_await TSuspendWithFuture(orderLinesFuture, context.TaskQueue, context.TerminalID);
        if (!orderLinesResult.IsSuccess()) {
            if (ShouldExit(orderLinesResult)) {
                LOG_E("Terminal " << context.TerminalID << " get order lines failed: "
                    << orderLinesResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " get order lines failed: "
                << orderLinesResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return orderLinesResult;
        }

        TResultSetParser orderLinesParser(orderLinesResult.GetResultSet(0));
        while (orderLinesParser.TryNextRow()) {
            int lineNumber = orderLinesParser.ColumnParser("OL_NUMBER").GetInt32();
            double amount = *orderLinesParser.ColumnParser("OL_AMOUNT").GetOptionalDouble();

            currentOrder.OrderLineNumbers.push_back(lineNumber);
            currentOrder.TotalAmount += amount;
        }

        if (currentOrder.OrderLineNumbers.empty()) {
            LOG_E("Terminal " << context.TerminalID << " failed to get order lines for "
                << warehouseID << ", " <<  districtID << ", " << ", " << currentOrder.OrderID);
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }

        // Update customer balance and delivery count
        currentOrder.CustomerBalance += currentOrder.TotalAmount;
        currentOrder.DeliveryCount += 1;
    }

    for (int districtID = DISTRICT_LOW_ID; districtID <= DISTRICT_HIGH_ID; ++districtID) {
        if (!orders[districtID - 1]) {
            continue;
        }
        auto& currentOrder = *orders[districtID - 1];

        // Delete the entry from the new order table
        auto deleteOrderFuture = DeleteNewOrder(session, *tx, context, currentOrder.OrderID, districtID, warehouseID);
        auto deleteOrderResult = co_await TSuspendWithFuture(deleteOrderFuture, context.TaskQueue, context.TerminalID);
        if (!deleteOrderResult.IsSuccess()) {
            if (ShouldExit(deleteOrderResult)) {
                LOG_E("Terminal " << context.TerminalID << " delete order failed: "
                    << deleteOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " delete order failed: "
                << deleteOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return deleteOrderResult;
        }

        // Update the carrier ID in the order
        auto updateCarrierFuture = UpdateCarrierID(session, *tx, context, currentOrder.OrderID, districtID, warehouseID, carrierID);
        auto updateCarrierResult = co_await TSuspendWithFuture(updateCarrierFuture, context.TaskQueue, context.TerminalID);
        if (!updateCarrierResult.IsSuccess()) {
            if (ShouldExit(updateCarrierResult)) {
                LOG_E("Terminal " << context.TerminalID << " update carrier ID failed: "
                    << updateCarrierResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " update carrier ID failed: "
                << updateCarrierResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return updateCarrierResult;
        }

        // Update all the order lines with the delivery date
        TInstant deliveryDate = TInstant::Now();
        auto updateDeliveryDateFuture = UpdateDeliveryDate(
            session, *tx, context, districtID, warehouseID, currentOrder, deliveryDate);
        auto updateDeliveryResult = co_await TSuspendWithFuture(updateDeliveryDateFuture, context.TaskQueue, context.TerminalID);
        if (!updateDeliveryResult.IsSuccess()) {
            if (ShouldExit(updateDeliveryResult)) {
                LOG_E("Terminal " << context.TerminalID << " update delivery date failed: "
                    << updateDeliveryResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " update delivery date failed: "
                << updateDeliveryResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return updateDeliveryResult;
        }

        auto updateCustomerFuture = UpdateCustomerBalanceAndDeliveryCount(
            session, *tx, context, warehouseID, districtID, currentOrder);
        auto updateCustomerResult = co_await TSuspendWithFuture(updateCustomerFuture, context.TaskQueue, context.TerminalID);
        if (!updateCustomerResult.IsSuccess()) {
            if (ShouldExit(updateCustomerResult)) {
                LOG_E("Terminal " << context.TerminalID << " update customer failed: "
                    << updateCustomerResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
                RequestStop();
                co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
            }
            LOG_T("Terminal " << context.TerminalID << " update customer failed: "
                << updateCustomerResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            co_return updateCustomerResult;
        }

        // Add this order to the processed orders
        ++processedOrderCount;
    }

    LOG_T("Terminal " << context.TerminalID
        << " is committing Delivery transaction, processed " << processedOrderCount << " districts, session: " << session.GetId());

    auto commitFuture = tx->Commit();
    auto commitResult = co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);

    TMonotonic endTs = TMonotonic::Now();
    latency = endTs - startTs;

    co_return commitResult;
}

} // namespace NYdb::NTPCC
