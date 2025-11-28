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

std::atomic<size_t> TransactionsInflight{0};

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

struct TSqlHelper {
    TSqlHelper(const TString& path) {
        // Items queries
        for (int i = 1; i <= MAX_ITEMS; ++i) {
            TStringStream ss;
            ss << Sprintf(R"(
                --!syntax_v1

                PRAGMA TablePathPrefix("%s");

            )", path.c_str());

            for (int j = 1; j <= i; ++j) {
                ss << Sprintf("DECLARE $item%d AS Int32;\n", j);
            }

            ss << "SELECT I_ID, I_PRICE, I_NAME, I_DATA FROM `" << TABLE_ITEM << "` WHERE I_ID IN (";
            for (int j = 1; j <= i; ++j) {
                if (j == 1) {
                    ss << "$item1";
                } else {
                    ss << ", $item" << j;
                }
            }
            ss << ")";
            ItemsSql[i - 1] = ss.Str();
        }

        // Stock queries
        for (int i = 1; i <= MAX_ITEMS; ++i) {
            TStringStream ss;
            ss << Sprintf(R"(
                --!syntax_v1

                PRAGMA TablePathPrefix("%s");

            )", path.c_str());

            for (int j = 1; j <= i; ++j) {
                ss << Sprintf("DECLARE $w%d AS Int32;\n", j);
                ss << Sprintf("DECLARE $i%d AS Int32;\n", j);
            }

            ss << "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, " <<
                            "S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " <<
                            "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 " <<
                            "FROM `" << TABLE_STOCK << "` WHERE (S_W_ID, S_I_ID) IN (";

            for (int j = 1; j <= i; ++j) {
                if (j == 1) {
                    ss << "($w1, $i1)";
                } else {
                    ss << ", ($w" << j << ", $i" << j << ")";
                }
            }
            ss << ")";
            StocksSql[i - 1] = ss.Str();
        }
    }

    TString ItemsSql[MAX_ITEMS];
    TString StocksSql[MAX_ITEMS];
};

//-----------------------------------------------------------------------------

struct StockUpdate {
    int WarehouseId;
    int ItemId;
    int Quantity;
    double Ytd;
    int OrderCount;
    int RemoteCount;
};

struct OrderLine {
    int WarehouseId;
    int DistrictId;
    int OrderId;
    int Number;
    int ItemId;
    double Amount;
    int SupplyWarehouseId;
    double Quantity;
    TString DistInfo;
};

struct Stock {
    int s_w_id;
    int s_i_id;
    int s_quantity;
    double s_ytd;
    int s_order_cnt;
    int s_remote_cnt;
    TString s_dist_01;
    TString s_dist_02;
    TString s_dist_03;
    TString s_dist_04;
    TString s_dist_05;
    TString s_dist_06;
    TString s_dist_07;
    TString s_dist_08;
    TString s_dist_09;
    TString s_dist_10;
};

struct TPairHash {
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2>& pair) const {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetCustomer(
    TSession& session, TTransactionContext& context, int warehouseID, int districtID, int customerID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $c_w_id AS Int32;
        DECLARE $c_d_id AS Int32;
        DECLARE $c_id AS Int32;

        SELECT C_DISCOUNT, C_LAST, C_CREDIT
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
        TTxControl::BeginTx(TTxSettings::SerializableRW()),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for customer result for "
        << warehouseID << ", " << districtID << ", " << customerID << ", session: " << session.GetId());
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetWarehouseTax(
    TSession& session, const TTransaction& tx, TTransactionContext& context, int warehouseID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $w_id AS Int32;

        SELECT W_TAX
          FROM `{}`
         WHERE W_ID = $w_id;
    )", context.Path.c_str(), TABLE_WAREHOUSE);

    auto params = TParamsBuilder()
        .AddParam("$w_id").Int32(warehouseID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for warehouse result for " << warehouseID << ", session: " << session.GetId());
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetDistrict(
    TSession& session, const TTransaction& tx, TTransactionContext& context, int warehouseID, int districtID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $d_w_id AS Int32;
        DECLARE $d_id AS Int32;

        SELECT D_NEXT_O_ID, D_TAX
          FROM `{}`
         WHERE D_W_ID = $d_w_id
           AND D_ID = $d_id;
    )", context.Path.c_str(), TABLE_DISTRICT);

    auto params = TParamsBuilder()
        .AddParam("$d_w_id").Int32(warehouseID).Build()
        .AddParam("$d_id").Int32(districtID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for district result for "
        << warehouseID << ", " << districtID << ", session: " << session.GetId());
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateDistrict(
    TSession& session,
    const TTransaction& tx,
    TTransactionContext& context,
    int warehouseID,
    int districtID,
    int nextOrderID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $d_w_id AS Int32;
        DECLARE $d_id AS Int32;
        DECLARE $d_next_o_id AS Int32;

        UPSERT INTO `{}` (D_W_ID, D_ID, D_NEXT_O_ID)
        VALUES ($d_w_id, $d_id, $d_next_o_id);
    )", context.Path.c_str(), TABLE_DISTRICT);

    auto params = TParamsBuilder()
        .AddParam("$d_w_id").Int32(warehouseID).Build()
        .AddParam("$d_id").Int32(districtID).Build()
        .AddParam("$d_next_o_id").Int32(nextOrderID).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for district update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult InsertNewOrder(
    TSession& session,
    const TTransaction& tx,
    TTransactionContext& context,
    int orderID,
    int districtID,
    int warehouseID)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $no_o_id AS Int32;
        DECLARE $no_d_id AS Int32;
        DECLARE $no_w_id AS Int32;

        UPSERT INTO `{}` (NO_O_ID, NO_D_ID, NO_W_ID)
        VALUES ($no_o_id, $no_d_id, $no_w_id);
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

    LOG_T("Terminal " << context.TerminalID << " waiting for new order insert result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult InsertOpenOrder(
    TSession& session,
    const TTransaction& tx,
    TTransactionContext& context,
    int orderID,
    int districtID,
    int warehouseID,
    int customerID,
    int orderLineCount,
    int allLocal)
{
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $o_id AS Int32;
        DECLARE $o_d_id AS Int32;
        DECLARE $o_w_id AS Int32;
        DECLARE $o_c_id AS Int32;
        DECLARE $o_entry_d AS Timestamp;
        DECLARE $o_ol_cnt AS Int32;
        DECLARE $o_all_local AS Int32;

        UPSERT INTO `{}` (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
        VALUES ($o_id, $o_d_id, $o_w_id, $o_c_id, $o_entry_d, $o_ol_cnt, $o_all_local);
    )", context.Path.c_str(), TABLE_OORDER);

    auto params = TParamsBuilder()
        .AddParam("$o_id").Int32(orderID).Build()
        .AddParam("$o_d_id").Int32(districtID).Build()
        .AddParam("$o_w_id").Int32(warehouseID).Build()
        .AddParam("$o_c_id").Int32(customerID).Build()
        .AddParam("$o_entry_d").Timestamp(TInstant::Now()).Build()
        .AddParam("$o_ol_cnt").Int32(orderLineCount).Build()
        .AddParam("$o_all_local").Int32(allLocal).Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for open order insert result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetItems(
    TSession& session,
    const TTransaction& tx,
    TTransactionContext& context,
    const std::vector<int>& itemIDs)
{
    auto& Log = context.Log;

    std::set<int> uniqueItemIDs(itemIDs.begin(), itemIDs.end());

    const TString& query = Singleton<TSqlHelper>(context.Path)->ItemsSql[uniqueItemIDs.size() - 1];
    auto paramsBuilder = TParamsBuilder();

    int itemCount = 1;
    for (auto itemID: uniqueItemIDs) {
        paramsBuilder.AddParam(Sprintf("$item%d", itemCount)).Int32(itemID).Build();
        itemCount++;
    }

    auto params = paramsBuilder.Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for items result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult GetStocks(
    TSession& session,
    const TTransaction& tx,
    TTransactionContext& context,
    const std::vector<std::pair<int, int>>& stockKeys)
{
    auto& Log = context.Log;

    std::set<std::pair<int, int>> uniquePairs(stockKeys.begin(), stockKeys.end());

    const TString& query = Singleton<TSqlHelper>(context.Path)->StocksSql[uniquePairs.size() - 1];
    auto paramsBuilder = TParamsBuilder();

    int itemCount = 1;
    for (auto [warehouseID, itemID]: uniquePairs) {
        paramsBuilder.AddParam(Sprintf("$w%d", itemCount)).Int32(warehouseID).Build();
        paramsBuilder.AddParam(Sprintf("$i%d", itemCount)).Int32(itemID).Build();
        itemCount++;
    }

    auto params = paramsBuilder.Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for stocks result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult UpdateStocks(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    const std::vector<StockUpdate>& stockUpdates) {
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Double,p5:Int32,p6:Int32>>;
        $mapper = ($row) -> (AsStruct(
            $row.p1 as S_W_ID, $row.p2 as S_I_ID, $row.p3 as S_QUANTITY,
            $row.p4 as S_YTD, $row.p5 as S_ORDER_CNT,
            $row.p6 as S_REMOTE_CNT));

        UPSERT INTO `{}` SELECT * FROM AS_TABLE(ListMap($values, $mapper));
    )", context.Path.c_str(), TABLE_STOCK);

    auto paramsBuilder = TParamsBuilder();
    auto& listBuilder = paramsBuilder.AddParam("$values").BeginList();
    for (const auto& update : stockUpdates) {
        listBuilder.AddListItem().BeginStruct()
            .AddMember("p1").Int32(update.WarehouseId)
            .AddMember("p2").Int32(update.ItemId)
            .AddMember("p3").Int32(update.Quantity)
            .AddMember("p4").Double(update.Ytd)
            .AddMember("p5").Int32(update.OrderCount)
            .AddMember("p6").Int32(update.RemoteCount)
        .EndStruct();
    }

    auto params = listBuilder.EndList().Build().Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for stocks update result");
    return result;
}

//-----------------------------------------------------------------------------

TAsyncExecuteQueryResult InsertOrderLines(
    TSession& session, const TTransaction& tx, TTransactionContext& context,
    const std::vector<OrderLine>& orderLines) {
    auto& Log = context.Log;
    static std::string query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        DECLARE $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Int32,p5:Int32,p6:Double,p7:Int32,p8:Double,p9:Utf8>>;
        $mapper = ($row) -> (AsStruct(
            $row.p1 as OL_W_ID, $row.p2 as OL_D_ID, $row.p3 as OL_O_ID, $row.p4 as OL_NUMBER, $row.p5 as OL_I_ID,
            $row.p6 as OL_AMOUNT, $row.p7 as OL_SUPPLY_W_ID, $row.p8 as OL_QUANTITY,
            $row.p9 as OL_DIST_INFO));

        UPSERT INTO `{}` SELECT * FROM AS_TABLE(ListMap($values, $mapper));
    )", context.Path.c_str(), TABLE_ORDER_LINE);

    auto paramsBuilder = TParamsBuilder();
    auto& listBuilder = paramsBuilder.AddParam("$values").BeginList();
    for (const auto& line : orderLines) {
        listBuilder.AddListItem().BeginStruct()
            .AddMember("p1").Int32(line.WarehouseId)
            .AddMember("p2").Int32(line.DistrictId)
            .AddMember("p3").Int32(line.OrderId)
            .AddMember("p4").Int32(line.Number)
            .AddMember("p5").Int32(line.ItemId)
            .AddMember("p6").Double(line.Amount)
            .AddMember("p7").Int32(line.SupplyWarehouseId)
            .AddMember("p8").Double(line.Quantity)
            .AddMember("p9").Utf8(line.DistInfo)
        .EndStruct();
    }

    auto params = listBuilder.EndList().Build().Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::Tx(tx),
        std::move(params));

    LOG_T("Terminal " << context.TerminalID << " waiting for order lines insert result");
    return result;
}

TString GetDistInfo(int districtID, const Stock& stock) {
    switch (districtID) {
        case 1: return stock.s_dist_01;
        case 2: return stock.s_dist_02;
        case 3: return stock.s_dist_03;
        case 4: return stock.s_dist_04;
        case 5: return stock.s_dist_05;
        case 6: return stock.s_dist_06;
        case 7: return stock.s_dist_07;
        case 8: return stock.s_dist_08;
        case 9: return stock.s_dist_09;
        case 10: return stock.s_dist_10;
        default: return TString(); // Return empty string for invalid district ID
    }
}

} // anonymous

//-----------------------------------------------------------------------------

NThreading::TFuture<TStatus> GetNewOrderTask(
    TTransactionContext& context,
    TDuration& latency,
    TSession session)
{
    TMonotonic startTs = TMonotonic::Now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    auto& Log = context.Log;

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    const int customerID = GetRandomCustomerID();

    LOG_T("Terminal " << context.TerminalID << " started NewOrder transaction in "
        << warehouseID << ", " << districtID << " for " << customerID << ", session: " << session.GetId());

    // Generate order line items

    const int numItems = RandomNumber(MIN_ITEMS, MAX_ITEMS);

    std::vector<int> itemIDs;
    std::vector<int> supplierWarehouseIDs;
    std::vector<int> orderQuantities;
    itemIDs.reserve(numItems);
    supplierWarehouseIDs.reserve(numItems);
    orderQuantities.reserve(numItems);
    int allLocal = 1;

    for (int i = 0; i < numItems; ++i) {
        itemIDs.push_back(GetRandomItemID());
        if (RandomNumber(1, 100) > 1) {
            supplierWarehouseIDs.push_back(warehouseID);
        } else {
            int supplierID;
            do {
                supplierID = RandomNumber(1, context.WarehouseCount);
            } while (supplierID == warehouseID && context.WarehouseCount > 1);
            supplierWarehouseIDs.push_back(supplierID);
            allLocal = 0;
        }
        orderQuantities.push_back(RandomNumber(1, 10));
    }

    // we need to cause 1% of the new orders to be rolled back.
    bool hasInvalidItem = false;
    if (RandomNumber(1, 100) == 1) {
        itemIDs[numItems - 1] = INVALID_ITEM_ID;
        hasInvalidItem = true;
    }

    // Get customer info

    auto customerFuture = GetCustomer(session, context, warehouseID, districtID, customerID);
    auto customerResult = co_await TSuspendWithFuture(customerFuture, context.TaskQueue, context.TerminalID);
    if (!customerResult.IsSuccess()) {
        if (ShouldExit(customerResult)) {
            LOG_E("Terminal " << context.TerminalID << " customer query failed: " << customerResult.GetStatus() << ", "
                << customerResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " customer query failed: " << customerResult.GetStatus() << ", "
            << customerResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return customerResult;
    }

    auto tx = *customerResult.GetTransaction();
    LOG_T("Terminal " << context.TerminalID << " NewOrder txId " << tx.GetId());

    // Get warehouse info

    auto warehouseFuture = GetWarehouseTax(session, tx, context, warehouseID);
    auto warehouseResult = co_await TSuspendWithFuture(warehouseFuture, context.TaskQueue, context.TerminalID);
    if (!warehouseResult.IsSuccess()) {
        if (ShouldExit(warehouseResult)) {
            LOG_E("Terminal " << context.TerminalID << " warehouse query failed: "
                << warehouseResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " warehouse query failed: "
            << warehouseResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return warehouseResult;
    }

    // Get district info and next order ID

    auto districtFuture = GetDistrict(session, tx, context, warehouseID, districtID);
    auto districtResult = co_await TSuspendWithFuture(districtFuture, context.TaskQueue, context.TerminalID);
    if (!districtResult.IsSuccess()) {
        if (ShouldExit(districtResult)) {
            LOG_E("Terminal " << context.TerminalID << " district query (neworder) failed: "
                << districtResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " district query (neworder) failed: "
            << districtResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return districtResult;
    }

    TResultSetParser districtParser(districtResult.GetResultSet(0));
    if (!districtParser.TryNextRow()) {
        LOG_E("Terminal " << context.TerminalID
            << ", warehouseId " << warehouseID << ", districtId " <<  districtID << " not found");
        RequestStop();
        co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
    }
    int nextOrderID = *districtParser.ColumnParser("D_NEXT_O_ID").GetOptionalInt32();

    // Update district with new next order ID

    auto updateDistrictFuture = UpdateDistrict(session, tx, context, warehouseID, districtID, nextOrderID + 1);
    auto updateDistrictResult = co_await TSuspendWithFuture(updateDistrictFuture, context.TaskQueue, context.TerminalID);
    if (!updateDistrictResult.IsSuccess()) {
        if (ShouldExit(updateDistrictResult)) {
            LOG_E("Terminal " << context.TerminalID << " district update failed: "
                << updateDistrictResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " district update failed: "
            << updateDistrictResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return updateDistrictResult;
    }

    // Insert new order

    auto newOrderFuture = InsertNewOrder(session, tx, context, nextOrderID, districtID, warehouseID);
    auto newOrderResult = co_await TSuspendWithFuture(newOrderFuture, context.TaskQueue, context.TerminalID);
    if (!newOrderResult.IsSuccess()) {
        if (ShouldExit(newOrderResult)) {
            LOG_E("Terminal " << context.TerminalID << " new order insert failed: "
                << newOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " new order insert failed: "
            << newOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return newOrderResult;
    }

    // Insert open order

    auto openOrderFuture = InsertOpenOrder(
        session, tx, context, nextOrderID, districtID, warehouseID, customerID, numItems, allLocal);
    auto openOrderResult = co_await TSuspendWithFuture(openOrderFuture, context.TaskQueue, context.TerminalID);
    if (!openOrderResult.IsSuccess()) {
        if (ShouldExit(openOrderResult)) {
            LOG_E("Terminal " << context.TerminalID << " open order insert failed: "
                << openOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " open order insert failed: "
            << openOrderResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return openOrderResult;
    }

    // Get item prices

    auto itemsFuture = GetItems(session, tx, context, itemIDs);
    auto itemsResult = co_await TSuspendWithFuture(itemsFuture, context.TaskQueue, context.TerminalID);
    if (!itemsResult.IsSuccess()) {
        if (ShouldExit(itemsResult)) {
            LOG_E("Terminal " << context.TerminalID << " items query failed: "
                << itemsResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " items query failed: "
            << itemsResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return itemsResult;
    }

    TResultSetParser itemsParser(itemsResult.GetResultSet(0));
    std::unordered_map<int, double> itemPrices;
    while (itemsParser.TryNextRow()) {
        int itemId = itemsParser.ColumnParser("I_ID").GetInt32();
        double price = *itemsParser.ColumnParser("I_PRICE").GetOptionalDouble();
        itemPrices[itemId] = price;
    }

    if (hasInvalidItem) {
        co_await tx.Rollback();
        throw TUserAbortedException();
    }

    // Get stock info

    std::vector<std::pair<int, int>> stockKeys;
    stockKeys.reserve(itemIDs.size());
    for (size_t i = 0; i < itemIDs.size(); ++i) {
        stockKeys.emplace_back(supplierWarehouseIDs[i], itemIDs[i]);
    }

    auto stocksFuture = GetStocks(session, tx, context, stockKeys);
    auto stocksResult = co_await TSuspendWithFuture(stocksFuture, context.TaskQueue, context.TerminalID);
    if (!stocksResult.IsSuccess()) {
        if (ShouldExit(stocksResult)) {
            LOG_E("Terminal " << context.TerminalID << " stocks query failed: "
                << stocksResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " stocks query failed: "
            << stocksResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return stocksResult;
    }

    TResultSetParser stocksParser(stocksResult.GetResultSet(0));
    std::unordered_map<std::pair<int, int>, Stock, TPairHash> stocks;
    while (stocksParser.TryNextRow()) {
        int w_id = stocksParser.ColumnParser("S_W_ID").GetInt32();
        int i_id = stocksParser.ColumnParser("S_I_ID").GetInt32();

        auto& stock = stocks[std::make_pair(w_id, i_id)];
        stock.s_w_id = w_id;
        stock.s_i_id = i_id;
        stock.s_quantity = *stocksParser.ColumnParser("S_QUANTITY").GetOptionalInt32();

        // for some reason it's empty in initial data from benchbase variant of TPC-C
        stock.s_ytd = stocksParser.ColumnParser("S_YTD").GetOptionalDouble().value_or(0.0);

        stock.s_order_cnt = *stocksParser.ColumnParser("S_ORDER_CNT").GetOptionalInt32();
        stock.s_remote_cnt = *stocksParser.ColumnParser("S_REMOTE_CNT").GetOptionalInt32();
        stock.s_dist_01 = *stocksParser.ColumnParser("S_DIST_01").GetOptionalUtf8();
        stock.s_dist_02 = *stocksParser.ColumnParser("S_DIST_02").GetOptionalUtf8();
        stock.s_dist_03 = *stocksParser.ColumnParser("S_DIST_03").GetOptionalUtf8();
        stock.s_dist_04 = *stocksParser.ColumnParser("S_DIST_04").GetOptionalUtf8();
        stock.s_dist_05 = *stocksParser.ColumnParser("S_DIST_05").GetOptionalUtf8();
        stock.s_dist_06 = *stocksParser.ColumnParser("S_DIST_06").GetOptionalUtf8();
        stock.s_dist_07 = *stocksParser.ColumnParser("S_DIST_07").GetOptionalUtf8();
        stock.s_dist_08 = *stocksParser.ColumnParser("S_DIST_08").GetOptionalUtf8();
        stock.s_dist_09 = *stocksParser.ColumnParser("S_DIST_09").GetOptionalUtf8();
        stock.s_dist_10 = *stocksParser.ColumnParser("S_DIST_10").GetOptionalUtf8();
    }

    // Process order lines and prepare updates

    std::vector<StockUpdate> stockUpdates;
    std::vector<OrderLine> orderLines;
    stockUpdates.reserve(numItems);
    orderLines.reserve(numItems);
    for (int ol_number = 1; ol_number <= numItems; ++ol_number) {
        int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
        int ol_i_id = itemIDs[ol_number - 1];
        int ol_quantity = orderQuantities[ol_number - 1];

        // Get item price
        auto itemIter = itemPrices.find(ol_i_id);
        if (itemIter == itemPrices.end()) {
            LOG_E("Terminal " << context.TerminalID << " item not found: " << ol_i_id);
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        double i_price = itemIter->second;
        double ol_amount = ol_quantity * i_price;

        // Get stock info
        auto stockIter = stocks.find(std::make_pair(ol_supply_w_id, ol_i_id));
        if (stockIter == stocks.end()) {
            LOG_E("Terminal " << context.TerminalID << " stock not found: W_ID=" << ol_supply_w_id
                    << ", I_ID=" << ol_i_id);
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        Stock& stock = stockIter->second;

        // Update stock quantity
        if (stock.s_quantity - ol_quantity >= 10) {
            stock.s_quantity -= ol_quantity;
        } else {
            stock.s_quantity += -ol_quantity + 91;
        }

        // Prepare stock update
        StockUpdate stockUpdate{
            stock.s_w_id,
            stock.s_i_id,
            stock.s_quantity,
            stock.s_ytd + ol_quantity,
            stock.s_order_cnt + 1,
            stock.s_remote_cnt + (ol_supply_w_id == warehouseID ? 0 : 1)
        };
        stockUpdates.push_back(stockUpdate);

        // Prepare order line
        OrderLine orderLine{
            warehouseID,
            districtID,
            nextOrderID,
            ol_number,
            ol_i_id,
            ol_amount,
            ol_supply_w_id,
            static_cast<double>(ol_quantity),
            GetDistInfo(districtID, stock)
        };
        orderLines.push_back(orderLine);
    }

    // Update stocks

    auto updateStocksFuture = UpdateStocks(session, tx, context, stockUpdates);
    auto updateStocksResult = co_await TSuspendWithFuture(updateStocksFuture, context.TaskQueue, context.TerminalID);
    if (!updateStocksResult.IsSuccess()) {
        if (ShouldExit(updateStocksResult)) {
            LOG_E("Terminal " << context.TerminalID << " stocks update failed: "
                << updateStocksResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " stocks update failed: "
            << updateStocksResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return updateStocksResult;
    }

    // Insert order lines

    auto orderLinesFuture = InsertOrderLines(session, tx, context, orderLines);
    auto orderLinesResult = co_await TSuspendWithFuture(orderLinesFuture, context.TaskQueue, context.TerminalID);
    if (!orderLinesResult.IsSuccess()) {
        if (ShouldExit(orderLinesResult)) {
            LOG_E("Terminal " << context.TerminalID << " order lines insert failed: "
                << orderLinesResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
            RequestStop();
            co_return TStatus(EStatus::CLIENT_INTERNAL_ERROR, NIssue::TIssues());
        }
        LOG_T("Terminal " << context.TerminalID << " order lines insert failed: "
            << orderLinesResult.GetIssues().ToOneLineString() << ", session: " << session.GetId());
        co_return orderLinesResult;
    }

    LOG_T("Terminal " << context.TerminalID << " is committing NewOrder transaction, session: " << session.GetId());

    auto commitFuture = tx.Commit();
    auto commitResult = co_await TSuspendWithFuture(commitFuture, context.TaskQueue, context.TerminalID);

    TMonotonic endTs = TMonotonic::Now();
    latency = endTs - startTs;

    co_return commitResult;
}

} // namespace NYdb::NTPCC
