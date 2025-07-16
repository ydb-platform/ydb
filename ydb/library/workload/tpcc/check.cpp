#include "check.h"

#include "constants.h"
#include "log.h"
#include "log_backend.h"
#include "util.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using namespace NYdb::NQuery;

//-----------------------------------------------------------------------------

void BaseCheckWarehouseTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // W_ID is PK, so we can take just min, max and count to check if all rows present
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count, MAX(`W_ID`) as max, MIN(`W_ID`) as min FROM `{}`;
    )", path.c_str(), TABLE_WAREHOUSE);

    TString fullPath = path + "/" + TABLE_WAREHOUSE;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max warehouses in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No warehouses found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero warehouses in " << fullPath << ": " << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max").GetOptionalInt32();
        if (int(rowCount) != expectedWhNumber || minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "Inconsistent table '" << fullPath << "' for " << expectedWhNumber
                 << " warehouses: minWh=" << minWh
                 << ", maxWh=" << maxWh
                 << ", whCount=" << rowCount;
            std::exit(1);
        }
    } catch (const std::exception& e) {
        Cerr << "Failed to count/min/max warehouses in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

//-----------------------------------------------------------------------------

void BaseCheckDistrictTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // D_W_ID, D_ID are part of PK, so we can check min/max district IDs and count
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`D_W_ID`) as max_w_id, MIN(`D_W_ID`) as min_w_id,
               MAX(`D_ID`) as max_d_id, MIN(`D_ID`) as min_d_id
        FROM `{}`;
    )", path.c_str(), TABLE_DISTRICT);

    TString fullPath = path + "/" + TABLE_DISTRICT;
    int expectedCount = expectedWhNumber * DISTRICT_HIGH_ID; // 10 districts per warehouse

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max districts in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No districts found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero districts in " << fullPath << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
        int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
        int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();

        if (int(rowCount) != expectedCount) {
            Cerr << "District count is " << rowCount << " and not " << expectedCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        if (minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "District warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                 << expectedWhNumber << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
            Cerr << "District ID range is [" << minDist << ", " << maxDist << "] instead of ["
                 << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate districts in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckCustomerTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // C_W_ID, C_D_ID, C_ID are part of PK
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`C_W_ID`) as max_w_id, MIN(`C_W_ID`) as min_w_id,
               MAX(`C_D_ID`) as max_d_id, MIN(`C_D_ID`) as min_d_id,
               MAX(`C_ID`) as max_c_id, MIN(`C_ID`) as min_c_id
        FROM `{}`;
    )", path.c_str(), TABLE_CUSTOMER);

    TString fullPath = path + "/" + TABLE_CUSTOMER;
    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_HIGH_ID;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max customers in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No customers found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero customers in " << fullPath << Endl;
            std::exit(1);
        }

        if (int(rowCount) != expectedCount) {
            Cerr << "Customer count is " << rowCount << " and not " << expectedCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
        int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
        int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();
        int minCust = *parser.ColumnParser("min_c_id").GetOptionalInt32();
        int maxCust = *parser.ColumnParser("max_c_id").GetOptionalInt32();

        if (minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "Customer warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                 << expectedWhNumber << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
            Cerr << "Customer district range is [" << minDist << ", " << maxDist << "] instead of ["
                 << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minCust != 1 || maxCust != CUSTOMERS_PER_DISTRICT) {
            Cerr << "Customer ID range is [" << minCust << ", " << maxCust << "] instead of [1, "
                 << CUSTOMERS_PER_DISTRICT << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate customers in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckItemTable(TQueryClient& client, const TString& path) {
    // I_ID is PK, fixed number of items regardless of warehouse count
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count, MAX(`I_ID`) as max, MIN(`I_ID`) as min FROM `{}`;
    )", path.c_str(), TABLE_ITEM);

    TString fullPath = path + "/" + TABLE_ITEM;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max items in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No items found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero items in " << fullPath << Endl;
            std::exit(1);
        }

        if (int(rowCount) != ITEM_COUNT) {
            Cerr << "Item count is " << rowCount << " and not " << ITEM_COUNT << " in " << fullPath << Endl;
            std::exit(1);
        }

        int minItem = *parser.ColumnParser("min").GetOptionalInt32();
        int maxItem = *parser.ColumnParser("max").GetOptionalInt32();
        if (minItem != 1 || maxItem != ITEM_COUNT) {
            Cerr << "Item ID range is [" << minItem << ", " << maxItem << "] instead of [1, "
                 << ITEM_COUNT << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate items in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckStockTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // S_W_ID, S_I_ID are part of PK - check warehouse and item ranges
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               COUNT(DISTINCT `S_W_ID`) as warehouse_count,
               MAX(`S_W_ID`) as max_w_id, MIN(`S_W_ID`) as min_w_id,
               MAX(`S_I_ID`) as max_i_id, MIN(`S_I_ID`) as min_i_id
        FROM `{}`;
    )", path.c_str(), TABLE_STOCK);

    TString fullPath = path + "/" + TABLE_STOCK;
    int expectedCount = expectedWhNumber * ITEM_COUNT; // 100000 items per warehouse

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max stock in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No stock found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        size_t warehouseCount = parser.ColumnParser("warehouse_count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero stock in " << fullPath << Endl;
            std::exit(1);
        }
        if (int(rowCount) != expectedCount) {
            Cerr << "Stock count is " << rowCount << " and not " << expectedCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
        int minItem = *parser.ColumnParser("min_i_id").GetOptionalInt32();
        int maxItem = *parser.ColumnParser("max_i_id").GetOptionalInt32();

        if (int(warehouseCount) != expectedWhNumber) {
            Cerr << "Stock warehouse count is " << warehouseCount << " and not "
                 << expectedWhNumber << " in " << fullPath << Endl;
            std::exit(1);
        }

        if (minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "Stock warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                 << expectedWhNumber << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minItem != 1 || maxItem != ITEM_COUNT) {
            Cerr << "Stock item range is [" << minItem << ", " << maxItem << "] instead of [1, "
                 << ITEM_COUNT << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate stock in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckOorderTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // O_W_ID, O_D_ID, O_ID are part of PK
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`O_W_ID`) as max_w_id, MIN(`O_W_ID`) as min_w_id,
               MAX(`O_D_ID`) as max_d_id, MIN(`O_D_ID`) as min_d_id,
               MAX(`O_ID`) as max_o_id, MIN(`O_ID`) as min_o_id
        FROM `{}`;
    )", path.c_str(), TABLE_OORDER);

    TString fullPath = path + "/" + TABLE_OORDER;
    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_HIGH_ID;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max orders in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No orders found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero orders in " << fullPath << Endl;
            std::exit(1);
        }
        if (int(rowCount) != expectedCount) {
            Cerr << "Order count is " << rowCount << " and not " << expectedCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
        int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
        int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();
        int minOrder = *parser.ColumnParser("min_o_id").GetOptionalInt32();
        int maxOrder = *parser.ColumnParser("max_o_id").GetOptionalInt32();

        if (minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "Order warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                 << expectedWhNumber << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
            Cerr << "Order district range is [" << minDist << ", " << maxDist << "] instead of ["
                 << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minOrder != 1 || maxOrder != CUSTOMERS_PER_DISTRICT) {
            Cerr << "Order ID range is [" << minOrder << ", " << maxOrder << "] instead of [1, "
                 << CUSTOMERS_PER_DISTRICT << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate orders in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckNewOrderTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // NO_W_ID, NO_D_ID, NO_O_ID are part of PK
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`NO_W_ID`) as max_w_id, MIN(`NO_W_ID`) as min_w_id,
               MAX(`NO_D_ID`) as max_d_id, MIN(`NO_D_ID`) as min_d_id,
               MAX(`NO_O_ID`) as max_o_id, MIN(`NO_O_ID`) as min_o_id
        FROM `{}`;
    )", path.c_str(), TABLE_NEW_ORDER);

    const auto newOrdersPerDistrict = CUSTOMERS_PER_DISTRICT - FIRST_UNPROCESSED_O_ID + 1;

    TString fullPath = path + "/" + TABLE_NEW_ORDER;
    int expectedCount = expectedWhNumber * newOrdersPerDistrict * DISTRICT_HIGH_ID;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max new orders in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No new orders found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero new orders in " << fullPath << Endl;
            std::exit(1);
        }

        if (int(rowCount) != expectedCount) {
            Cerr << "New order count is " << rowCount << " and not " << expectedCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
        int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
        int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();
        int minOrder = *parser.ColumnParser("min_o_id").GetOptionalInt32();
        int maxOrder = *parser.ColumnParser("max_o_id").GetOptionalInt32();

        if (minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "New order warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                 << expectedWhNumber << "] in " << fullPath << Endl;
            std::exit(1);
        }

        if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
            Cerr << "New order district range is [" << minDist << ", " << maxDist << "] instead of ["
                 << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath << Endl;
            std::exit(1);
        }

        // New orders are for orders 2101-3000, so minimum should be FIRST_UNPROCESSED_O_ID
        if (minOrder < FIRST_UNPROCESSED_O_ID || maxOrder != CUSTOMERS_PER_DISTRICT) {
            Cerr << "New order ID range is [" << minOrder << ", " << maxOrder << "] instead of ["
                 << FIRST_UNPROCESSED_O_ID << ", " << CUSTOMERS_PER_DISTRICT << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate new orders in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckOrderLineTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER are part of PK
    // Check that each district has orders for all order IDs 1-3000
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        $per_district = (
            SELECT OL_W_ID, OL_D_ID, COUNT(DISTINCT OL_O_ID) as order_count
            FROM `{}`
            GROUP BY OL_W_ID, OL_D_ID
        );

        SELECT MIN(order_count) as min_orders, MAX(order_count) as max_orders,
               COUNT(*) as district_count
        FROM $per_district;
    )", path.c_str(), TABLE_ORDER_LINE);

    TString fullPath = path + "/" + TABLE_ORDER_LINE;
    int expectedDistrictCount = expectedWhNumber * DISTRICT_HIGH_ID;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to check order lines per district in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No order line districts found" << Endl;
        std::exit(1);
    }

    try {
        size_t districtCount = parser.ColumnParser("district_count").GetUint64();
        if (int(districtCount) != expectedDistrictCount) {
            Cerr << "Order line district count is " << districtCount << " and not "
                 << expectedDistrictCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        size_t minOrders = *parser.ColumnParser("min_orders").GetOptionalUint64();
        size_t maxOrders = *parser.ColumnParser("max_orders").GetOptionalUint64();

        if (minOrders != CUSTOMERS_PER_DISTRICT || maxOrders != CUSTOMERS_PER_DISTRICT) {
            Cerr << "Order line order count per district is [" << minOrders << ", " << maxOrders << "] instead of ["
                 << CUSTOMERS_PER_DISTRICT << ", " << CUSTOMERS_PER_DISTRICT << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate order lines in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

void BaseCheckHistoryTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // H_C_W_ID, H_C_NANO_TS are part of PK
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`H_C_W_ID`) as max_w_id, MIN(`H_C_W_ID`) as min_w_id
        FROM `{}`;
    )", path.c_str(), TABLE_HISTORY);

    TString fullPath = path + "/" + TABLE_HISTORY;
    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_HIGH_ID;

    auto result = client.RetryQuery([&](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    ExitIfError(result, TStringBuilder() << "Failed to count/min/max history in " << fullPath);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        Cerr << "No history found" << Endl;
        std::exit(1);
    }

    try {
        size_t rowCount = parser.ColumnParser("count").GetUint64();
        if (rowCount == 0) {
            Cerr << "Zero history records in " << fullPath << Endl;
            std::exit(1);
        }

        int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
        int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();

        if (int(rowCount) != expectedCount) {
            Cerr << "History count is " << rowCount << " and not " << expectedCount << " in " << fullPath << Endl;
            std::exit(1);
        }

        if (minWh != 1 || maxWh != expectedWhNumber) {
            Cerr << "History warehouse range is [" << minWh << ", " << maxWh
                 << "] instead of [1, " << expectedWhNumber << "] in " << fullPath << Endl;
            std::exit(1);
        }
    } catch (std::exception& e) {
        Cerr << "Failed to validate history in " << fullPath << ": " << e.what() << Endl;
        std::exit(1);
    }
}

//-----------------------------------------------------------------------------

class TPCCChecker {
public:
    TPCCChecker(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig)
        : ConnectionConfig(connectionConfig)
        , Config(runConfig)
        , LogBackend(new TLogBackendWithCapture("cerr", runConfig.LogPriority, TUI_LOG_LINES))
        , Log(std::make_unique<TLog>(THolder(static_cast<TLogBackend*>(LogBackend))))
    {
    }

    void CheckSync();

private:
    void BaseCheck(TQueryClient& client);

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;

    // XXX Log instance owns LogBackend (unfortunately, it accepts THolder with LogBackend)
    TLogBackendWithCapture* LogBackend;
    std::unique_ptr<TLog> Log;
};

void TPCCChecker::CheckSync() {
    auto connectionConfigCopy = ConnectionConfig;
    TDriver driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);
    TQueryClient queryClient(driver);

    BaseCheck(queryClient);

    // to flush
    LogBackend->ReopenLog();

    Cout << "Everything is good!" << Endl;

    driver.Stop(true);
}

void TPCCChecker::BaseCheck(TQueryClient& client) {
    const auto& path = Config.Path;
    auto expectedWhNumber = Config.WarehouseCount;
    if (expectedWhNumber == 0) {
        Cerr << "Zero warehouses specified, nothing to check" << Endl;
        std::exit(1);
    }

    LOG_I("Checking " << TABLE_WAREHOUSE << "...");
    BaseCheckWarehouseTable(client, path, expectedWhNumber);
    LOG_I(TABLE_WAREHOUSE << " is OK");

    LOG_I("Checking " << TABLE_DISTRICT << "...");
    BaseCheckDistrictTable(client, path, expectedWhNumber);
    LOG_I(TABLE_DISTRICT << " is OK");

    LOG_I("Checking " << TABLE_CUSTOMER << "...");
    BaseCheckCustomerTable(client, path, expectedWhNumber);
    LOG_I(TABLE_CUSTOMER << " is OK");

    LOG_I("Checking " << TABLE_ITEM << "...");
    BaseCheckItemTable(client, path);
    LOG_I(TABLE_ITEM << " is OK");

    if (Config.JustImported) {
        LOG_I("Checking " << TABLE_STOCK << "...");
        BaseCheckStockTable(client, path, expectedWhNumber);
        LOG_I(TABLE_STOCK << " is OK");

        LOG_I("Checking " << TABLE_OORDER << "...");
        BaseCheckOorderTable(client, path, expectedWhNumber);
        LOG_I(TABLE_OORDER << " is OK");

        LOG_I("Checking " << TABLE_NEW_ORDER << "...");
        BaseCheckNewOrderTable(client, path, expectedWhNumber);
        LOG_I(TABLE_NEW_ORDER << " is OK");

        LOG_I("Checking " << TABLE_ORDER_LINE << "...");
        BaseCheckOrderLineTable(client, path, expectedWhNumber);
        LOG_I(TABLE_ORDER_LINE << " is OK");

        LOG_I("Checking " << TABLE_HISTORY << "...");
        BaseCheckHistoryTable(client, path, expectedWhNumber);
        LOG_I(TABLE_HISTORY << " is OK");
    }
}

} // anonymous

//-----------------------------------------------------------------------------

void CheckSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    TPCCChecker checker(connectionConfig, runConfig);
    checker.CheckSync();
}

} // namespace NYdb::NTPCC
