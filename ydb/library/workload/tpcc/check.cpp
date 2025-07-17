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
using namespace NThreading;

//-----------------------------------------------------------------------------

TFuture<void> BaseCheckWarehouseTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // W_ID is PK, so we can take just min, max and count to check if all rows present
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count, MAX(`W_ID`) as max, MIN(`W_ID`) as min FROM `{}`;
    )", path.c_str(), TABLE_WAREHOUSE);

    TString fullPath = path + "/" + TABLE_WAREHOUSE;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max warehouses in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No warehouses found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero warehouses in " << fullPath << ": ";
            }

            int minWh = *parser.ColumnParser("min").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max").GetOptionalInt32();
            if (int(rowCount) != expectedWhNumber || minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "Inconsistent table '" << fullPath << "' for " << expectedWhNumber
                    << " warehouses: minWh=" << minWh
                    << ", maxWh=" << maxWh
                    << ", whCount=" << rowCount;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to count/min/max warehouses in " << fullPath << ": " << e.what();
        }
    });
}

//-----------------------------------------------------------------------------

TFuture<void> BaseCheckDistrictTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // D_W_ID, D_ID are part of PK, so we can check min/max district IDs and count
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`D_W_ID`) as max_w_id, MIN(`D_W_ID`) as min_w_id,
               MAX(`D_ID`) as max_d_id, MIN(`D_ID`) as min_d_id
        FROM `{}`;
    )", path.c_str(), TABLE_DISTRICT);

    TString fullPath = path + "/" + TABLE_DISTRICT;
    int expectedCount = expectedWhNumber * DISTRICT_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedCount, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max districts in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No districts found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero districts in " << fullPath;
            }

            if (int(rowCount) != expectedCount) {
                ythrow yexception() << "District count is " << rowCount << " and not " << expectedCount << " in " << fullPath;
            }

            int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
            int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
            int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();

            if (minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "District warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                     << expectedWhNumber << "] in " << fullPath;
            }

            if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
                ythrow yexception() << "District ID range is [" << minDist << ", " << maxDist << "] instead of ["
                     << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate districts in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckCustomerTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
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
    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedCount, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max customers in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No customers found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero customers in " << fullPath;
            }

            if (int(rowCount) != expectedCount) {
                ythrow yexception() << "Customer count is " << rowCount << " and not " << expectedCount << " in " << fullPath;
            }

            int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
            int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
            int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();
            int minCust = *parser.ColumnParser("min_c_id").GetOptionalInt32();
            int maxCust = *parser.ColumnParser("max_c_id").GetOptionalInt32();

            if (minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "Customer warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                     << expectedWhNumber << "] in " << fullPath;
            }

            if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
                ythrow yexception() << "Customer district range is [" << minDist << ", " << maxDist << "] instead of ["
                     << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath;
            }

            if (minCust != 1 || maxCust != CUSTOMERS_PER_DISTRICT) {
                ythrow yexception() << "Customer ID range is [" << minCust << ", " << maxCust << "] instead of [1, "
                     << CUSTOMERS_PER_DISTRICT << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate customers in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckItemTable(TQueryClient& client, const TString& path) {
    // I_ID is PK, fixed number of items regardless of warehouse count
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count, MAX(`I_ID`) as max, MIN(`I_ID`) as min FROM `{}`;
    )", path.c_str(), TABLE_ITEM);

    TString fullPath = path + "/" + TABLE_ITEM;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max items in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No items found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero items in " << fullPath;
            }

            if (int(rowCount) != ITEM_COUNT) {
                ythrow yexception() << "Item count is " << rowCount << " and not " << ITEM_COUNT << " in " << fullPath;
            }

            int minItem = *parser.ColumnParser("min").GetOptionalInt32();
            int maxItem = *parser.ColumnParser("max").GetOptionalInt32();
            if (minItem != 1 || maxItem != ITEM_COUNT) {
                ythrow yexception() << "Item ID range is [" << minItem << ", " << maxItem << "] instead of [1, "
                     << ITEM_COUNT << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate items in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckStockTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
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
    int expectedCount = expectedWhNumber * ITEM_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedCount, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max stock in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No stock found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            size_t warehouseCount = parser.ColumnParser("warehouse_count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero stock in " << fullPath;
            }

            if (int(rowCount) != expectedCount) {
                ythrow yexception() << "Stock count is " << rowCount << " and not " << expectedCount << " in " << fullPath;
            }

            int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
            int minItem = *parser.ColumnParser("min_i_id").GetOptionalInt32();
            int maxItem = *parser.ColumnParser("max_i_id").GetOptionalInt32();

            if (int(warehouseCount) != expectedWhNumber) {
                ythrow yexception() << "Stock warehouse count is " << warehouseCount << " and not "
                     << expectedWhNumber << " in " << fullPath;
            }

            if (minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "Stock warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                     << expectedWhNumber << "] in " << fullPath;
            }

            if (minItem != 1 || maxItem != ITEM_COUNT) {
                ythrow yexception() << "Stock item range is [" << minItem << ", " << maxItem << "] instead of [1, "
                     << ITEM_COUNT << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate stock in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckOorderTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
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
    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedCount, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max orders in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No orders found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero orders in " << fullPath;
            }

            if (int(rowCount) != expectedCount) {
                ythrow yexception() << "Order count is " << rowCount << " and not " << expectedCount << " in " << fullPath;
            }

            int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
            int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
            int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();
            int minOrder = *parser.ColumnParser("min_o_id").GetOptionalInt32();
            int maxOrder = *parser.ColumnParser("max_o_id").GetOptionalInt32();

            if (minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "Order warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                     << expectedWhNumber << "] in " << fullPath;
            }

            if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
                ythrow yexception() << "Order district range is [" << minDist << ", " << maxDist << "] instead of ["
                     << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath;
            }

            if (minOrder != 1 || maxOrder != CUSTOMERS_PER_DISTRICT) {
                ythrow yexception() << "Order ID range is [" << minOrder << ", " << maxOrder << "] instead of [1, "
                     << CUSTOMERS_PER_DISTRICT << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate orders in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckNewOrderTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
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
    int expectedCount = expectedWhNumber * newOrdersPerDistrict * DISTRICT_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedCount, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max new orders in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No new orders found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero new orders in " << fullPath;
            }

            if (int(rowCount) != expectedCount) {
                ythrow yexception() << "New order count is " << rowCount << " and not " << expectedCount << " in " << fullPath;
            }

            int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();
            int minDist = *parser.ColumnParser("min_d_id").GetOptionalInt32();
            int maxDist = *parser.ColumnParser("max_d_id").GetOptionalInt32();
            int minOrder = *parser.ColumnParser("min_o_id").GetOptionalInt32();
            int maxOrder = *parser.ColumnParser("max_o_id").GetOptionalInt32();

            if (minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "New order warehouse range is [" << minWh << ", " << maxWh << "] instead of [1, "
                     << expectedWhNumber << "] in " << fullPath;
            }

            if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID) {
                ythrow yexception() << "New order district range is [" << minDist << ", " << maxDist << "] instead of ["
                     << DISTRICT_LOW_ID << ", " << DISTRICT_HIGH_ID << "] in " << fullPath;
            }

            // New orders are for orders 2101-3000, so minimum should be FIRST_UNPROCESSED_O_ID
            if (minOrder < FIRST_UNPROCESSED_O_ID || maxOrder != CUSTOMERS_PER_DISTRICT) {
                ythrow yexception() << "New order ID range is [" << minOrder << ", " << maxOrder << "] instead of ["
                     << FIRST_UNPROCESSED_O_ID << ", " << CUSTOMERS_PER_DISTRICT << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate new orders in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckOrderLineTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
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
    int expectedDistrictCount = expectedWhNumber * DISTRICT_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedDistrictCount](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to check order lines per district in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No order line districts found";
        }

        try {
            size_t districtCount = parser.ColumnParser("district_count").GetUint64();
            if (int(districtCount) != expectedDistrictCount) {
                ythrow yexception() << "Order line district count is " << districtCount << " and not "
                     << expectedDistrictCount << " in " << fullPath;
            }

            size_t minOrders = *parser.ColumnParser("min_orders").GetOptionalUint64();
            size_t maxOrders = *parser.ColumnParser("max_orders").GetOptionalUint64();

            if (minOrders != CUSTOMERS_PER_DISTRICT || maxOrders != CUSTOMERS_PER_DISTRICT) {
                ythrow yexception() << "Order line order count per district is [" << minOrders << ", " << maxOrders << "] instead of ["
                     << CUSTOMERS_PER_DISTRICT << ", " << CUSTOMERS_PER_DISTRICT << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate order lines in " << fullPath << ": " << e.what();
        }
    });
}

TFuture<void> BaseCheckHistoryTable(TQueryClient& client, const TString& path, int expectedWhNumber) {
    // H_C_W_ID, H_C_NANO_TS are part of PK
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) as count,
               MAX(`H_C_W_ID`) as max_w_id, MIN(`H_C_W_ID`) as min_w_id
        FROM `{}`;
    )", path.c_str(), TABLE_HISTORY);

    TString fullPath = path + "/" + TABLE_HISTORY;
    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_COUNT;

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([fullPath, expectedCount, expectedWhNumber](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Failed to count/min/max history in " << fullPath);

        TResultSetParser parser(result.GetResultSet(0));
        if (!parser.TryNextRow()) {
            ythrow yexception() << "No history found";
        }

        try {
            size_t rowCount = parser.ColumnParser("count").GetUint64();
            if (rowCount == 0) {
                ythrow yexception() << "Zero history records in " << fullPath;
            }

            if (int(rowCount) != expectedCount) {
                ythrow yexception() << "History count is " << rowCount << " and not " << expectedCount << " in " << fullPath;
            }

            int minWh = *parser.ColumnParser("min_w_id").GetOptionalInt32();
            int maxWh = *parser.ColumnParser("max_w_id").GetOptionalInt32();

            if (minWh != 1 || maxWh != expectedWhNumber) {
                ythrow yexception() << "History warehouse range is [" << minWh << ", " << maxWh
                     << "] instead of [1, " << expectedWhNumber << "] in " << fullPath;
            }
        } catch (const std::exception& e) {
            ythrow yexception() << "Failed to validate history in " << fullPath << ": " << e.what();
        }
    });
}

//-----------------------------------------------------------------------------

// based on checks in TPC-C for CockroachDB

TFuture<void> ConsistencyCheck3321(TQueryClient& client, const TString& path) {
	// 3.3.2.1 Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// W_YTD = sum (D_YTD)

    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        $districtData = SELECT
            D_W_ID, sum(D_YTD) AS sum_d_ytd
        FROM
            `{}`
        GROUP BY
            D_W_ID;

        SELECT w.W_ID as w_id, w.W_YTD as w_ytd, d.sum_d_ytd as sum_d_ytd
        FROM `{}` as w
        FULL JOIN $districtData as d on w.W_ID = d.D_W_ID
        WHERE ABS(w.W_YTD - d.sum_d_ytd) > 1e-6
        LIMIT 1;
    )", path.c_str(), TABLE_DISTRICT, TABLE_WAREHOUSE);

    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "Check 3.3.2.1 failed");

        TResultSetParser parser(result.GetResultSet(0));
        if (parser.TryNextRow()) {
            ythrow yexception() << "Check 3.3.2.1 failed: D_W_ID and sum(D_YTD) mismatch";
        }
    });
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
    void WaitCheck(const TFuture<void>& future, const std::string& description);

    void BaseCheck(TQueryClient& client);
    void ConsistencyCheck(TQueryClient& client);

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;

    // XXX Log instance owns LogBackend (unfortunately, it accepts THolder with LogBackend)
    TLogBackendWithCapture* LogBackend;
    std::unique_ptr<TLog> Log;

    int FailedChecksCount = 0;
    std::vector<std::pair<TFuture<void>, std::string>> RunningChecks;
};

void TPCCChecker::CheckSync() {
    auto connectionConfigCopy = ConnectionConfig;
    TDriver driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);
    TQueryClient queryClient(driver);

    BaseCheck(queryClient);
    ConsistencyCheck(queryClient);

    for (auto& [future, description]: RunningChecks) {
        WaitCheck(future, description);
    }

    // to flush
    LogBackend->ReopenLog();

    if (FailedChecksCount == 0) {
        Cout << "Everything is good!" << Endl;
    }

    driver.Stop(true);
}

void TPCCChecker::WaitCheck(const TFuture<void>& future, const std::string& description) {
    Cout << "Checking " << description << " ";
    Flush(Cout);
    try {
        future.GetValueSync();
        Cout << "[" << NColorizer::StdOut().GreenColor() << "OK" << NColorizer::StdOut().Default() << "]";
    } catch (const std::exception& ex) {
        Cout << "[" << NColorizer::StdOut().RedColor() << "Failed" << NColorizer::StdOut().Default() << "]: "
            << ex.what();
        ++FailedChecksCount;
    }
    Cout << Endl;
}

void TPCCChecker::BaseCheck(TQueryClient& client) {
    const auto& path = Config.Path;
    auto expectedWhNumber = Config.WarehouseCount;
    if (expectedWhNumber == 0) {
        Cerr << "Zero warehouses specified, nothing to check" << Endl;
        std::exit(1);
    }

    RunningChecks.insert(RunningChecks.end(), {
        { BaseCheckWarehouseTable(client, path, expectedWhNumber), TABLE_WAREHOUSE },
        { BaseCheckDistrictTable(client, path, expectedWhNumber), TABLE_DISTRICT },
        { BaseCheckCustomerTable(client, path, expectedWhNumber), TABLE_CUSTOMER },
        { BaseCheckItemTable(client, path), TABLE_ITEM },
        { BaseCheckStockTable(client, path, expectedWhNumber), TABLE_STOCK },
    });

    if (Config.JustImported) {
        RunningChecks.insert(RunningChecks.end(), {
            { BaseCheckOorderTable(client, path, expectedWhNumber), TABLE_OORDER },
            { BaseCheckNewOrderTable(client, path, expectedWhNumber), TABLE_NEW_ORDER },
            { BaseCheckOrderLineTable(client, path, expectedWhNumber), TABLE_ORDER_LINE },
            { BaseCheckHistoryTable(client, path, expectedWhNumber), TABLE_HISTORY },
        });
    }
}

void TPCCChecker::ConsistencyCheck(TQueryClient& client) {
    RunningChecks.insert(RunningChecks.end(), {
        { ConsistencyCheck3321(client, Config.Path), "3.3.2.1" },
    });
}

} // anonymous

//-----------------------------------------------------------------------------

void CheckSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    TPCCChecker checker(connectionConfig, runConfig);
    checker.CheckSync();
}

} // namespace NYdb::NTPCC
