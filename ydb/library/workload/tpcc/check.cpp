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

// waits all futures and returns either the one with exception,
// or void future.
TFuture<void> WaitAllAndCheck(const std::vector<TFuture<void>>& futures) {
    auto result = WaitAll(futures).Apply([allFutures = std::move(futures)](const auto&) {
        // return any with error
        for (const auto& future: allFutures) {
            if (future.HasException()) {
                return future;
            }
        }

        return MakeFuture();
    });

    return result;
}

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
                ythrow yexception() << "District count is " << rowCount << " and not "
                    << expectedCount << " in " << fullPath;
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
                ythrow yexception() << "Customer count is " << rowCount << " and not "
                    << expectedCount << " in " << fullPath;
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
                ythrow yexception() << "Stock count is " << rowCount << " and not "
                    << expectedCount << " in " << fullPath;
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
                ythrow yexception() << "Order count is " << rowCount << " and not "
                    << expectedCount << " in " << fullPath;
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
                ythrow yexception() << "New order count is " << rowCount << " and not "
                    << expectedCount << " in " << fullPath;
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
                ythrow yexception() << "Order line order count per district is ["
                     << minOrders << ", " << maxOrders << "] instead of ["
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
                ythrow yexception() << "History count is " << rowCount << " and not "
                    << expectedCount << " in " << fullPath;
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

TFuture<void> CheckNoRows(TQueryClient& client, const TString& query, const TString& description = {}) {
    return client.RetryQuery([query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).Apply([description](const TFuture<TExecuteQueryResult>& future) {
        auto result = future.GetValueSync();
        ThrowIfError(result, TStringBuilder() << "query failed");

        TResultSetParser parser(result.GetResultSet(0));
        if (parser.TryNextRow()) {
            if (!description.empty()) {
                ythrow yexception() << description;
            }
            ythrow yexception();
        }
    });
}

TFuture<void> ConsistencyCheck3321(TQueryClient& client, const TString& path) {
	// 3.3.2.1 Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// W_YTD = sum (D_YTD)

    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        $districtData = SELECT D_W_ID, sum(D_YTD) AS sum_d_ytd
        FROM `{}`
        GROUP BY D_W_ID;

        SELECT w.W_ID as w_id, w.W_YTD as w_ytd, d.sum_d_ytd as sum_d_ytd
        FROM `{}` as w
        FULL JOIN $districtData as d on w.W_ID = d.D_W_ID
        WHERE ABS(w.W_YTD - d.sum_d_ytd) > 1e-3
        LIMIT 1;
    )", path.c_str(), TABLE_DISTRICT, TABLE_WAREHOUSE);

    return CheckNoRows(client, query);
}

TFuture<void> ConsistencyCheck3322(TQueryClient& client, const TString& path) {
	// Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)

    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        $district_data = SELECT D_W_ID, D_ID, D_NEXT_O_ID
        FROM `{}`
        ORDER BY D_W_ID, D_ID;

        $order_data = SELECT O_W_ID, O_D_ID, MAX(O_ID) as max_o_id
        FROM `{}`
        GROUP BY O_W_ID, O_D_ID
        ORDER BY O_W_ID, O_D_ID;

        $new_order_data = SELECT NO_W_ID, NO_D_ID, MAX(NO_O_ID) as max_no_o_id
        FROM `{}`
        GROUP BY NO_W_ID, NO_D_ID
        ORDER BY NO_W_ID, NO_D_ID;

        SELECT * FROM $district_data as d
        LEFT JOIN $order_data as o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID
        LEFT JOIN $new_order_data as no ON d.D_W_ID = no.NO_W_ID AND d.D_ID = no.NO_D_ID
        WHERE (d.D_NEXT_O_ID - 1) != o.max_o_id OR o.max_o_id != no.max_no_o_id
        LIMIT 1;
    )", path.c_str(), TABLE_DISTRICT, TABLE_OORDER, TABLE_NEW_ORDER);

    return CheckNoRows(client, query);
}

TFuture<void> ConsistencyCheck3323(TQueryClient& client, const TString& path) {
	// max(NO_O_ID) - min(NO_O_ID) + 1 = # of rows in new_order for each warehouse/district

    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        $aggregation = SELECT
            NO_W_ID, NO_D_ID, (count(*) - (max(NO_O_ID) - min(NO_O_ID) + 1)) as delta
        FROM
            `{}`
        GROUP BY
            NO_W_ID, NO_D_ID;

        SELECT delta from $aggregation WHERE delta != 0 LIMIT 1;
    )", path.c_str(), TABLE_NEW_ORDER);

    return CheckNoRows(client, query);
}

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
    void ConsistencyCheckPart1(TQueryClient& client);
    void ConsistencyCheckPart2(TQueryClient& client);
    void ConsistencyCheck3324(TQueryClient& client);
    void ConsistencyCheck3325(TQueryClient& client);
    void ConsistencyCheck3326(TQueryClient& client);
    void ConsistencyCheck3327(TQueryClient& client);
    void ConsistencyCheck3328(TQueryClient& client);
    void ConsistencyCheck3329(TQueryClient& client);
    void ConsistencyCheck33210(TQueryClient& client);
    void ConsistencyCheck33211(TQueryClient& client);
    void ConsistencyCheck33212(TQueryClient& client);

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

    // Each member starts multiple async checks. To evenly load the cluster we
    // split checks into such "batches" and execute batch-by-batch
    std::vector<void (TPCCChecker::*)(TQueryClient&)> checkFunctions = {
        &TPCCChecker::BaseCheck,
        &TPCCChecker::ConsistencyCheckPart1,
        &TPCCChecker::ConsistencyCheckPart2,
        &TPCCChecker::ConsistencyCheck3324,
        &TPCCChecker::ConsistencyCheck3325,
        &TPCCChecker::ConsistencyCheck3326,
        //&TPCCChecker::ConsistencyCheck3327, //mem
        &TPCCChecker::ConsistencyCheck3328,
        &TPCCChecker::ConsistencyCheck3329,
        //&TPCCChecker::ConsistencyCheck33210, // mem
    };

    if (Config.JustImported) {
        checkFunctions.insert(checkFunctions.end(), &TPCCChecker::ConsistencyCheck33211);
    }

    // checkFunctions.insert(checkFunctions.end(), &TPCCChecker::ConsistencyCheck33212); // mem

    for (auto& checkFunction : checkFunctions) {
        (this->*checkFunction)(queryClient);

        for (auto& [future, description]: RunningChecks) {
            WaitCheck(future, description);
        }

        if (FailedChecksCount != 0) {
            Cout << "Some checks failed, aborting!" << Endl;
            driver.Stop(true);
            std::exit(1);
        }

        RunningChecks.clear();
    }

    // to flush any pending logs
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

void TPCCChecker::ConsistencyCheckPart1(TQueryClient& client) {
    RunningChecks.insert(RunningChecks.end(), {
        { ConsistencyCheck3321(client, Config.Path), "3.3.2.1" },
        { ConsistencyCheck3322(client, Config.Path), "3.3.2.2" },
    });
}

void TPCCChecker::ConsistencyCheckPart2(TQueryClient& client) {
    RunningChecks.insert(RunningChecks.end(), {
        { ConsistencyCheck3323(client, Config.Path), "3.3.2.3" },
    });
}

void TPCCChecker::ConsistencyCheck3324(TQueryClient& client) {
    // sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]

    const int WAREHOUSE_RANGE_SIZE = 1000;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $order_data = SELECT O_W_ID, O_D_ID, SUM(O_OL_CNT) as sum_ol_cnt
            FROM `{}`
            WHERE O_W_ID >= {} AND O_W_ID <= {}
            GROUP BY O_W_ID, O_D_ID
            ORDER BY O_W_ID, O_D_ID;

            $order_line_data = SELECT OL_W_ID, OL_D_ID, COUNT(*) as ol_count
            FROM `{}`
            WHERE OL_W_ID >= {} AND OL_W_ID <= {}
            GROUP BY OL_W_ID, OL_D_ID
            ORDER BY OL_W_ID, OL_D_ID;

            SELECT * FROM $order_data as o
            FULL JOIN $order_line_data as ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID
            WHERE o.sum_ol_cnt != ol.ol_count
            LIMIT 1;
        )", Config.Path.c_str(), TABLE_OORDER, startWh, endWh, TABLE_ORDER_LINE, startWh, endWh);

        // because of #21490 we run queries 1 by one
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.insert(RunningChecks.end(), {
        { result, "3.3.2.4" },
    });
}

void TPCCChecker::ConsistencyCheck3325(TQueryClient& client) {
    const int WAREHOUSE_RANGE_SIZE = 250;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $warehouse_from = {};
            $warehouse_to = {};

            $missing_in_order =
            SELECT no.NO_W_ID AS W_ID, no.NO_D_ID AS D_ID, no.NO_O_ID AS O_ID, o.O_W_ID as O_W_ID, o.O_CARRIER_ID as CID
            FROM `{}` AS no
            LEFT JOIN (
                SELECT O_W_ID as O_W_ID, O_D_ID as O_D_ID, O_ID as O_ID, O_CARRIER_ID as O_CARRIER_ID FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            ON no.NO_W_ID = o.O_W_ID AND no.NO_D_ID = o.O_D_ID AND no.NO_O_ID = o.O_ID
            WHERE no.NO_W_ID >= $warehouse_from AND no.NO_W_ID <= $warehouse_to
            AND (o.O_W_ID IS NULL OR (o.O_CARRIER_ID IS NOT NULL AND o.O_CARRIER_ID != 0))
            LIMIT 1;

            $missing_in_new_order =
            SELECT o.O_W_ID AS W_ID, o.O_D_ID AS D_ID, o.O_ID AS O_ID
            FROM (
                SELECT * FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            LEFT JOIN (
                SELECT * FROM `{}`
                WHERE NO_W_ID >= $warehouse_from AND NO_W_ID <= $warehouse_to
            ) AS no
            ON o.O_W_ID = no.NO_W_ID AND o.O_D_ID = no.NO_D_ID AND o.O_ID = no.NO_O_ID
            WHERE (o.O_CARRIER_ID IS NULL OR o.O_CARRIER_ID == 0) AND no.NO_W_ID IS NULL
            LIMIT 1;

            SELECT *
            FROM $missing_in_order
            UNION ALL
            SELECT *
            FROM $missing_in_new_order
            LIMIT 1;
        )", Config.Path.c_str(), startWh, endWh, TABLE_NEW_ORDER, TABLE_OORDER,
           TABLE_OORDER, TABLE_NEW_ORDER);

        // because of #21490 we run queries 1 by one
        // also these queries consume a lot of memory, so probably 1 by one is better.
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.insert(RunningChecks.end(), {
        { result, "3.3.2.5" },
    });
}

void TPCCChecker::ConsistencyCheck3326(TQueryClient& client) {
    const int WAREHOUSE_RANGE_SIZE = 250;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $warehouse_from = {};
            $warehouse_to = {};

            -- Aggregate order_line
            $line_counts = SELECT
                OL_W_ID AS W_ID,
                OL_D_ID AS D_ID,
                OL_O_ID AS O_ID,
                COUNT(*) AS CNT
            FROM `{}`
            WHERE OL_W_ID >= $warehouse_from AND OL_W_ID <= $warehouse_to
            GROUP BY OL_W_ID, OL_D_ID, OL_O_ID;

            -- Orders with mismatched line count
            $mismatch_cnt = SELECT
                o.O_W_ID AS W_ID,
                o.O_D_ID AS D_ID,
                o.O_ID AS O_ID,
                o.O_OL_CNT AS DeclaredCnt,
                l.CNT AS ActualCnt
            FROM (
                SELECT * FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            LEFT JOIN $line_counts AS l
            ON o.O_W_ID = l.W_ID AND o.O_D_ID = l.D_ID AND o.O_ID = l.O_ID
            WHERE o.O_OL_CNT != l.CNT;

            -- Order_lines without a matching oorder row
            $missing_order = SELECT
                l.W_ID AS W_ID,
                l.D_ID AS D_ID,
                l.O_ID AS O_ID
            FROM $line_counts AS l
            LEFT JOIN (
                SELECT * FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            ON l.W_ID = o.O_W_ID AND l.D_ID = o.O_D_ID AND l.O_ID = o.O_ID
            WHERE o.O_W_ID IS NULL;

            -- Combined check
            SELECT * FROM $mismatch_cnt
            UNION ALL
            SELECT W_ID, D_ID, O_ID, CAST(NULL AS Int32), CAST(NULL AS Int32) FROM $missing_order
            LIMIT 1;
        )", Config.Path.c_str(), startWh, endWh, TABLE_ORDER_LINE, TABLE_OORDER, TABLE_OORDER
        );

        // because of #21490 we run queries 1 by one
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.insert(RunningChecks.end(), {
        { result, "3.3.2.6" },
    });
}

// TODO: rewrite (to much mem and materialization size)
void TPCCChecker::ConsistencyCheck3327(TQueryClient& client) {
    const int WAREHOUSE_RANGE_SIZE = 100;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $warehouse_from = {};
            $warehouse_to = {};

            -- Aggregate delivery nullness for each order
            $line_flags = SELECT
                OL_W_ID AS W_ID,
                OL_D_ID AS D_ID,
                OL_O_ID AS O_ID,
                MIN(OL_DELIVERY_D IS NULL) AS ALL_NULL,
                MAX(OL_DELIVERY_D IS NULL) AS SOME_NULL
            FROM `{}`
            WHERE OL_W_ID >= $warehouse_from AND OL_W_ID <= $warehouse_to
            GROUP BY OL_W_ID, OL_D_ID, OL_O_ID;

            -- Check for mismatch between delivery status and carrier presence
            SELECT *
            FROM $line_flags AS l
            JOIN (
                SELECT * FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            ON l.W_ID = o.O_W_ID AND l.D_ID = o.O_D_ID AND l.O_ID = o.O_ID
            WHERE
                (o.O_CARRIER_ID IS NULL AND ALL_NULL = false)
                OR
                (o.O_CARRIER_ID IS NOT NULL AND SOME_NULL = true)
            LIMIT 1;
        )", Config.Path.c_str(), startWh, endWh,
           TABLE_ORDER_LINE, TABLE_OORDER);

        // because of #21490 we run queries 1 by one
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.insert(RunningChecks.end(), {
        { result, "3.3.2.7" },
    });
}

void TPCCChecker::ConsistencyCheck3328(TQueryClient& client) {
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        -- Aggregate history by warehouse
        $history_sums = SELECT
            H_W_ID AS W_ID,
            SUM(H_AMOUNT) AS SUM_H_AMOUNT
        FROM `{}`
        GROUP BY H_W_ID;

        -- Join with warehouse and check mismatch
        SELECT w.W_ID, w.W_YTD, h.SUM_H_AMOUNT, ABS(w.W_YTD - h.SUM_H_AMOUNT) as delta
        FROM `{}` as w
        JOIN $history_sums AS h
        ON w.W_ID = h.W_ID
        WHERE ABS(w.W_YTD - h.SUM_H_AMOUNT) > 1e-3
        LIMIT 1;
    )", Config.Path.c_str(), TABLE_HISTORY, TABLE_WAREHOUSE);

    RunningChecks.insert(RunningChecks.end(), {
        { CheckNoRows(client, query), "3.3.2.8" },
    });
}

void TPCCChecker::ConsistencyCheck3329(TQueryClient& client) {
    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        -- Aggregate history amounts per district
        $history_sums = SELECT
            H_W_ID AS W_ID,
            H_D_ID AS D_ID,
            SUM(H_AMOUNT) AS SUM_H_AMOUNT
        FROM `{}`
        GROUP BY H_W_ID, H_D_ID;

        -- Join with district and compare D_YTD to summed H_AMOUNT
        SELECT h.W_ID, h.D_ID, h.SUM_H_AMOUNT, ABS(d.D_YTD - h.SUM_H_AMOUNT) as delta
        FROM `{}` as d
        JOIN $history_sums AS h
        ON d.D_W_ID = h.W_ID AND d.D_ID = h.D_ID
        WHERE ABS(d.D_YTD - h.SUM_H_AMOUNT) > 1e-3
        LIMIT 1;
    )", Config.Path.c_str(), TABLE_HISTORY, TABLE_DISTRICT);

    RunningChecks.insert(RunningChecks.end(), {
        { CheckNoRows(client, query), "3.3.2.9" },
    });
}

// TODO: rewrite (to much mem and materialization size)
void TPCCChecker::ConsistencyCheck33210(TQueryClient& client) {
    // 3.3.2.10: For each customer, C_BALANCE = sum(delivered OL_AMOUNTs) - sum(H_AMOUNT)
    const int WAREHOUSE_RANGE_SIZE = 250;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $warehouse_from = {};
            $warehouse_to = {};

            $history = SELECT
                H_C_W_ID AS W_ID,
                H_C_D_ID AS D_ID,
                H_C_ID AS C_ID,
                SUM(H_AMOUNT) AS H_SUM
            FROM `{}`
            WHERE H_C_W_ID >= $warehouse_from AND H_C_W_ID <= $warehouse_to
            GROUP BY H_C_W_ID, H_C_D_ID, H_C_ID;

            $ol_sum = SELECT
                o.O_W_ID AS W_ID,
                o.O_D_ID AS D_ID,
                o.O_C_ID AS C_ID,
                SUM(ol.OL_AMOUNT) AS OL_SUM
            FROM (
                SELECT * FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            JOIN `{}` AS ol
            ON ol.OL_W_ID = o.O_W_ID AND ol.OL_D_ID = o.O_D_ID AND ol.OL_O_ID = o.O_ID
            WHERE ol.OL_DELIVERY_D IS NOT NULL
            GROUP BY o.O_W_ID, o.O_D_ID, o.O_C_ID;

            SELECT
                c.C_W_ID AS W_ID,
                c.C_D_ID AS D_ID,
                c.C_ID,
                c.C_BALANCE,
                c.C_YTD_PAYMENT,
                COALESCE(o.OL_SUM, 0.0) AS OL_SUM,
                COALESCE(h.H_SUM, 0.0) AS H_SUM
            FROM (
                SELECT * FROM `{}`
                WHERE C_W_ID >= $warehouse_from AND C_W_ID <= $warehouse_to
            ) AS c
            LEFT JOIN $history AS h
            ON c.C_W_ID = h.W_ID AND c.C_D_ID = h.D_ID AND c.C_ID = h.C_ID
            LEFT JOIN $ol_sum AS o
            ON c.C_W_ID = o.W_ID AND c.C_D_ID = o.D_ID AND c.C_ID = o.C_ID
            WHERE ABS(c.C_BALANCE - (OL_SUM - H_SUM)) > 1e-3
            LIMIT 1;
        )", Config.Path.c_str(), startWh, endWh,
           TABLE_HISTORY, TABLE_OORDER, TABLE_ORDER_LINE, TABLE_CUSTOMER);

        // because of #21490 we run queries 1 by one
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.push_back({ result, "3.3.2.10" });
}

void TPCCChecker::ConsistencyCheck33211(TQueryClient& client) {
    // 3.3.2.11: For each district, ORDER count - NEW_ORDER count = 2100
    const int WAREHOUSE_RANGE_SIZE = 250;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $warehouse_from = {};
            $warehouse_to = {};

            $order_counts = SELECT O_W_ID AS W_ID, O_D_ID AS D_ID, COUNT(*) AS ORDER_CNT
            FROM `{}`
            WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            GROUP BY O_W_ID, O_D_ID;

            $new_order_counts = SELECT NO_W_ID AS W_ID, NO_D_ID AS D_ID, COUNT(*) AS NEW_ORDER_CNT
            FROM `{}`
            WHERE NO_W_ID >= $warehouse_from AND NO_W_ID <= $warehouse_to
            GROUP BY NO_W_ID, NO_D_ID;

            SELECT o.W_ID, o.D_ID, (o.ORDER_CNT - n.NEW_ORDER_CNT) as delta
            FROM $order_counts AS o
            JOIN $new_order_counts AS n
              ON o.W_ID = n.W_ID AND o.D_ID = n.D_ID
            WHERE (ORDER_CNT - NEW_ORDER_CNT) != 2100
            LIMIT 1;
        )", Config.Path.c_str(), startWh, endWh,
           TABLE_OORDER, TABLE_NEW_ORDER);

        // because of #21490 we run queries 1 by one
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.push_back({ result, "3.3.2.11" });
}

// TODO: rewrite (to much mem and materialization size)
void TPCCChecker::ConsistencyCheck33212(TQueryClient& client) {
    // 3.3.2.12: For each customer, C_BALANCE + C_YTD_PAYMENT = sum(delivered OL_AMOUNTs)
    const int WAREHOUSE_RANGE_SIZE = 100;
    std::vector<TFuture<void>> rangeFutures;

    for (int startWh = 1; startWh <= Config.WarehouseCount; startWh += WAREHOUSE_RANGE_SIZE) {
        int endWh = std::min(startWh + WAREHOUSE_RANGE_SIZE - 1, Config.WarehouseCount);

        TString query = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            $warehouse_from = {};
            $warehouse_to = {};

            $ol_sum = SELECT
                o.O_W_ID AS W_ID,
                o.O_D_ID AS D_ID,
                o.O_C_ID AS C_ID,
                SUM(ol.OL_AMOUNT) AS OL_SUM
            FROM (
                SELECT * FROM `{}`
                WHERE O_W_ID >= $warehouse_from AND O_W_ID <= $warehouse_to
            ) AS o
            JOIN `{}` AS ol
            ON ol.OL_W_ID = o.O_W_ID AND ol.OL_D_ID = o.O_D_ID AND ol.OL_O_ID = o.O_ID
            WHERE ol.OL_DELIVERY_D IS NOT NULL
            GROUP BY o.O_W_ID, o.O_D_ID, o.O_C_ID;

            SELECT *
            FROM (
                SELECT * FROM `{}`
                WHERE C_W_ID >= $warehouse_from AND C_W_ID <= $warehouse_to
            ) AS c
            JOIN $ol_sum AS l
              ON c.C_W_ID = l.W_ID AND c.C_D_ID = l.D_ID AND c.C_ID = l.C_ID
            WHERE ABS(C_BALANCE + C_YTD_PAYMENT - l.OL_SUM) > 1e-3
            LIMIT 1;
        )", Config.Path.c_str(), startWh, endWh,
           TABLE_OORDER, TABLE_ORDER_LINE, TABLE_CUSTOMER);

        // because of #21490 we run queries 1 by one
        TStringStream ss;
        ss << "w_id_from=" << startWh << ", w_id_to=" << endWh;
        auto future = CheckNoRows(client, query, ss.Str());
        future.Wait();
        rangeFutures.push_back(future);
    }

    auto result = WaitAllAndCheck(rangeFutures);
    RunningChecks.push_back({ result, "3.3.2.12" });
}

} // anonymous

//-----------------------------------------------------------------------------

void CheckSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    TPCCChecker checker(connectionConfig, runConfig);
    checker.CheckSync();
}

} // namespace NYdb::NTPCC
