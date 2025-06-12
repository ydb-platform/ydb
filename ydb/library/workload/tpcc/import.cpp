#include "import.h"

#include "constants.h"
#include "data_splitter.h"
#include "log.h"
#include "stderr_capture.h"
#include "util.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/random/fast.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>
#include <util/string/printf.h>

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <atomic>
#include <expected>
#include <memory>
#include <stop_token>
#include <thread>
#include <vector>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

using Clock = std::chrono::steady_clock;

//-----------------------------------------------------------------------------

constexpr int MAX_RETRIES = 100;
constexpr int BACKOFF_MILLIS = 10;
constexpr int BACKOFF_CEILING = 5;

constexpr auto INDEX_PROGRESS_CHECK_INTERVAL = std::chrono::seconds(1);

//-----------------------------------------------------------------------------

int GetBackoffWaitMs(int retryCount) {
    const int waitTimeCeilingMs = (1 << BACKOFF_CEILING) * BACKOFF_MILLIS;
    int waitTimeMs = BACKOFF_MILLIS;

    for (int i = 0; i < retryCount; ++i) {
        waitTimeMs = std::min(waitTimeMs * 2, waitTimeCeilingMs);
    }

    // Add random jitter (0-99ms like in Java)
    return waitTimeMs + RandomNumber(0, 99);
}

//-----------------------------------------------------------------------------

// Generates a random string of length (strLen-1) to match Benchbase's TPCCUtil.randomStr behavior
TString RandomStringBenchbase(int strLen, char baseChar = 'a') {
    if (strLen > 1) {
        int actualLength = strLen - 1;
        TString result;
        result.reserve(actualLength);

        for (int i = 0; i < actualLength; ++i) {
            result += static_cast<char>(baseChar + RandomNumber(0, 25));
        }
        return result;
    } else {
        return "";
    }
}

// Generates a random string with [a-z] characters
TString RandomAlphaString(int minLength, int maxLength) {
    int length = RandomNumber(minLength, maxLength);
    return RandomStringBenchbase(length, 'a');
}

// Generates a random string with [A-Z] characters
TString RandomUpperAlphaString(int minLength, int maxLength) {
    int length = RandomNumber(minLength, maxLength);
    return RandomStringBenchbase(length, 'A');
}

// Generates a random string with [09] characters
TString RandomNumericString(int length) {
    TString result;
    result.reserve(length);

    for (int i = 0; i < length; ++i) {
        result += static_cast<char>('0' + RandomNumber(0, 9));
    }
    return result;
}

// Hash function for deterministic random (like Benchbase's Customer.hashCode())
size_t HashCustomer(int warehouseId, int districtId, int customerId) {
    return std::hash<int>{}(warehouseId) ^
           (std::hash<int>{}(districtId) << 1) ^
           (std::hash<int>{}(customerId) << 2);
}

// Deterministic random count like Benchbase's getRandomCount
int GetRandomCount(int warehouseId, int customerId, int districtId) {
    size_t seed = HashCustomer(warehouseId, districtId, customerId);
    TReallyFastRng32 rng(seed);
    return rng.Uniform(5, 16); // Random between 5 and 15 inclusive
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadItems(NTable::TTableClient& client, const TString& tableFullPath, TLog* Log) {
    LOG_T("Loading " << ITEM_COUNT << " items...");

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    for (int i = 1; i <= ITEM_COUNT; ++i) {
        TString data;
        int randPct = RandomNumber(1, 100);
        int len = RandomNumber(26, 50);
        if (randPct > 10) {
            // 90% of time i_data isa random string of length [26 .. 50]
            data = RandomStringBenchbase(len);
        } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere in
            // middle
            int startORIGINAL = RandomNumber(2, len - 8);
            data = RandomStringBenchbase(startORIGINAL) + "ORIGINAL" + RandomStringBenchbase(len - startORIGINAL - 8);
        }

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("I_ID").Int32(i)
            .AddMember("I_NAME").Utf8(RandomAlphaString(14, 24))
            .AddMember("I_PRICE").Double(RandomNumber(100, 10000) / 100.0)
            .AddMember("I_DATA").Utf8(data)
            .AddMember("I_IM_ID").Int32(RandomNumber(1, 10000))
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadWarehouses(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int startId,
    int lastId,
    TLog* Log)
{
    LOG_T("Loading warehouses " << startId << " to " << lastId);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    for (int warehouseId = startId; warehouseId <= lastId; ++warehouseId) {
        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("W_ID").Int32(warehouseId)
            .AddMember("W_YTD").Double(300000.00)
            .AddMember("W_TAX").Double(RandomNumber(0, 2000) / 10000.0)
            .AddMember("W_NAME").Utf8(RandomAlphaString(6, 10))
            .AddMember("W_STREET_1").Utf8(RandomAlphaString(10, 20))
            .AddMember("W_STREET_2").Utf8(RandomAlphaString(10, 20))
            .AddMember("W_CITY").Utf8(RandomAlphaString(10, 20))
            .AddMember("W_STATE").Utf8(RandomUpperAlphaString(3, 3))
            .AddMember("W_ZIP").Utf8("123456789")
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadStock(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int itemId,
    int itemsToLoad,
    TLog* Log)
{
    LOG_T("Loading stock for warehouse " << wh << " items " << itemId << " to " << (itemId + itemsToLoad - 1));

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    for (int i = 0; i < itemsToLoad; ++i) {
        int currentItemId = itemId + i;

        // s_data - match Benchbase inline logic exactly
        TString data;
        int randPct = RandomNumber(1, 100);
        int len = RandomNumber(26, 50);
        if (randPct > 10) {
            // 90% of time i_data isa random string of length [26 ..
            // 50]
            data = RandomStringBenchbase(len);
        } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere
            // in middle
            int startORIGINAL = RandomNumber(2, len - 8);
            data = RandomStringBenchbase(startORIGINAL) + "ORIGINAL" + RandomStringBenchbase(len - startORIGINAL - 8);
        }

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("S_W_ID").Int32(wh)
            .AddMember("S_I_ID").Int32(currentItemId)
            .AddMember("S_QUANTITY").Int32(RandomNumber(10, 100))
            .AddMember("S_ORDER_CNT").Int32(0)
            .AddMember("S_REMOTE_CNT").Int32(0)
            .AddMember("S_DATA").Utf8(data)
            .AddMember("S_DIST_01").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_02").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_03").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_04").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_05").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_06").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_07").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_08").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_09").Utf8(RandomStringBenchbase(24))
            .AddMember("S_DIST_10").Utf8(RandomStringBenchbase(24))
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadDistricts(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int startId,
    int lastId,
    TLog* Log)
{
    LOG_T("Loading districts for warehouses " << startId << " to " << lastId);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    for (int warehouseId = startId; warehouseId <= lastId; ++warehouseId) {
        for (int districtId = DISTRICT_LOW_ID; districtId <= DISTRICT_HIGH_ID; ++districtId) {
            valueBuilder.AddListItem()
                .BeginStruct()
                .AddMember("D_W_ID").Int32(warehouseId)
                .AddMember("D_ID").Int32(districtId)
                .AddMember("D_YTD").Double(30000.00)
                .AddMember("D_TAX").Double(RandomNumber(0, 2000) / 10000.0)
                .AddMember("D_NEXT_O_ID").Int32(CUSTOMERS_PER_DISTRICT + 1)
                .AddMember("D_NAME").Utf8(RandomAlphaString(6, 10))
                .AddMember("D_STREET_1").Utf8(RandomAlphaString(10, 20))
                .AddMember("D_STREET_2").Utf8(RandomAlphaString(10, 20))
                .AddMember("D_CITY").Utf8(RandomAlphaString(10, 20))
                .AddMember("D_STATE").Utf8(RandomUpperAlphaString(3, 3))
                .AddMember("D_ZIP").Utf8("123456789")
                .EndStruct();
        }
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadCustomers(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    TLog* Log)
{
    LOG_T("Loading customers for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    for (int customerId = 1; customerId <= CUSTOMERS_PER_DISTRICT; ++customerId) {
        TString last;
        if (customerId <= 1000) {
            last = GetLastName(customerId - 1);
        } else {
            last = GetNonUniformRandomLastNameForLoad();
        }

        TString credit = RandomNumber(1, 100) <= 10 ? "BC" : "GC";

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("C_W_ID").Int32(wh)
            .AddMember("C_D_ID").Int32(district)
            .AddMember("C_ID").Int32(customerId)
            .AddMember("C_DISCOUNT").Double(RandomNumber(1, 5000) / 10000.0)
            .AddMember("C_CREDIT").Utf8(credit)
            .AddMember("C_LAST").Utf8(last)
            .AddMember("C_FIRST").Utf8(RandomAlphaString(8, 16))
            .AddMember("C_CREDIT_LIM").Double(50000.00)
            .AddMember("C_BALANCE").Double(-10.00)
            .AddMember("C_YTD_PAYMENT").Double(10.00)
            .AddMember("C_PAYMENT_CNT").Int32(1)
            .AddMember("C_DELIVERY_CNT").Int32(0)
            .AddMember("C_STREET_1").Utf8(RandomAlphaString(10, 20))
            .AddMember("C_STREET_2").Utf8(RandomAlphaString(10, 20))
            .AddMember("C_CITY").Utf8(RandomAlphaString(10, 20))
            .AddMember("C_STATE").Utf8(RandomUpperAlphaString(3, 3))
            .AddMember("C_ZIP").Utf8(RandomNumericString(4) + "11111")
            .AddMember("C_PHONE").Utf8(RandomNumericString(16))
            .AddMember("C_SINCE").Timestamp(TInstant::Now())
            .AddMember("C_MIDDLE").Utf8("OE")
            .AddMember("C_DATA").Utf8(RandomAlphaString(300, 500))
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadCustomerHistory(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    TLog* Log)
{
    LOG_T("Loading customer history for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    i64 prevTs = 0;
    for (int customerId = 1; customerId <= CUSTOMERS_PER_DISTRICT; ++customerId) {
        TInstant date = TInstant::Now();
        // Match Benchbase: ensure monotonic nanosecond timestamps
        i64 nanoTs = TInstant::Now().NanoSeconds();
        if (nanoTs <= prevTs) {
            nanoTs = prevTs + 1;
        }
        prevTs = nanoTs;

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("H_C_W_ID").Int32(wh)
            .AddMember("H_C_ID").Int32(customerId)
            .AddMember("H_C_D_ID").Int32(district)
            .AddMember("H_D_ID").Int32(district)
            .AddMember("H_W_ID").Int32(wh)
            .AddMember("H_DATE").Timestamp(date)
            .AddMember("H_AMOUNT").Double(10.00)
            .AddMember("H_DATA").Utf8(RandomAlphaString(10, 24))
            .AddMember("H_C_NANO_TS").Int64(nanoTs)
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadOpenOrders(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    TLog* Log)
{
    LOG_T("Loading open orders for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, customersPerDistrict]
    std::vector<int> customerIds;
    customerIds.reserve(CUSTOMERS_PER_DISTRICT);
    for (int i = 1; i <= CUSTOMERS_PER_DISTRICT; ++i) {
        customerIds.push_back(i);
    }
    Shuffle(customerIds.begin(), customerIds.end());

    for (int orderId = 1; orderId <= CUSTOMERS_PER_DISTRICT; ++orderId) {
        int customerId = customerIds[orderId - 1]; // Use shuffled customer ID
        int carrierId = (orderId < FIRST_UNPROCESSED_O_ID) ? RandomNumber(1, 10) : 0;
        int olCnt = GetRandomCount(wh, orderId, district); // Deterministic count

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("O_W_ID").Int32(wh)
            .AddMember("O_D_ID").Int32(district)
            .AddMember("O_ID").Int32(orderId)
            .AddMember("O_C_ID").Int32(customerId)
            .AddMember("O_CARRIER_ID").Int32(carrierId)
            .AddMember("O_OL_CNT").Int32(olCnt)
            .AddMember("O_ALL_LOCAL").Int32(1)
            .AddMember("O_ENTRY_D").Timestamp(TInstant::Now())
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadNewOrders(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    TLog* Log)
{
    LOG_T("Loading new orders for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    static_assert(FIRST_UNPROCESSED_O_ID < CUSTOMERS_PER_DISTRICT,
                "FIRST_UNPROCESSED_O_ID must be less than CUSTOMERS_PER_DISTRICT");

    // New Order data (only for recent orders)
    for (int orderId = FIRST_UNPROCESSED_O_ID; orderId <= CUSTOMERS_PER_DISTRICT; ++orderId) {
        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("NO_W_ID").Int32(wh)
            .AddMember("NO_D_ID").Int32(district)
            .AddMember("NO_O_ID").Int32(orderId)
            .EndStruct();
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadOrderLines(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    TLog* Log)
{
    LOG_T("Loading order lines for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder();
    valueBuilder.BeginList();

    for (int orderId = 1; orderId <= CUSTOMERS_PER_DISTRICT; ++orderId) {
        int olCnt = GetRandomCount(wh, orderId, district); // Deterministic count

        // Order Line data
        for (int lineNumber = 1; lineNumber <= olCnt; ++lineNumber) {
            int itemId = RandomNumber(1, ITEM_COUNT);

            // Set OL_DELIVERY_D and OL_AMOUNT based on itemId condition (like Benchbase!)
            TInstant deliveryDate;
            double amount;
            if (itemId < FIRST_UNPROCESSED_O_ID) {
                deliveryDate = TInstant::Now();
                amount = 0.0;
            } else {
                deliveryDate = TInstant::Zero(); // epoch timestamp
                // random within [0.01 .. 9,999.99]
                amount = RandomNumber(1, 999999) / 100.0;
            }

            valueBuilder.AddListItem()
                .BeginStruct()
                .AddMember("OL_W_ID").Int32(wh)
                .AddMember("OL_D_ID").Int32(district)
                .AddMember("OL_O_ID").Int32(orderId)
                .AddMember("OL_NUMBER").Int32(lineNumber)
                .AddMember("OL_I_ID").Int32(itemId)
                .AddMember("OL_DELIVERY_D").Timestamp(deliveryDate)
                .AddMember("OL_AMOUNT").Double(amount)
                .AddMember("OL_SUPPLY_W_ID").Int32(wh)
                .AddMember("OL_QUANTITY").Double(5.0)
                .AddMember("OL_DIST_INFO").Utf8(RandomStringBenchbase(24))
                .EndStruct();
        }
    }

    valueBuilder.EndList();
    return client.BulkUpsert(tableFullPath, valueBuilder.Build()).ExtractValueSync();
}

//-----------------------------------------------------------------------------

template<typename LoadFunc>
void ExecuteWithRetry(const TString& operationName, LoadFunc loadFunc, TLog* Log) {
    for (int retryCount = 0; retryCount < MAX_RETRIES; ++retryCount) {
        auto result = loadFunc();
        if (result.IsSuccess()) {
            return;
        }

        if (retryCount < MAX_RETRIES - 1) {
            int waitMs = GetBackoffWaitMs(retryCount);
            LOG_T("Retrying " << operationName << " after " << waitMs << " ms due to: "
                    << result.GetIssues().ToOneLineString());
            Sleep(TDuration::MilliSeconds(waitMs));
        } else {
            LOG_E(operationName << " failed after " << MAX_RETRIES << " retries: "
                    << result.GetIssues().ToOneLineString());
            RequestStop();
            return;
        }
    }
}

//-----------------------------------------------------------------------------

void LoadSmallTables(TDriver& driver, const TString& path, int warehouseCount, TLog* Log) {
    NTable::TTableClient tableClient(driver);

    TString itemTablePath = path + "/" + TABLE_ITEM;
    TString warehouseTablePath = path + "/" + TABLE_WAREHOUSE;
    TString districtTablePath = path + "/" + TABLE_DISTRICT;

    ExecuteWithRetry("LoadItems", [&]() {
        return LoadItems(tableClient, itemTablePath, Log);
    }, Log);
    ExecuteWithRetry("LoadWarehouses", [&]() {
        return LoadWarehouses(tableClient, warehouseTablePath, 1, warehouseCount, Log);
    }, Log);
    ExecuteWithRetry("LoadDistricts", [&]() {
        return LoadDistricts(tableClient, districtTablePath, 1, warehouseCount, Log);
    }, Log);
}

//-----------------------------------------------------------------------------

struct TIndexBuildState {
    TOperation::TOperationId Id;
    TString Table;
    TString Name;
    double Progress = 0.0;

    TIndexBuildState(TOperation::TOperationId id, const TString& table, const TString& name)
        : Id(id), Table(table), Name(name) {}
};

struct TLoadState {
    enum ELoadState {
        ELOAD_INDEXED_TABLES = 0,
        ELOAD_TABLES_BUILD_INDICES,
        EWAIT_INDICES,
        ESUCCESS
    };

    explicit TLoadState(std::stop_token stopToken)
        : State(ELOAD_INDEXED_TABLES)
        , StopToken(stopToken)
    {
    }

    ELoadState State;

    // shared with loader threads

    std::stop_token StopToken;

    std::atomic<size_t> DataSizeLoaded{0};

    std::atomic<size_t> IndexedRangesLoaded{0};
    std::atomic<size_t> RangesLoaded{0};

    // single threaded

    std::vector<TIndexBuildState> IndexBuildStates;
    size_t CurrentIndex = 0;
    size_t ApproximateDataSize = 0;
};

//-----------------------------------------------------------------------------

void LoadRange(TDriver& driver, const TString& path, int whStart, int whEnd, TLoadState& state, TLog* Log) {
    NTable::TTableClient tableClient(driver);

    static_assert(ITEM_COUNT % 10 == 0, "ITEM_COUNT must be divisible by 10");

    TString stockTablePath = path + "/" + TABLE_STOCK;
    TString customerTablePath = path + "/" + TABLE_CUSTOMER;
    TString orderLineTablePath = path + "/" + TABLE_ORDER_LINE;
    TString historyTablePath = path + "/" + TABLE_HISTORY;
    TString oorderTablePath = path + "/" + TABLE_OORDER;
    TString newOrderTablePath = path + "/" + TABLE_NEW_ORDER;

    size_t customerDataSizePerWh = static_cast<size_t>(TDataSplitter::GetPerWarehouseMB(TABLE_CUSTOMER) * 1024 * 1024);
    size_t oorderDataSizePerWh = static_cast<size_t>(TDataSplitter::GetPerWarehouseMB(TABLE_OORDER) * 1024 * 1024);
    size_t stockDataSizePerWh = static_cast<size_t>(TDataSplitter::GetPerWarehouseMB(TABLE_STOCK) * 1024 * 1024);
    size_t orderLineDataSizePerWh = static_cast<size_t>(TDataSplitter::GetPerWarehouseMB(TABLE_ORDER_LINE) * 1024 * 1024);
    size_t historyDataSizePerWh = static_cast<size_t>(TDataSplitter::GetPerWarehouseMB(TABLE_HISTORY) * 1024 * 1024);
    size_t newOrderDataSizePerWh = static_cast<size_t>(TDataSplitter::GetPerWarehouseMB(TABLE_NEW_ORDER) * 1024 * 1024);

    const size_t indexedPerWh = customerDataSizePerWh + oorderDataSizePerWh;

    // load tables with indices first (so that we could start to build indices in background)
    for (int wh = whStart; wh <= whEnd; ++wh) {
        if (state.StopToken.stop_requested()) {
            return;
        }

        for (int district = DISTRICT_LOW_ID; district <= DISTRICT_HIGH_ID; ++district) {
            ExecuteWithRetry("LoadCustomers", [&]() {
                return LoadCustomers(tableClient, customerTablePath, wh, district, Log);
            }, Log);
            ExecuteWithRetry("LoadOpenOrders", [&]() {
                return LoadOpenOrders(tableClient, oorderTablePath, wh, district, Log);
            }, Log);
        }
        state.DataSizeLoaded.fetch_add(indexedPerWh, std::memory_order_relaxed);
    }

    state.IndexedRangesLoaded.fetch_add(1);

    const size_t perWhDatasize = stockDataSizePerWh + orderLineDataSizePerWh + historyDataSizePerWh + newOrderDataSizePerWh;

    for (int wh = whStart; wh <= whEnd; ++wh) {
        if (state.StopToken.stop_requested()) {
            return;
        }

        constexpr int itemBatchSize = ITEM_COUNT / 10;
        for (int batch = 0; batch < 10; ++batch) {
            int startItemId = batch * itemBatchSize + 1;
            int itemsToLoad = itemBatchSize;
            ExecuteWithRetry("LoadStock", [&]() {
                return LoadStock(tableClient, stockTablePath, wh, startItemId, itemsToLoad, Log);
            }, Log);
        }

        for (int district = DISTRICT_LOW_ID; district <= DISTRICT_HIGH_ID; ++district) {
            ExecuteWithRetry("LoadOrderLines", [&]() {
                return LoadOrderLines(tableClient, orderLineTablePath, wh, district, Log);
            }, Log);
            ExecuteWithRetry("LoadCustomerHistory", [&]() {
                return LoadCustomerHistory(tableClient, historyTablePath, wh, district, Log);
            }, Log);
            ExecuteWithRetry("LoadNewOrders", [&]() {
                return LoadNewOrders(tableClient, newOrderTablePath, wh, district, Log);
            }, Log);
        }

        state.DataSizeLoaded.fetch_add(perWhDatasize, std::memory_order_relaxed);
    }

    state.RangesLoaded.fetch_add(1);
}

//-----------------------------------------------------------------------------

TOperation::TOperationId CreateIndex(
    NTable::TTableClient& client,
    const TString& path,
    const char* table,
    const char* indexName,
    const std::vector<std::string>& columns,
    TLog* Log)
{
    TString tablePath;
    if (!path.empty()) {
        tablePath = path + "/" + table;
    } else {
        tablePath = table;
    }

    auto settings = NTable::TAlterTableSettings()
        .AppendAddIndexes({NTable::TIndexDescription(indexName, NTable::EIndexType::GlobalSync, columns)});

    TOperation::TOperationId operationId;
    auto result = client.RetryOperationSync([&](NTable::TSession session) {
        auto opResult = session.AlterTableLong(tablePath, settings).GetValueSync();
        if (opResult.Ready() && !opResult.Status().IsSuccess()) {
            LOG_W("Failed to create index " << indexName << " for " << tablePath << ": "
                << opResult.ToString() << ", retrying");
            return opResult.Status();
        } else {
            operationId = opResult.Id();
        }
        return TStatus(EStatus::SUCCESS, NIssue::TIssues());
    });

    if (operationId.GetKind() == TOperation::TOperationId::UNUSED) {
        LOG_E("Failed to create index " << indexName << " for " << tablePath);
    }

    return operationId;
}

TIndexBuildState CreateCustomerIndex(NTable::TTableClient& client, const TString& path, TLog* Log) {
    std::vector<std::string> columns = { "C_W_ID", "C_D_ID", "C_LAST", "C_FIRST" };
    auto id = CreateIndex(client, path, TABLE_CUSTOMER, INDEX_CUSTOMER_NAME, columns, Log);
    LOG_T("Creating customer index: " << id.ToString());
    return TIndexBuildState(id, TABLE_CUSTOMER, INDEX_CUSTOMER_NAME);
}

TIndexBuildState CreateOpenOrderIndex(NTable::TTableClient& client, const TString& path, TLog* Log) {
    std::vector<std::string> columns = { "O_W_ID", "O_D_ID", "O_C_ID", "O_ID" };
    auto id = CreateIndex(client, path, TABLE_OORDER, INDEX_ORDER, columns, Log);
    LOG_T("Creating oorder index: " << id.ToString());
    return TIndexBuildState(id, TABLE_OORDER, INDEX_ORDER);
}

void CreateIndices(TDriver& driver, const TString& path, TLoadState& loadState, TLog* Log) {
    NTable::TTableClient client(driver);

    loadState.IndexBuildStates = {
        CreateCustomerIndex(client, path, Log),
        CreateOpenOrderIndex(client, path, Log)
    };
}

//-----------------------------------------------------------------------------

// returns either progress (100 – done) or string with error
std::expected<double, std::string> GetIndexProgress(
    NOperation::TOperationClient& client,
    const TOperation::TOperationId& id)
{
    auto operation = client.Get<NTable::TBuildIndexOperation>(id).GetValueSync();
    if (operation.Ready()) {
        if (operation.Status().IsSuccess() && operation.Metadata().State == NTable::EBuildIndexState::Done) {
            return 100;
        }

        TStringStream ss;
        ss << "Failed to create index, operation id " << id.ToString() << ": " << operation.Status()
            << ", build state: " << operation.Metadata().State << ", " << operation.Metadata().Path;
        return std::unexpected(ss.Str());
    } else {
        return operation.Metadata().Progress;
    }
}

//-----------------------------------------------------------------------------

std::stop_source StopByInterrupt;

void InterruptHandler(int) {
    StopByInterrupt.request_stop();
}

//-----------------------------------------------------------------------------

class TPCCLoader {
public:
    struct TCalculatedStatusData {
        size_t CurrentDataSizeLoaded = 0;
        double PercentLoaded = 0.0;
        double InstantSpeedMiBs = 0.0;
        double AvgSpeedMiBs = 0.0;
        int ElapsedMinutes = 0;
        int ElapsedSeconds = 0;
        int EstimatedTimeLeftMinutes = 0;
        int EstimatedTimeLeftSeconds = 0;
        bool IsWaitingForIndices = false;
        bool IsLoadingTablesAndBuildingIndices = false;
    };

    TPCCLoader(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig)
        : ConnectionConfig(connectionConfig)
        , Config(runConfig)
        , Log(std::make_unique<TLog>(CreateLogBackend("cerr", runConfig.LogPriority, true)))
        , PreviousDataSizeLoaded(0)
        , StartTime(Clock::now())
        , LoadState(StopByInterrupt.get_token())
        , LogCapture(Config.DisplayMode == TRunConfig::EDisplayMode::Tui ?
                     std::make_unique<TStdErrCapture>(TUI_LOG_LINES) : nullptr)
    {
    }

    void ImportSync() {
        Config.SetDisplayUpdateInterval();
        CalculateApproximateDataSize();

        // we want to switch buffers and draw UI ASAP to properly display logs
        // produced after this point and before the first screen update
        if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
            UpdateDisplayIfNeeded(Clock::now());
        }

        // in particular this log message
        LOG_I("Starting TPC-C data import for " << Config.WarehouseCount << " warehouses using " <<
                Config.ThreadCount << " threads. Approximate data size: " << GetFormattedSize(LoadState.ApproximateDataSize));

        // TODO: detect number of threads
        size_t threadCount = std::min(Config.WarehouseCount, Config.ThreadCount );
        threadCount = std::max(threadCount, size_t(1));

        // TODO: calculate optimal number of drivers (but per thread looks good)
        size_t driverCount = threadCount;

        std::vector<TDriver> drivers;
        drivers.reserve(driverCount);
        for (size_t i = 0; i < driverCount; ++i) {
            drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(ConnectionConfig));
        }

        // we set handler as late as possible to overwrite
        // any handlers set deeply inside (e.g. in)
        signal(SIGINT, InterruptHandler);
        signal(SIGTERM, InterruptHandler);

        StartTime = Clock::now();
        auto startTs = TInstant::Now();

        std::vector<std::thread> threads;
        threads.reserve(threadCount);

        for (size_t threadId = 0; threadId < threadCount; ++threadId) {
            int whStart = threadId * Config.WarehouseCount / threadCount + 1;
            int whEnd = (threadId + 1) * Config.WarehouseCount / threadCount;

            threads.emplace_back([threadId, &drivers, driverCount, this, whStart, whEnd]() {
                auto& driver = drivers[threadId % driverCount];
                if (threadId == 0) {
                    LoadSmallTables(driver, Config.Path, Config.WarehouseCount, Log.get());
                }
                LoadRange(driver, Config.Path, whStart, whEnd, LoadState, Log.get());
            });
        }

        NOperation::TOperationClient operationClient(drivers[0]);

        Clock::time_point lastIndexProgressCheck = Clock::time_point::min();

        while (true) {
            if (StopByInterrupt.stop_requested()) {
                break;
            }

            if (LoadState.State == TLoadState::ESUCCESS) {
                break;
            }

            auto now = Clock::now();
            UpdateDisplayIfNeeded(now);

            switch (LoadState.State) {
            case TLoadState::ELOAD_INDEXED_TABLES: {
                // Check if all indexed ranges are loaded and start index creation
                size_t indexedRangesLoaded = LoadState.IndexedRangesLoaded.load(std::memory_order_relaxed);
                if (indexedRangesLoaded >= threadCount) {
                    CreateIndices(drivers[0], Config.Path, LoadState, Log.get());
                    LOG_I("Indexed tables loaded, indices are being built in background. Continuing with remaining tables");
                    LoadState.State = TLoadState::ELOAD_TABLES_BUILD_INDICES;
                    lastIndexProgressCheck = now;
                }
                break;
            }
            case TLoadState::ELOAD_TABLES_BUILD_INDICES: {
                // Check if all ranges are loaded (work is complete)
                size_t rangesLoaded = LoadState.RangesLoaded.load(std::memory_order_relaxed);
                if (rangesLoaded >= threadCount) {
                    LOG_I("All tables loaded successfully. Waiting for indices to be ready");
                    LoadState.State = TLoadState::EWAIT_INDICES;
                }
                [[fallthrough]];
            }
            case TLoadState::EWAIT_INDICES: {
                auto timeSinceLastCheck = now - lastIndexProgressCheck;
                if (timeSinceLastCheck < INDEX_PROGRESS_CHECK_INTERVAL) {
                    break;
                }
                lastIndexProgressCheck = now;

                // update progress of all indices and advance LoadState.CurrentIndex
                for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                    auto& indexState = LoadState.IndexBuildStates[i];
                    auto progress = GetIndexProgress(operationClient, indexState.Id);
                    if (!progress) {
                        LOG_E("Failed to build index " << indexState.Name <<  ": " << progress.error());
                        RequestStop();
                        return;
                    }
                    indexState.Progress = *progress;
                    if (i == LoadState.CurrentIndex && indexState.Progress == 100.0) {
                        ++LoadState.CurrentIndex;
                    }
                }

                if (LoadState.State == TLoadState::EWAIT_INDICES && LoadState.CurrentIndex >= LoadState.IndexBuildStates.size()) {
                    LOG_I("Indices created successfully");
                    LoadState.State = TLoadState::ESUCCESS;
                    continue;
                }
                break;
            }
            case TLoadState::ESUCCESS:
                break;
            }

            std::this_thread::sleep_for(TRunConfig::SleepMsEveryIterationMainLoop);
            now = Clock::now();
        }

        if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
            ExitTuiMode();
        }

        if (StopByInterrupt.stop_requested()) {
            LOG_I("Stop requested, waiting for threads to finish");
        }

        for (auto& thread : threads) {
            thread.join();
        }

        for (auto& driver : drivers) {
            driver.Stop(true);
        }

        auto endTs = TInstant::Now();
        auto duration = endTs - startTs;

        if (LoadState.State == TLoadState::ESUCCESS) {
            // Calculate average upload speed
            size_t totalDataLoaded = LoadState.DataSizeLoaded.load(std::memory_order_relaxed);
            auto totalElapsedSeconds = duration.Seconds();
            double avgSpeedMiBs = totalElapsedSeconds > 0 ?
                static_cast<double>(totalDataLoaded) / (1024 * 1024) / totalElapsedSeconds : 0.0;

            std::stringstream avgSpeedStream;
            avgSpeedStream << std::fixed << std::setprecision(1) << avgSpeedMiBs;
            std::string avgSpeedMiBsStr = avgSpeedStream.str();

            LOG_I("TPC-C data import completed successfully in " << duration.ToString()
                  << " (avg: " << avgSpeedMiBsStr << " MiB/s)");
        }
    }

private:
    void CalculateApproximateDataSize() {
        // Note, we calculate approximate data size based on heavy tables only
        double stockSize = TDataSplitter::GetPerWarehouseMB(TABLE_STOCK) * Config.WarehouseCount;
        double customerSize = TDataSplitter::GetPerWarehouseMB(TABLE_CUSTOMER) * Config.WarehouseCount;
        double historySize = TDataSplitter::GetPerWarehouseMB(TABLE_HISTORY) * Config.WarehouseCount;
        double oorderSize = TDataSplitter::GetPerWarehouseMB(TABLE_OORDER) * Config.WarehouseCount;
        double orderLineSize = TDataSplitter::GetPerWarehouseMB(TABLE_ORDER_LINE) * Config.WarehouseCount;

        double totalMB = stockSize + customerSize + historySize + oorderSize + orderLineSize;

        LoadState.ApproximateDataSize = static_cast<size_t>(totalMB * 1024 * 1024);  // Convert to bytes
    }

    void UpdateDisplayIfNeeded(Clock::time_point now) {
        auto delta = now - LastDisplayUpdate;
        if (delta < Config.DisplayUpdateInterval) {
            return;
        }

        // Calculate all status data
        TCalculatedStatusData data;
        data.IsWaitingForIndices = !LoadState.IndexBuildStates.empty() && LoadState.State == TLoadState::EWAIT_INDICES;
        data.IsLoadingTablesAndBuildingIndices = LoadState.State == TLoadState::ELOAD_TABLES_BUILD_INDICES;

        if (!data.IsWaitingForIndices) {
            data.CurrentDataSizeLoaded = LoadState.DataSizeLoaded.load(std::memory_order_relaxed);

            data.PercentLoaded = LoadState.ApproximateDataSize > 0 ?
                (static_cast<double>(data.CurrentDataSizeLoaded) / LoadState.ApproximateDataSize) * 100.0 : 0.0;

            auto deltaSeconds = std::chrono::duration<double>(delta).count();
            data.InstantSpeedMiBs = deltaSeconds > 0 ?
                static_cast<double>(data.CurrentDataSizeLoaded - PreviousDataSizeLoaded) / (1024 * 1024) / deltaSeconds : 0.0;

            auto totalElapsed = std::chrono::duration<double>(now - StartTime).count();
            data.AvgSpeedMiBs = totalElapsed > 0 ?
                static_cast<double>(data.CurrentDataSizeLoaded) / (1024 * 1024) / totalElapsed : 0.0;

            data.ElapsedMinutes = static_cast<int>(totalElapsed / 60);
            data.ElapsedSeconds = static_cast<int>(totalElapsed) % 60;

            if (data.AvgSpeedMiBs > 0 && data.CurrentDataSizeLoaded < LoadState.ApproximateDataSize) {
                double remainingBytes = LoadState.ApproximateDataSize - data.CurrentDataSizeLoaded;
                double remainingSeconds = remainingBytes / (1024 * 1024) / data.AvgSpeedMiBs;
                data.EstimatedTimeLeftMinutes = static_cast<int>(remainingSeconds / 60);
                data.EstimatedTimeLeftSeconds = static_cast<int>(remainingSeconds) % 60;
            }

            PreviousDataSizeLoaded = data.CurrentDataSizeLoaded;
        }

        switch (Config.DisplayMode) {
        case TRunConfig::EDisplayMode::Text:
            UpdateDisplayTextMode(data);
            break;
        case TRunConfig::EDisplayMode::Tui:
            UpdateDisplayTuiMode(data);
            break;
        default:
            ;
        }

        LastDisplayUpdate = now;
    }

    void UpdateDisplayTextMode(const TCalculatedStatusData& data) {
        if (!data.IsWaitingForIndices) {
            std::stringstream ss;
            ss << std::fixed << std::setprecision(1) << "Progress: " << data.PercentLoaded << "% "
                << "(" << GetFormattedSize(data.CurrentDataSizeLoaded) << ") "
                << std::setprecision(1) << data.InstantSpeedMiBs << " MiB/s "
                << "(avg: " << data.AvgSpeedMiBs << " MiB/s) "
                << "elapsed: " << data.ElapsedMinutes << ":" << std::setfill('0') << std::setw(2) << data.ElapsedSeconds << " "
                << "ETA: " << data.EstimatedTimeLeftMinutes << ":"
                << std::setfill('0') << std::setw(2) << data.EstimatedTimeLeftSeconds;

            if (data.IsLoadingTablesAndBuildingIndices) {
                ss << " | ";
                for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                    const auto& indexState = LoadState.IndexBuildStates[i];
                    if (i > 0) ss << ", ";
                    ss << "index " << (i + 1) << ": " << std::fixed << std::setprecision(1) << indexState.Progress << "%";
                }
            }

            LOG_I(ss.str());
        } else {
            // waiting for indices
            if (LoadState.CurrentIndex < LoadState.IndexBuildStates.size()) {
                std::stringstream ss;
                ss << "Waiting for indices ";
                for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                    const auto& indexState = LoadState.IndexBuildStates[i];
                    if (i > 0) ss << ", ";
                    ss << "index " << (i + 1) << ": " << std::fixed << std::setprecision(1) << indexState.Progress << "%";
                }
                LOG_I(ss.str());
            }
        }
    }

    void UpdateDisplayTuiMode(const TCalculatedStatusData& data) {
        using namespace ftxui;

        // fist update is very special: we switch buffers and capture stderr to display live logs
        static bool firstUpdate = true;
        if (firstUpdate) {
            if (LogCapture) {
                LogCapture->StartCapture();
            }

            // Switch to alternate screen buffer (like htop)
            std::cout << "\033[?1049h";
            std::cout << "\033[2J\033[H"; // Clear screen and move to top
            firstUpdate = false;
        }

        if (LogCapture) {
            LogCapture->UpdateCapture();
        }

        // our header with main information

        std::stringstream headerSs;
        headerSs << "TPC-C Import: " << Config.WarehouseCount << " warehouses, "
                 << Config.ThreadCount << " threads   Estimated size: "
                 << GetFormattedSize(LoadState.ApproximateDataSize);

        std::stringstream progressSs;
        progressSs << std::fixed << std::setprecision(1) << data.PercentLoaded << "% ("
                   << GetFormattedSize(data.CurrentDataSizeLoaded) << ")";

        std::stringstream speedSs;
        speedSs << std::fixed << std::setprecision(1)
                << "Speed: " << data.InstantSpeedMiBs << " MiB/s   "
                << "Avg: " << data.AvgSpeedMiBs << " MiB/s   "
                << "Elapsed: " << data.ElapsedMinutes << ":"
                << std::setfill('0') << std::setw(2) << data.ElapsedSeconds << "   "
                << "ETA: " << data.EstimatedTimeLeftMinutes << ":"
                << std::setfill('0') << std::setw(2) << data.EstimatedTimeLeftSeconds;

        // Calculate progress ratio for gauge
        float progressRatio = static_cast<float>(data.PercentLoaded / 100.0);

        // Left side: Import details
        auto importDetails = vbox({
            text(headerSs.str()),
            hbox({
                text("Progress: "),
                gauge(progressRatio) | flex,
                text("  " + progressSs.str())
            }),
            text(speedSs.str())
        });

        auto topRow = hbox({
            importDetails | flex,
            separator()
        });

        // Index progress section (always shown)

        Elements indexElements;
        TString indexText;
        if (LoadState.IndexBuildStates.empty()) {
            indexText = "Index Creation Progress didn't start";
        } else {
            indexText = "Index Creation Progress:";
        }
        indexElements.push_back(text(indexText));

        if (LoadState.IndexBuildStates.empty()) {
            // Index building not started yet, need to leave enough space
            for (size_t i = 0; i < INDEX_COUNT; ++i) {
                float indexRatio = static_cast<float>(0.0);

                std::stringstream indexSs;
                indexSs << std::fixed << std::setprecision(1) << 0.0 << "%";

                indexElements.push_back(
                    hbox({
                        text("  [ index " + std::to_string(i + 1) + " ] "),
                        gauge(indexRatio) | flex,
                        text(" " + indexSs.str())
                    })
                );
            }
        } else {
            // Show progress for each index
            for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                const auto& indexState = LoadState.IndexBuildStates[i];
                float indexRatio = static_cast<float>(indexState.Progress / 100.0);

                std::stringstream indexSs;
                indexSs << std::fixed << std::setprecision(1) << indexState.Progress << "%";

                indexElements.push_back(
                    hbox({
                        text("  [ index " + std::to_string(i + 1) + " ] "),
                        gauge(indexRatio) | flex,
                        text(" " + indexSs.str())
                    })
                );
            }
        }

        // Create scrollable logs panel

        Elements logElements;

        const auto& capturedLines = LogCapture->GetLogLines();
        size_t truncatedCount = LogCapture->GetTruncatedCount();
        if (truncatedCount > 0) {
            logElements.push_back(text("... logs truncated: " + std::to_string(truncatedCount) + " lines"));
        }

        for (const auto& line: capturedLines) {
            logElements.push_back(text(line));
        }

        auto logsContent = vbox(logElements);
        auto logsPanel = window(text(" Logs "), logsContent | vscroll_indicator | frame | flex);

        // Main layout - fill the entire screen

        auto layout = vbox({
            topRow,
            separator(),
            vbox(indexElements),
            separator(),
            logsPanel | flex
        });

        // Render full screen

        std::cout << "\033[H"; // Move cursor to top
        auto screen = Screen::Create(Dimension::Full(), Dimension::Full());
        Render(screen, layout);
        std::cout << screen.ToString();
    }

    void ExitTuiMode() {
        // Restore stderr and flush captured logs
        if (LogCapture) {
            LogCapture->RestoreAndFlush();
        }

        // Switch back to main screen buffer (restore original content)
        std::cout << "\033[?1049l";
        std::cout.flush();
    }

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;

    std::unique_ptr<TLog> Log;
    Clock::time_point LastDisplayUpdate;
    size_t PreviousDataSizeLoaded;
    Clock::time_point StartTime;
    TLoadState LoadState;
    std::unique_ptr<TStdErrCapture> LogCapture;
};

} // anonymous namespace

//-----------------------------------------------------------------------------

void ImportSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    TPCCLoader loader(connectionConfig, runConfig);
    loader.ImportSync();
}

} // namespace NYdb::NTPCC
