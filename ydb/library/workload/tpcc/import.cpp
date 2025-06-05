#include "import.h"

#include "constants.h"
#include "data_splitter.h"
#include "log.h"
#include "util.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/random/fast.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>
#include <util/string/printf.h>

#include <atomic>
#include <format>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr int MAX_RETRIES = 100;
constexpr int BACKOFF_MILLIS = 10;
constexpr int BACKOFF_CEILING = 5;

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
    TVector<int> customerIds;
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
            throw std::runtime_error(
                std::format("Failed to execute {}: {}",
                    operationName.c_str(), result.GetIssues().ToOneLineString().c_str()));
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

struct TLoadState {
    std::atomic<size_t> DataSizeLoaded{0};
    std::atomic<size_t> IndexedRangesLoaded{0};
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
}

} // anonymous namespace

//-----------------------------------------------------------------------------

void ImportSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    auto log = std::make_unique<TLog>(CreateLogBackend("cerr", runConfig.LogPriority, true));
    auto* Log = log.get(); // to make LOG_* macros working

    auto connectionConfigCopy = connectionConfig;
    auto driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);

    LOG_I("Starting TPC-C data import for " << runConfig.WarehouseCount << " warehouses");

    auto startTs = TInstant::Now();

    LoadSmallTables(driver, runConfig.Path, runConfig.WarehouseCount, Log);

    TLoadState loadState;

    for (int warehouseId = 1; warehouseId <= runConfig.WarehouseCount; ++warehouseId) {
        LOG_T("Loading data for warehouse " << warehouseId);
        LoadRange(driver, runConfig.Path, warehouseId, warehouseId, loadState, Log);
    }

    auto endTs = TInstant::Now();
    auto delta = endTs - startTs;

    driver.Stop(true);

    LOG_I("TPC-C data import completed successfully in " << delta.Minutes());
}

} // namespace NYdb::NTPCC
