#include "import.h"

#include "constants.h"
#include "data_splitter.h"
#include "import_tui.h"
#include "log.h"
#include "log_backend.h"
#include "path_checker.h"
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

#include <google/protobuf/arena.h>

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

constexpr int UPSERT_MAX_RETRIES = 100;
constexpr int UPSERT_BACKOFF_MILLIS = 10;
constexpr int UPSERT_BACKOFF_CEILING = 5;

constexpr int INDEX_CHECK_MAX_RETRIES = 10;
constexpr int INDEX_CHECK_BACKOFF_MILLIS = 1000;
constexpr int INDEX_CHECK_BACKOFF_CEILING = 6;

constexpr auto INDEX_PROGRESS_CHECK_INTERVAL = std::chrono::seconds(1);

//-----------------------------------------------------------------------------

int GetBackoffWaitMs(int retryCount, int millis, int ceiling) {
    const int waitTimeCeilingMs = (1 << ceiling) * millis;
    int waitTimeMs = millis;

    for (int i = 0; i < retryCount; ++i) {
        waitTimeMs = std::min(waitTimeMs * 2, waitTimeCeilingMs);
    }

    // Add random jitter (0-99ms like in Java)
    return waitTimeMs + RandomNumber(0, 99);
}

//-----------------------------------------------------------------------------

// Generates a random string of length (strLen-1) to match Benchbase's TPCCUtil.randomStr behavior
TString RandomStringBenchbase(TReallyFastRng32& fastRng, int strLen, char baseChar = 'a') {
    if (strLen > 1) {
        int actualLength = strLen - 1;
        TString result;
        result.reserve(actualLength);

        for (int i = 0; i < actualLength; ++i) {
            result += static_cast<char>(baseChar + RandomNumber(fastRng, 0, 25));
        }
        return result;
    } else {
        return "";
    }
}

// Generates a random string with [a-z] characters
TString RandomAlphaString(TReallyFastRng32& fastRng, int minLength, int maxLength) {
    int length = RandomNumber(fastRng, minLength, maxLength);
    return RandomStringBenchbase(fastRng, length, 'a');
}

// Generates a random string with [A-Z] characters
TString RandomUpperAlphaString(TReallyFastRng32& fastRng, int minLength, int maxLength) {
    int length = RandomNumber(fastRng, minLength, maxLength);
    return RandomStringBenchbase(fastRng, length, 'A');
}

// Generates a random string with [09] characters
TString RandomNumericString(TReallyFastRng32& fastRng, int length) {
    TString result;
    result.reserve(length);

    for (int i = 0; i < length; ++i) {
        result += static_cast<char>('0' + RandomNumber(fastRng, 0, 9));
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

NTable::TBulkUpsertResult LoadItems(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading " << ITEM_COUNT << " items...");

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    for (int i = 1; i <= ITEM_COUNT; ++i) {
        TString data;
        int randPct = RandomNumber(fastRng, 1, 100);
        int len = RandomNumber(fastRng, 26, 50);
        if (randPct > 10) {
            // 90% of time i_data isa random string of length [26 .. 50]
            data = RandomStringBenchbase(fastRng, len);
        } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere in
            // middle
            int startORIGINAL = RandomNumber(fastRng, 2, len - 8);
            data = RandomStringBenchbase(fastRng, startORIGINAL) + "ORIGINAL" + RandomStringBenchbase(fastRng, len - startORIGINAL - 8);
        }

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("I_ID").Int32(i)
            .AddMember("I_NAME").Utf8(RandomAlphaString(fastRng, 14, 24))
            .AddMember("I_PRICE").Double(RandomNumber(fastRng, 100, 10000) / 100.0)
            .AddMember("I_DATA").Utf8(data)
            .AddMember("I_IM_ID").Int32(RandomNumber(fastRng, 1, 10000))
            .EndStruct();
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadWarehouses(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int startId,
    int lastId,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading warehouses " << startId << " to " << lastId);

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    for (int warehouseId = startId; warehouseId <= lastId; ++warehouseId) {
        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("W_ID").Int32(warehouseId)
            .AddMember("W_YTD").Double(DISTRICT_INITIAL_YTD * DISTRICT_COUNT)
            .AddMember("W_TAX").Double(RandomNumber(fastRng, 0, 2000) / 10000.0)
            .AddMember("W_NAME").Utf8(RandomAlphaString(fastRng, 6, 10))
            .AddMember("W_STREET_1").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("W_STREET_2").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("W_CITY").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("W_STATE").Utf8(RandomUpperAlphaString(fastRng, 3, 3))
            .AddMember("W_ZIP").Utf8("123456789")
            .EndStruct();
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadStock(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int itemId,
    int itemsToLoad,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading stock for warehouse " << wh << " items " << itemId << " to " << (itemId + itemsToLoad - 1));

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    for (int i = 0; i < itemsToLoad; ++i) {
        int currentItemId = itemId + i;

        // s_data - match Benchbase inline logic exactly
        TString data;
        int randPct = RandomNumber(fastRng, 1, 100);
        int len = RandomNumber(fastRng, 26, 50);
        if (randPct > 10) {
            // 90% of time i_data isa random string of length [26 ..
            // 50]
            data = RandomStringBenchbase(fastRng, len);
        } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere
            // in middle
            int startORIGINAL = RandomNumber(fastRng, 2, len - 8);
            data = RandomStringBenchbase(fastRng, startORIGINAL) + "ORIGINAL" + RandomStringBenchbase(fastRng, len - startORIGINAL - 8);
        }

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("S_W_ID").Int32(wh)
            .AddMember("S_I_ID").Int32(currentItemId)
            .AddMember("S_QUANTITY").Int32(RandomNumber(fastRng, 10, 100))
            .AddMember("S_ORDER_CNT").Int32(0)
            .AddMember("S_REMOTE_CNT").Int32(0)
            .AddMember("S_DATA").Utf8(data)
            .AddMember("S_DIST_01").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_02").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_03").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_04").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_05").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_06").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_07").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_08").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_09").Utf8(RandomStringBenchbase(fastRng, 24))
            .AddMember("S_DIST_10").Utf8(RandomStringBenchbase(fastRng, 24))
            .EndStruct();
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadDistricts(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int startId,
    int lastId,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading districts for warehouses " << startId << " to " << lastId);

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    for (int warehouseId = startId; warehouseId <= lastId; ++warehouseId) {
        for (int districtId = DISTRICT_LOW_ID; districtId <= DISTRICT_HIGH_ID; ++districtId) {
                    valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("D_W_ID").Int32(warehouseId)
            .AddMember("D_ID").Int32(districtId)
            .AddMember("D_YTD").Double(DISTRICT_INITIAL_YTD)
            .AddMember("D_TAX").Double(RandomNumber(fastRng, 0, 2000) / 10000.0)
            .AddMember("D_NEXT_O_ID").Int32(CUSTOMERS_PER_DISTRICT + 1)
            .AddMember("D_NAME").Utf8(RandomAlphaString(fastRng, 6, 10))
            .AddMember("D_STREET_1").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("D_STREET_2").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("D_CITY").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("D_STATE").Utf8(RandomUpperAlphaString(fastRng, 3, 3))
            .AddMember("D_ZIP").Utf8("123456789")
            .EndStruct();
        }
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadCustomers(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading customers for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    for (int customerId = 1; customerId <= CUSTOMERS_PER_DISTRICT; ++customerId) {
        TString last;
        if (customerId <= 1000) {
            last = GetLastName(customerId - 1);
        } else {
            last = GetNonUniformRandomLastNameForLoad();
        }

        TString credit = RandomNumber(fastRng, 1, 100) <= 10 ? "BC" : "GC";

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("C_W_ID").Int32(wh)
            .AddMember("C_D_ID").Int32(district)
            .AddMember("C_ID").Int32(customerId)
            .AddMember("C_DISCOUNT").Double(RandomNumber(fastRng, 1, 5000) / 10000.0)
            .AddMember("C_CREDIT").Utf8(credit)
            .AddMember("C_LAST").Utf8(last)
            .AddMember("C_FIRST").Utf8(RandomAlphaString(fastRng, 8, 16))
            .AddMember("C_CREDIT_LIM").Double(50000.00)
            .AddMember("C_BALANCE").Double(-10.00)
            .AddMember("C_YTD_PAYMENT").Double(10.00)
            .AddMember("C_PAYMENT_CNT").Int32(1)
            .AddMember("C_DELIVERY_CNT").Int32(0)
            .AddMember("C_STREET_1").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("C_STREET_2").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("C_CITY").Utf8(RandomAlphaString(fastRng, 10, 20))
            .AddMember("C_STATE").Utf8(RandomUpperAlphaString(fastRng, 3, 3))
            .AddMember("C_ZIP").Utf8(RandomNumericString(fastRng, 4) + "11111")
            .AddMember("C_PHONE").Utf8(RandomNumericString(fastRng, 16))
            .AddMember("C_SINCE").Timestamp(TInstant::Now())
            .AddMember("C_MIDDLE").Utf8("OE")
            .AddMember("C_DATA").Utf8(RandomAlphaString(fastRng, 300, 500))
            .EndStruct();
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadCustomerHistory(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    i64 baseTs,
    TLog* Log)
{
    LOG_T("Loading customer history for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    TInstant date = TInstant::Now();

    for (int customerId = 1; customerId <= CUSTOMERS_PER_DISTRICT; ++customerId) {
        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("H_C_W_ID").Int32(wh)
            .AddMember("H_C_ID").Int32(customerId)
            .AddMember("H_C_D_ID").Int32(district)
            .AddMember("H_D_ID").Int32(district)
            .AddMember("H_W_ID").Int32(wh)
            .AddMember("H_DATE").Timestamp(date)
            .AddMember("H_AMOUNT").Double(10.00)
            .AddMember("H_DATA").Utf8(RandomAlphaString(fastRng, 10, 24))
            .AddMember("H_C_NANO_TS").Int64(baseTs++)
            .EndStruct();
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadOpenOrders(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading open orders for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder(&arena);
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
        std::optional<int> carrierId;
        if (orderId < FIRST_UNPROCESSED_O_ID) {
            carrierId = RandomNumber(fastRng, 1, 10);
        }
        int olCnt = GetRandomCount(wh, orderId, district); // Deterministic count

        valueBuilder.AddListItem()
            .BeginStruct()
            .AddMember("O_W_ID").Int32(wh)
            .AddMember("O_D_ID").Int32(district)
            .AddMember("O_ID").Int32(orderId)
            .AddMember("O_C_ID").Int32(customerId)
            .AddMember("O_CARRIER_ID").OptionalInt32(carrierId)
            .AddMember("O_OL_CNT").Int32(olCnt)
            .AddMember("O_ALL_LOCAL").Int32(1)
            .AddMember("O_ENTRY_D").Timestamp(TInstant::Now())
            .EndStruct();
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadNewOrders(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    google::protobuf::Arena& arena,
    TLog* Log)
{
    LOG_T("Loading new orders for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder(&arena);
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

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

NTable::TBulkUpsertResult LoadOrderLines(
    NTable::TTableClient& client,
    const TString& tableFullPath,
    int wh,
    int district,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    LOG_T("Loading order lines for warehouse " << wh << " district " << district);

    auto valueBuilder = TValueBuilder(&arena);
    valueBuilder.BeginList();

    for (int orderId = 1; orderId <= CUSTOMERS_PER_DISTRICT; ++orderId) {
        int olCnt = GetRandomCount(wh, orderId, district); // Deterministic count

        // Order Line data
        for (int lineNumber = 1; lineNumber <= olCnt; ++lineNumber) {
            int itemId = RandomNumber(fastRng, 1, ITEM_COUNT);

            // Set OL_DELIVERY_D and OL_AMOUNT based on itemId condition (like Benchbase!)
            std::optional<TInstant> deliveryDate;
            double amount;
            if (orderId < FIRST_UNPROCESSED_O_ID) {
                deliveryDate = TInstant::Now();
                amount = 0.0;
            } else {
                // random within [0.01 .. 9,999.99]
                amount = RandomNumber(fastRng, 1, 999999) / 100.0;
            }

            valueBuilder.AddListItem()
                .BeginStruct()
                .AddMember("OL_W_ID").Int32(wh)
                .AddMember("OL_D_ID").Int32(district)
                .AddMember("OL_O_ID").Int32(orderId)
                .AddMember("OL_NUMBER").Int32(lineNumber)
                .AddMember("OL_I_ID").Int32(itemId)
                .AddMember("OL_DELIVERY_D").OptionalTimestamp(deliveryDate)
                .AddMember("OL_AMOUNT").Double(amount)
                .AddMember("OL_SUPPLY_W_ID").Int32(wh)
                .AddMember("OL_QUANTITY").Double(5.0)
                .AddMember("OL_DIST_INFO").Utf8(RandomStringBenchbase(fastRng, 24))
                .EndStruct();
        }
    }

    valueBuilder.EndList();

    NTable::TBulkUpsertSettings bulkSettings;
    bulkSettings.Arena(&arena);
    auto result = client.BulkUpsert(tableFullPath, valueBuilder.Build(), bulkSettings).ExtractValueSync();
    arena.Reset();
    return result;
}

//-----------------------------------------------------------------------------

template<typename LoadFunc>
void ExecuteWithRetry(const TString& operationName, LoadFunc loadFunc, google::protobuf::Arena& arena, TLog* Log) {
    for (int retryCount = 0; retryCount < UPSERT_MAX_RETRIES; ++retryCount) {
        if (GetGlobalInterruptSource().stop_requested()) {
            break;
        }

        auto result = loadFunc();
        if (result.IsSuccess()) {
            return;
        }

        arena.Reset();

        const auto status = result.GetStatus();
        bool shouldFail = status == EStatus::NOT_FOUND || status == EStatus::UNDETERMINED
            || status == EStatus::UNAUTHORIZED || status == EStatus::SCHEME_ERROR;
        if (shouldFail) {
            LOG_E(operationName << " failed: " << result.GetIssues().ToOneLineString());
            RequestStopWithError();
            return;
        }

        if (retryCount < UPSERT_MAX_RETRIES - 1) {
            int waitMs = GetBackoffWaitMs(retryCount, UPSERT_BACKOFF_MILLIS, UPSERT_BACKOFF_CEILING);
            LOG_T("Retrying " << operationName << " after " << waitMs << " ms due to: "
                    << result.GetStatus() << ", " << result.GetIssues().ToOneLineString());
            Sleep(TDuration::MilliSeconds(waitMs));
        } else {
            LOG_E(operationName << " failed after " << UPSERT_MAX_RETRIES << " retries: "
                    << result.GetIssues().ToOneLineString());
            RequestStopWithError();
            return;
        }
    }
}

//-----------------------------------------------------------------------------

void LoadSmallTables(
    TDriver& driver,
    const TString& path,
    int warehouseCount,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
    NTable::TTableClient tableClient(driver);

    TString itemTablePath = path + "/" + TABLE_ITEM;
    TString warehouseTablePath = path + "/" + TABLE_WAREHOUSE;
    TString districtTablePath = path + "/" + TABLE_DISTRICT;

    ExecuteWithRetry("LoadItems", [&]() {
        return LoadItems(tableClient, itemTablePath, arena, fastRng, Log);
    }, arena, Log);

    for (int wh = 1; wh <= warehouseCount; wh += MAX_WAREHOUSES_PER_IMPORT_BATCH) {
        int lastId = Min(wh + MAX_WAREHOUSES_PER_IMPORT_BATCH - 1, warehouseCount);
        ExecuteWithRetry("LoadWarehouses", [&]() {
            return LoadWarehouses(tableClient, warehouseTablePath, wh, lastId, arena, fastRng, Log);
        }, arena, Log);
        ExecuteWithRetry("LoadDistricts", [&]() {
            return LoadDistricts(tableClient, districtTablePath, wh, lastId, arena, fastRng, Log);
        }, arena, Log);
    }
}

//-----------------------------------------------------------------------------

void LoadRange(
    TDriver& driver,
    const TString& path,
    int whStart,
    int whEnd,
    TImportState& state,
    google::protobuf::Arena& arena,
    TReallyFastRng32& fastRng,
    TLog* Log)
{
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
                return LoadCustomers(tableClient, customerTablePath, wh, district, arena, fastRng, Log);
            }, arena, Log);
            ExecuteWithRetry("LoadOpenOrders", [&]() {
                return LoadOpenOrders(tableClient, oorderTablePath, wh, district, arena, fastRng, Log);
            }, arena, Log);
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
                return LoadStock(tableClient, stockTablePath, wh, startItemId, itemsToLoad, arena, fastRng, Log);
            }, arena, Log);
        }

        i64 prevTs = 0;
        for (int district = DISTRICT_LOW_ID; district <= DISTRICT_HIGH_ID; ++district) {
            ExecuteWithRetry("LoadOrderLines", [&]() {
                return LoadOrderLines(tableClient, orderLineTablePath, wh, district, arena, fastRng, Log);
            }, arena, Log);

            // Match Benchbase: ensure monotonic nanosecond timestamps
            i64 nanoTs = TInstant::Now().NanoSeconds();
            if (nanoTs <= prevTs) {
                nanoTs = prevTs + 1;
            }
            prevTs = nanoTs;

            ExecuteWithRetry("LoadCustomerHistory", [&]() {
                return LoadCustomerHistory(tableClient, historyTablePath, wh, district, arena, fastRng, nanoTs, Log);
            }, arena, Log);

            ExecuteWithRetry("LoadNewOrders", [&]() {
                return LoadNewOrders(tableClient, newOrderTablePath, wh, district, arena, Log);
            }, arena, Log);
        }

        state.DataSizeLoaded.fetch_add(perWhDatasize, std::memory_order_relaxed);
    }

    state.RangesLoaded.fetch_add(1);
}

//-----------------------------------------------------------------------------

bool IsOperationStarted(TStatus operationStatus) {
    return operationStatus.IsSuccess() || operationStatus.GetStatus() == EStatus::STATUS_UNDEFINED;
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
        if (IsOperationStarted(opResult.Status())) {
            operationId = opResult.Id();
            LOG_I("Started index creation for " << indexName << ": " << operationId.ToString());
            if (operationId.GetKind() == TOperation::TOperationId::BUILD_INDEX) {
                return TStatus(EStatus::SUCCESS, NIssue::TIssues());
            }
            return opResult.Status();
        } else {
            LOG_W("Failed to create index " << indexName << " for " << tablePath << ": "
                << opResult.ToString() << ", retrying");
            return opResult.Status();
        }
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

void CreateIndices(TDriver& driver, const TString& path, TImportState& loadState, TLog* Log) {
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
    const TOperation::TOperationId& id,
    TLog* Log) noexcept
{
    NYdb::TStatus lastStatus(EStatus::STATUS_UNDEFINED, NIssue::TIssues());
    for (int i = 0; i < INDEX_CHECK_MAX_RETRIES; ++i) {
        if (GetGlobalInterruptSource().stop_requested()) {
            break;
        }

        try {
            auto operation = client.Get<NTable::TBuildIndexOperation>(id).GetValueSync();
            lastStatus = operation.Status();
            if (operation.Ready()) {
                if (lastStatus.IsSuccess() && operation.Metadata().State == NTable::EBuildIndexState::Done) {
                    return 100;
                } else {
                    if (lastStatus.GetStatus() == EStatus::CANCELLED) {
                        TStringStream ss;
                        ss << "Index operation id " << id.ToString() << ", externally cancelled";
                        return std::unexpected(ss.Str());
                    }
                    // we don't check which kind of failure is in status,
                    // because we expect only retryable errors here
                    LOG_D("Failed to check index operation id: " << lastStatus);
                }
            } else {
                return operation.Metadata().Progress;
            }
        } catch (const std::exception& ex) {
            if (i + 1 < INDEX_CHECK_MAX_RETRIES) {
                LOG_W("Failed to check index operation " << id.ToString() << ": " << ex.what() << ", retrying");
            } else {
                TStringStream ss;
                ss << "Failed to check index operation id " << id.ToString() << ", exception: " << ex.what();
                return std::unexpected(ss.Str());
            }
        }

        int waitMs = GetBackoffWaitMs(i, INDEX_CHECK_BACKOFF_MILLIS, INDEX_CHECK_BACKOFF_CEILING);
        Sleep(TDuration::MilliSeconds(waitMs));
    }

    TStringStream ss;
    ss << "Failed to check index operation id " << id.ToString()
       << ", after " << INDEX_CHECK_MAX_RETRIES << " retries. "
       << "Last status: " << lastStatus;

    return std::unexpected(ss.Str());
}

//-----------------------------------------------------------------------------

void InterruptHandler(int) {
    GetGlobalInterruptSource().request_stop();
}

//-----------------------------------------------------------------------------

class TPCCLoader {
public:
    TPCCLoader(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig)
        : ConnectionConfig(connectionConfig)
        , Config(runConfig)
        , LogBackend(new TLogBackendWithCapture("cerr", runConfig.LogPriority, TUI_LOG_LINES))
        , Log(std::make_shared<TLog>(THolder(static_cast<TLogBackend*>(LogBackend))))
        , PreviousDataSizeLoaded(0)
        , StartTime(Clock::now())
        , LoadState(GetGlobalInterruptSource().get_token())
    {
        ConnectionConfig.IsNetworkIntensive = true;
        ConnectionConfig.UsePerChannelTcpConnection = true;
        ConnectionConfig.UseAllNodes = true;
    }

    void ImportSync() {
        if (Config.WarehouseCount == 0) {
            std::cerr << "Specified zero warehouses" << std::endl;
            std::exit(1);
        }

        CheckPathForImport(ConnectionConfig, Config.Path);

        Config.SetDisplay();
        CalculateApproximateDataSize();

        std::vector<TDriver> drivers;
        drivers.reserve(10);
        drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(ConnectionConfig));

        if (Config.LoadThreadCount == 0) {
            int32_t computeCores = 0;
            std::string reason;
            try {
                computeCores = NumberOfComputeCpus(drivers[0]);
            } catch (const std::exception& ex) {
                reason = ex.what();
            }

            if (computeCores == 0) {
                std::cerr << "Failed to autodetect max number of load threads";
                if (!reason.empty()) {
                    std::cerr << ": " << reason;
                }

                std::cerr << ". Please specify '--threads' manually." << std::endl;
                std::exit(1);
            }

            const size_t clientCpuCount = NumberOfMyCpus();

            const size_t optimalThreadCount =
                (computeCores + COMPUTE_CORES_PER_IMPORT_THREAD - 1) / COMPUTE_CORES_PER_IMPORT_THREAD;

            Config.LoadThreadCount = std::min(clientCpuCount, optimalThreadCount);
        }

        // TODO: detect number of threads
        size_t threadCount = std::min(Config.WarehouseCount, Config.LoadThreadCount);
        threadCount = std::max(threadCount, size_t(1));

        // we want to switch buffers and draw UI ASAP to properly display logs
        // produced after this point and before the first screen update
        if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
            LogBackend->StartCapture();
            TImportDisplayData dataToDisplay(LoadState);
            Tui = std::make_unique<TImportTui>(Log, Config, *LogBackend, dataToDisplay);
        }

#ifndef NDEBUG
        LOG_W("You're running a CLI binary built without NDEBUG defined, import will take much longer than expected");
#endif

        // TODO: calculate optimal number of drivers (but per thread looks good)
        size_t driverCount = threadCount;

        LOG_I("Starting TPC-C data import for " << Config.WarehouseCount << " warehouses using " <<
                threadCount << " threads and " << driverCount << " YDB drivers. Approximate data size: "
                << GetFormattedSize(LoadState.ApproximateDataSize));

        // already have 1, add more if needed
        drivers.reserve(driverCount);
        for (size_t i = 1; i < driverCount; ++i) {
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
                google::protobuf::Arena arena;
                TReallyFastRng32 fastRng(threadId * TInstant::Now().Seconds());
                auto& driver = drivers[threadId % driverCount];
                if (threadId == 0) {
                    LoadSmallTables(driver, Config.Path, Config.WarehouseCount, arena, fastRng, Log.get());
                } else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(threadId));
                }
                LoadRange(driver, Config.Path, whStart, whEnd, LoadState, arena, fastRng, Log.get());
            });
        }

        NOperation::TOperationClient operationClient(drivers[0]);

        Clock::time_point lastIndexProgressCheck = Clock::time_point::min();

        while (true) {
            if (GetGlobalInterruptSource().stop_requested()) {
                break;
            }

            if (LoadState.State == TImportState::ESUCCESS) {
                break;
            }

            auto now = Clock::now();

            try {
                UpdateDisplayIfNeeded(now);
            } catch (const std::exception& ex) {
                // in theory might happen when TUI loop has exited
                // and we haven't seen stop requested yet
                LOG_E("Exception while updating display: " << ex.what());
                GetGlobalInterruptSource().request_stop();
                break;
            }

            switch (LoadState.State) {
            case TImportState::ELOAD_INDEXED_TABLES: {
                // Check if all indexed ranges are loaded and start index creation
                size_t indexedRangesLoaded = LoadState.IndexedRangesLoaded.load(std::memory_order_relaxed);
                if (indexedRangesLoaded >= threadCount) {
                    CreateIndices(drivers[0], Config.Path, LoadState, Log.get());
                    for (const auto& state: LoadState.IndexBuildStates) {
                        if (state.Id.GetKind() == TOperation::TOperationId::UNUSED) {
                            GetGlobalInterruptSource().request_stop();
                            break;
                        }
                    }
                    if (GetGlobalInterruptSource().stop_requested()) {
                        break;
                    }

                    LOG_I("Indexed tables loaded, indices are being built in background. Continuing with remaining tables");
                    LoadState.State = TImportState::ELOAD_TABLES_BUILD_INDICES;
                    lastIndexProgressCheck = now;
                }
                break;
            }
            case TImportState::ELOAD_TABLES_BUILD_INDICES: {
                // Check if all ranges are loaded (work is complete)
                size_t rangesLoaded = LoadState.RangesLoaded.load(std::memory_order_relaxed);
                if (rangesLoaded >= threadCount) {
                    LOG_I("All tables loaded successfully. Waiting for indices to be ready");
                    LoadState.State = TImportState::EWAIT_INDICES;
                }
                [[fallthrough]];
            }
            case TImportState::EWAIT_INDICES: {
                auto timeSinceLastCheck = now - lastIndexProgressCheck;
                if (timeSinceLastCheck < INDEX_PROGRESS_CHECK_INTERVAL) {
                    break;
                }
                lastIndexProgressCheck = now;

                // update progress of all indices and advance LoadState.CurrentIndex
                for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                    auto& indexState = LoadState.IndexBuildStates[i];
                    auto progress = GetIndexProgress(operationClient, indexState.Id, Log.get());
                    if (!progress) {
                        LOG_E("Failed to build index " << indexState.Name <<  ": " << progress.error());
                        RequestStopWithError();
                        break;
                    }
                    indexState.Progress = *progress;
                    if (i == LoadState.CurrentIndex && indexState.Progress == 100.0) {
                        ++LoadState.CurrentIndex;
                    }
                }
                if (GetGlobalInterruptSource().stop_requested()) {
                    break;
                }

                if (LoadState.State == TImportState::EWAIT_INDICES && LoadState.CurrentIndex >= LoadState.IndexBuildStates.size()) {
                    LOG_I("Indices created successfully");
                    LoadState.State = TImportState::ESUCCESS;
                    continue;
                }
                break;
            }
            case TImportState::ESUCCESS:
                break;
            }

            std::this_thread::sleep_for(TRunConfig::SleepMsEveryIterationMainLoop);
            now = Clock::now();
        }

        if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
            ExitTuiMode();
        }

        if (GetGlobalInterruptSource().stop_requested()) {
            LOG_I("Stop requested, waiting for threads to finish");
        }

        for (auto& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        // if there is either error we have failed to retry or user wants to cancel import,
        // we must try to cancel indices being created
        if (GetGlobalInterruptSource().stop_requested() && !LoadState.IndexBuildStates.empty()) {
            for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                auto& indexState = LoadState.IndexBuildStates[i];
                try {
                    auto operation = operationClient.Get<NTable::TBuildIndexOperation>(indexState.Id).GetValueSync();
                    if (operation.Metadata().State == NTable::EBuildIndexState::TransferData) {
                        auto cancelResult = operationClient.Cancel(indexState.Id).GetValueSync();
                        if (cancelResult.IsSuccess()) {
                            LOG_I("Cancelled creation of index '" << indexState.Name
                                << "' for table '" << indexState.Table
                                << "', operation id '" << indexState.Id.ToString() << "'");
                        } else {
                            LOG_W("Failed to cancel creation of index '" << indexState.Name
                                << "' for table '" << indexState.Table
                                << "', operation id '" << indexState.Id.ToString() << "'");
                        }
                    }
                } catch (const std::exception& ex) {
                    LOG_W("Exception while cancelling index '" << indexState.Name << "' for table '"
                        << indexState.Table << "', operation id '" << indexState.Id.ToString() << "': "
                        << ex.what());
                }
            }
        }

        for (auto& driver : drivers) {
            driver.Stop(true);
        }

        auto endTs = TInstant::Now();
        auto duration = endTs - startTs;

        if (LoadState.State == TImportState::ESUCCESS) {
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
        } else {
            ythrow yexception() << "either there was a critical error or user cancelled the import. See the logs.";
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
        TImportDisplayData displayData(LoadState);

        displayData.StatusData.IsWaitingForIndices =
            !LoadState.IndexBuildStates.empty() && LoadState.State == TImportState::EWAIT_INDICES;

            displayData.StatusData.IsLoadingTablesAndBuildingIndices =
                LoadState.State == TImportState::ELOAD_TABLES_BUILD_INDICES;

        auto totalElapsed = std::chrono::duration<double>(now - StartTime).count();
        displayData.StatusData.ElapsedMinutes = static_cast<int>(totalElapsed / 60);
        displayData.StatusData.ElapsedSeconds = static_cast<int>(totalElapsed) % 60;

        if (!displayData.StatusData.IsWaitingForIndices) {
            displayData.StatusData.CurrentDataSizeLoaded = LoadState.DataSizeLoaded.load(std::memory_order_relaxed);

            displayData.StatusData.PercentLoaded = LoadState.ApproximateDataSize > 0 ?
                (static_cast<double>(
                    displayData.StatusData.CurrentDataSizeLoaded) / LoadState.ApproximateDataSize) * 100.0 : 0.0;

            auto deltaSeconds = std::chrono::duration<double>(delta).count();
            displayData.StatusData.InstantSpeedMiBs = deltaSeconds > 0 ?
                static_cast<double>(
                    displayData.StatusData.CurrentDataSizeLoaded - PreviousDataSizeLoaded) / (1024 * 1024) / deltaSeconds : 0.0;

            displayData.StatusData.AvgSpeedMiBs = totalElapsed > 0 ?
                static_cast<double>(displayData.StatusData.CurrentDataSizeLoaded) / (1024 * 1024) / totalElapsed : 0.0;

            if (displayData.StatusData.AvgSpeedMiBs > 0
                    && displayData.StatusData.CurrentDataSizeLoaded < LoadState.ApproximateDataSize) {
                double remainingBytes = LoadState.ApproximateDataSize - displayData.StatusData.CurrentDataSizeLoaded;
                double remainingSeconds = remainingBytes / (1024 * 1024) / displayData.StatusData.AvgSpeedMiBs;
                displayData.StatusData.EstimatedTimeLeftMinutes = static_cast<int>(remainingSeconds / 60);
                displayData.StatusData.EstimatedTimeLeftSeconds = static_cast<int>(remainingSeconds) % 60;
            }

            PreviousDataSizeLoaded = displayData.StatusData.CurrentDataSizeLoaded;
        } else {
            // we still want to display that data is loaded while we are waiting for indices
            displayData.StatusData.CurrentDataSizeLoaded = LoadState.DataSizeLoaded.load(std::memory_order_relaxed);
            displayData.StatusData.PercentLoaded = 100;

            double remainingSeconds = 0;
            for (size_t i = 0; i < LoadState.IndexBuildStates.size(); ++i) {
                const auto& indexState = LoadState.IndexBuildStates[i];
                remainingSeconds = std::max(remainingSeconds, indexState.GetRemainingSeconds());
            }
            displayData.StatusData.EstimatedTimeLeftMinutes = static_cast<int>(remainingSeconds / 60);
            displayData.StatusData.EstimatedTimeLeftSeconds = static_cast<int>(remainingSeconds) % 60;
        }

        switch (Config.DisplayMode) {
        case TRunConfig::EDisplayMode::Text:
            UpdateDisplayTextMode(displayData.StatusData);
            break;
        case TRunConfig::EDisplayMode::Tui:
            Tui->Update(displayData);
            break;
        default:
            ;
        }

        LastDisplayUpdate = now;
    }

    void UpdateDisplayTextMode(const TImportStatusData& data) {
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

    void ExitTuiMode() {
        Tui.reset();
        LogBackend->StopCaptureAndFlush(Cerr);
    }

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;

    // XXX Log instance owns LogBackend (unfortunately, it accepts THolder with LogBackend)
    TLogBackendWithCapture* LogBackend;
    std::shared_ptr<TLog> Log;

    Clock::time_point LastDisplayUpdate;
    size_t PreviousDataSizeLoaded;
    Clock::time_point StartTime;
    TImportState LoadState;

    std::unique_ptr<TImportTui> Tui;
};

} // anonymous namespace

//-----------------------------------------------------------------------------

void ImportSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    try {
        TPCCLoader loader(connectionConfig, runConfig);
        loader.ImportSync();
    } catch (const std::exception& ex) {
        std::cerr << "Exception while execution: " << ex.what() << std::endl;
        throw NConsoleClient::TNeedToExitWithCode(EXIT_FAILURE);
    }
}

} // namespace NYdb::NTPCC
