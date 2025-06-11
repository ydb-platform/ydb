#include "data_splitter.h"

#include "constants.h"

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <algorithm>
#include <unordered_map>

// it follows the logic of
// https://github.com/ydb-platform/benchhelpers/blob/main/tpcc/ydb/tpcc_helper.py

namespace NYdb::NTPCC {

namespace {
    // Constants from tpcc_helper.py
    constexpr int DEFAULT_MIN_PARTITIONS = 50;
    constexpr int DEFAULT_MIN_WAREHOUSES_PER_SHARD = 100;

    constexpr int MIN_ITEMS_PER_SHARD = 100;

    // PER_WAREHOUSE_MB mapping for heavy tables
    const std::unordered_map<TString, double> PER_WAREHOUSE_MB = {
        {TABLE_STOCK, 45.0},
        {TABLE_CUSTOMER, 20.1},
        {TABLE_ORDER_LINE, 35.0},
        {TABLE_HISTORY, 2.4},
        {TABLE_OORDER, 1.5}
    };
}

int TDataSplitter::CalcMinParts(int warehouseCount) {
    return std::max(DEFAULT_MIN_PARTITIONS, warehouseCount / DEFAULT_MIN_WAREHOUSES_PER_SHARD);
}

// Static methods to expose constants
int TDataSplitter::GetDefaultMinPartitions() {
    return DEFAULT_MIN_PARTITIONS;
}

int TDataSplitter::GetDefaultMinWarehousesPerShard() {
    return DEFAULT_MIN_WAREHOUSES_PER_SHARD;
}

int TDataSplitter::GetMinItemsPerShard() {
    return MIN_ITEMS_PER_SHARD;
}

double TDataSplitter::GetPerWarehouseMB(const TString& table) {
    auto it = PER_WAREHOUSE_MB.find(table);
    return it != PER_WAREHOUSE_MB.end() ? it->second : 0.0;
}

const std::unordered_map<TString, double>& TDataSplitter::GetPerWarehouseMBMap() {
    return PER_WAREHOUSE_MB;
}

std::vector<int> TDataSplitter::GetSplitKeys(const TString& table) const {
    int minShardCount = CalcMinParts(WarehouseCount);

    if (table == TABLE_ITEM) {
        // Special case for item table - no warehouse id in primary key
        int itemsPerShard = ITEM_COUNT / minShardCount;
        itemsPerShard = std::max(MIN_ITEMS_PER_SHARD, itemsPerShard);

        std::vector<int> splitKeys;
        int curItem = itemsPerShard;
        while (curItem < ITEM_COUNT) {
            splitKeys.push_back(curItem);
            curItem += itemsPerShard;
        }
        return splitKeys;
    }

    int warehousesPerShard;
    auto it = PER_WAREHOUSE_MB.find(table);
    if (it != PER_WAREHOUSE_MB.end()) {
        // Heavy table - calculate based on size
        double mbPerWh = it->second;
        warehousesPerShard = static_cast<int>((DEFAULT_SHARD_SIZE_MB + mbPerWh - 1) / mbPerWh);
        int warehousesPerShard2 = (WarehouseCount + minShardCount - 1) / minShardCount;
        warehousesPerShard = std::min(warehousesPerShard, warehousesPerShard2);
    } else {
        // Light table - calculate based on min shard count
        warehousesPerShard = (WarehouseCount + minShardCount - 1) / minShardCount;
    }

    if (warehousesPerShard < 2) {
        return {};
    }

    std::vector<int> splitKeys;
    // First warehouse id is 1
    int currentSplitKey = 1 + warehousesPerShard;
    while (currentSplitKey < WarehouseCount + 1) {
        splitKeys.push_back(currentSplitKey);
        currentSplitKey += warehousesPerShard;
    }

    return splitKeys;
}

TString TDataSplitter::GetSplitKeysString(const TString& table) const {
    auto splitKeys = GetSplitKeys(table);

    if (splitKeys.empty()) {
        return "";
    }

    TStringBuilder result;
    for (size_t i = 0; i < splitKeys.size(); ++i) {
        if (i > 0) {
            result << ",";
        }
        result << splitKeys[i];
    }

    return result;
}

} // namespace NYdb::NTPCC
