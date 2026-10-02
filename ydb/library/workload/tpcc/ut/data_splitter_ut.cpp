#include <ydb/library/workload/tpcc/data_splitter.h>
#include <ydb/library/workload/tpcc/constants.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>

using namespace NYdb;
using namespace NYdb::NTPCC;

Y_UNIT_TEST_SUITE(TDataSplitterTest) {

    Y_UNIT_TEST(ShouldCalculateMinPartsCorrectly) {
        // Test CalcMinParts static method
        int defaultMinPartitions = TDataSplitter::GetDefaultMinPartitions();
        int defaultMinWarehousesPerShard = TDataSplitter::GetDefaultMinWarehousesPerShard();

        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::CalcMinParts(1), defaultMinPartitions);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::CalcMinParts(100), defaultMinPartitions);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::CalcMinParts(1000), defaultMinPartitions);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::CalcMinParts(5000), defaultMinPartitions); // 5000/100 = 50
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::CalcMinParts(10000), 10000 / defaultMinWarehousesPerShard); // 10000/100 = 100
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::CalcMinParts(20000), 20000 / defaultMinWarehousesPerShard); // 20000/100 = 200
    }

    Y_UNIT_TEST(ShouldHandleSingleWarehouse) {
        TDataSplitter splitter(1);
        int minShardCount = TDataSplitter::CalcMinParts(1);
        int minItemsPerShard = TDataSplitter::GetMinItemsPerShard();

        // For 1 warehouse, most tables should have no split keys (single shard)

        // Item table - special case, depends on ITEM_COUNT
        auto itemSplits = splitter.GetSplitKeys(TABLE_ITEM);
        int itemsPerShard = ITEM_COUNT / minShardCount;
        itemsPerShard = std::max(minItemsPerShard, itemsPerShard);
        int expectedItemSplits = (ITEM_COUNT - 1) / itemsPerShard;
        UNIT_ASSERT_VALUES_EQUAL(itemSplits.size(), expectedItemSplits);
        if (!itemSplits.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(itemSplits[0], itemsPerShard);
        }

        // Heavy tables - should have no splits for 1 warehouse
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_STOCK).empty());
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_CUSTOMER).empty());
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_ORDER_LINE).empty());
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_HISTORY).empty());
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_OORDER).empty());

        // Light tables - should have no splits for 1 warehouse
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_WAREHOUSE).empty());
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_DISTRICT).empty());
        UNIT_ASSERT(splitter.GetSplitKeys(TABLE_NEW_ORDER).empty());
    }

    Y_UNIT_TEST(ShouldHandle1000Warehouses) {
        TDataSplitter splitter(1000);
        int minShardCount = TDataSplitter::CalcMinParts(1000);
        int minItemsPerShard = TDataSplitter::GetMinItemsPerShard();

        // Item table
        auto itemSplits = splitter.GetSplitKeys(TABLE_ITEM);
        int itemsPerShard = ITEM_COUNT / minShardCount;
        itemsPerShard = std::max(minItemsPerShard, itemsPerShard);
        int expectedItemSplits = (ITEM_COUNT - 1) / itemsPerShard;
        UNIT_ASSERT(expectedItemSplits >= 0);
        UNIT_ASSERT_VALUES_EQUAL(itemSplits.size(), expectedItemSplits);

        // Heavy tables - check based on PER_WAREHOUSE_MB
        // TABLE_STOCK
        auto stockSplits = splitter.GetSplitKeys(TABLE_STOCK);
        double stockMbPerWh = TDataSplitter::GetPerWarehouseMB(TABLE_STOCK);
        int stockWarehousesPerShard = static_cast<int>(std::ceil(DEFAULT_SHARD_SIZE_MB / stockMbPerWh));
        int stockWarehousesPerShard2 = (1000 + minShardCount - 1) / minShardCount;
        stockWarehousesPerShard = std::min(stockWarehousesPerShard, stockWarehousesPerShard2);
        int expectedStockSplits = (1000 - 1) / stockWarehousesPerShard;
        UNIT_ASSERT(expectedStockSplits > 0);
        UNIT_ASSERT_VALUES_EQUAL(stockSplits.size(), expectedStockSplits);
        if (!stockSplits.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(stockSplits[0], 1 + stockWarehousesPerShard);
        }

        // TABLE_CUSTOMER
        auto customerSplits = splitter.GetSplitKeys(TABLE_CUSTOMER);
        double customerMbPerWh = TDataSplitter::GetPerWarehouseMB(TABLE_CUSTOMER);
        int customerWarehousesPerShard = static_cast<int>(std::ceil(DEFAULT_SHARD_SIZE_MB / customerMbPerWh));
        int customerWarehousesPerShard2 = (1000 + minShardCount - 1) / minShardCount;
        customerWarehousesPerShard = std::min(customerWarehousesPerShard, customerWarehousesPerShard2);
        int expectedCustomerSplits = (1000 - 1) / customerWarehousesPerShard;
        UNIT_ASSERT(expectedCustomerSplits > 0);
        UNIT_ASSERT_VALUES_EQUAL(customerSplits.size(), expectedCustomerSplits);

        // Light tables
        auto warehouseSplits = splitter.GetSplitKeys(TABLE_WAREHOUSE);
        int lightWarehousesPerShard = (1000 + minShardCount - 1) / minShardCount;
        int expectedLightSplits = (1000 - 1) / lightWarehousesPerShard;
        UNIT_ASSERT(expectedLightSplits > 0);
        UNIT_ASSERT_VALUES_EQUAL(warehouseSplits.size(), expectedLightSplits);
    }

    Y_UNIT_TEST(ShouldHandle20000Warehouses) {
        TDataSplitter splitter(20000);
        int minShardCount = TDataSplitter::CalcMinParts(20000);
        int minItemsPerShard = TDataSplitter::GetMinItemsPerShard();

        // Item table
        auto itemSplits = splitter.GetSplitKeys(TABLE_ITEM);
        int itemsPerShard = ITEM_COUNT / minShardCount;
        itemsPerShard = std::max(minItemsPerShard, itemsPerShard);
        int expectedItemSplits = (ITEM_COUNT - 1) / itemsPerShard;
        UNIT_ASSERT_VALUES_EQUAL(itemSplits.size(), expectedItemSplits);
        if (!itemSplits.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(itemSplits[0], itemsPerShard);
        }

        // Heavy tables
        // TABLE_ORDER_LINE
        auto orderLineSplits = splitter.GetSplitKeys(TABLE_ORDER_LINE);
        double orderLineMbPerWh = TDataSplitter::GetPerWarehouseMB(TABLE_ORDER_LINE);
        int orderLineWarehousesPerShard = static_cast<int>(std::ceil(DEFAULT_SHARD_SIZE_MB / orderLineMbPerWh));
        int orderLineWarehousesPerShard2 = (20000 + minShardCount - 1) / minShardCount;
        orderLineWarehousesPerShard = std::min(orderLineWarehousesPerShard, orderLineWarehousesPerShard2);
        int expectedOrderLineSplits = (20000 - 1) / orderLineWarehousesPerShard;
        UNIT_ASSERT_VALUES_EQUAL(orderLineSplits.size(), expectedOrderLineSplits);
        if (!orderLineSplits.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(orderLineSplits[0], 1 + orderLineWarehousesPerShard);
        }

        // TABLE_HISTORY
        auto historySplits = splitter.GetSplitKeys(TABLE_HISTORY);
        double historyMbPerWh = TDataSplitter::GetPerWarehouseMB(TABLE_HISTORY);
        int historyWarehousesPerShard = static_cast<int>(std::ceil(DEFAULT_SHARD_SIZE_MB / historyMbPerWh));
        int historyWarehousesPerShard2 = (20000 + minShardCount - 1) / minShardCount;
        historyWarehousesPerShard = std::min(historyWarehousesPerShard, historyWarehousesPerShard2);
        int expectedHistorySplits = (20000 - 1) / historyWarehousesPerShard;
        UNIT_ASSERT_VALUES_EQUAL(historySplits.size(), expectedHistorySplits);

        // Light tables
        auto districtSplits = splitter.GetSplitKeys(TABLE_DISTRICT);
        int lightWarehousesPerShard = (20000 + minShardCount - 1) / minShardCount;
        int expectedLightSplits = (20000 - 1) / lightWarehousesPerShard;
        UNIT_ASSERT_VALUES_EQUAL(districtSplits.size(), expectedLightSplits);
    }

    Y_UNIT_TEST(ShouldGenerateCorrectSplitKeySequence) {
        TDataSplitter splitter(100);

        // Test with a light table that should have splits
        auto splits = splitter.GetSplitKeys(TABLE_WAREHOUSE);

        if (!splits.empty()) {
            // Verify splits are in ascending order
            for (size_t i = 1; i < splits.size(); ++i) {
                UNIT_ASSERT(splits[i] > splits[i-1]);
            }

            // Verify first split is > 1 (first warehouse ID)
            UNIT_ASSERT(splits[0] > 1);

            // Verify last split is < warehouse count + 1
            UNIT_ASSERT(splits.back() < 101);
        }
    }

    Y_UNIT_TEST(ShouldGenerateConsistentStringRepresentation) {
        TDataSplitter splitter(50);

        auto splits = splitter.GetSplitKeys(TABLE_CUSTOMER);
        auto splitString = splitter.GetSplitKeysString(TABLE_CUSTOMER);

        if (splits.empty()) {
            UNIT_ASSERT(splitString.empty());
        } else {
            // Verify string contains correct number of commas
            int commaCount = 0;
            for (char c : splitString) {
                if (c == ',') commaCount++;
            }
            UNIT_ASSERT_VALUES_EQUAL(commaCount, static_cast<int>(splits.size()) - 1);

            // Verify first number matches
            int firstNumber = std::stoi(splitString.substr(0, splitString.find(',')));
            UNIT_ASSERT_VALUES_EQUAL(firstNumber, splits[0]);
        }
    }

    Y_UNIT_TEST(ShouldHandleEdgeCases) {
        // Test with very small warehouse count where splits are not feasible
        TDataSplitter splitter1(2);
        auto splits1 = splitter1.GetSplitKeys(TABLE_WAREHOUSE);
        int minShardCount = TDataSplitter::CalcMinParts(2);
        int warehousesPerShard = (2 + minShardCount - 1) / minShardCount;
        // Since warehousesPerShard < 2, should return empty (verified by the logic)
        UNIT_ASSERT(splits1.empty());
        UNIT_ASSERT(warehousesPerShard < 2); // Verify our assumption

        // Test unknown table name
        TDataSplitter splitter2(1000);
        auto unknownSplits = splitter2.GetSplitKeys("unknown_table");
        // Unknown table should be treated as light table
        UNIT_ASSERT(!unknownSplits.empty());

        // Test that unknown table returns 0.0 for MB per warehouse
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetPerWarehouseMB("unknown_table"), 0.0);

        // Test boundary case - exactly at min warehouse threshold
        int minWarehousesPerShard = TDataSplitter::GetDefaultMinWarehousesPerShard();
        TDataSplitter splitterBoundary(minWarehousesPerShard * 2); // Should create 2 splits
        auto boundaryWarehouseSplits = splitterBoundary.GetSplitKeys(TABLE_WAREHOUSE);
        UNIT_ASSERT(!boundaryWarehouseSplits.empty());
    }

    Y_UNIT_TEST(ShouldValidateSplitKeyProperties) {
        // Test split key properties for different warehouse counts
        std::vector<int> warehouseCounts = {100, 500, 1000, 5000, 10000};

        for (int warehouses : warehouseCounts) {
            TDataSplitter splitter(warehouses);

            // Test light table splits
            auto warehouseSplits = splitter.GetSplitKeys(TABLE_WAREHOUSE);
            if (!warehouseSplits.empty()) {
                // First split should be > 1
                UNIT_ASSERT(warehouseSplits[0] > 1);
                // Last split should be < warehouses + 1
                UNIT_ASSERT(warehouseSplits.back() <= warehouses);
                // All splits should be unique and ascending
                for (size_t i = 1; i < warehouseSplits.size(); ++i) {
                    UNIT_ASSERT(warehouseSplits[i] > warehouseSplits[i-1]);
                }
            }

            // Test heavy table splits
            auto stockSplits = splitter.GetSplitKeys(TABLE_STOCK);
            if (!stockSplits.empty()) {
                UNIT_ASSERT(stockSplits[0] > 1);
                UNIT_ASSERT(stockSplits.back() <= warehouses);
                for (size_t i = 1; i < stockSplits.size(); ++i) {
                    UNIT_ASSERT(stockSplits[i] > stockSplits[i-1]);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldCalculateCorrectTableSizes) {
        // Test approximate table sizes for different warehouse counts
        const auto& perWarehouseMB = TDataSplitter::GetPerWarehouseMBMap();

        // For 1000 warehouses, calculate expected sizes using API
        TDataSplitter splitter(1000);

        // Verify that heavy tables get split appropriately based on size
        auto stockSplits = splitter.GetSplitKeys(TABLE_STOCK);
        auto customerSplits = splitter.GetSplitKeys(TABLE_CUSTOMER);
        auto orderLineSplits = splitter.GetSplitKeys(TABLE_ORDER_LINE);
        auto historySplits = splitter.GetSplitKeys(TABLE_HISTORY);
        auto oorderSplits = splitter.GetSplitKeys(TABLE_OORDER);

        // All heavy tables should have splits for 1000 warehouses
        UNIT_ASSERT(!stockSplits.empty());
        UNIT_ASSERT(!customerSplits.empty());
        UNIT_ASSERT(!orderLineSplits.empty());
        UNIT_ASSERT(!historySplits.empty());
        UNIT_ASSERT(!oorderSplits.empty());

        // Verify that the number of shards makes sense given the size constraints
        // Each shard should be approximately DEFAULT_SHARD_SIZE_MB (2000 MB)
        double stockMbPerWh = TDataSplitter::GetPerWarehouseMB(TABLE_STOCK);
        double totalStockMB = 1000 * stockMbPerWh;
        int stockShards = stockSplits.size() + 1;
        int expectedStockShards = static_cast<int>(std::ceil(totalStockMB / DEFAULT_SHARD_SIZE_MB));
        int minShardCount = TDataSplitter::CalcMinParts(1000);

        // Verify the shard count is reasonable
        UNIT_ASSERT(stockShards >= expectedStockShards || stockShards >= minShardCount);

        // Test that all heavy tables are in the mapping
        UNIT_ASSERT(perWarehouseMB.find(TABLE_STOCK) != perWarehouseMB.end());
        UNIT_ASSERT(perWarehouseMB.find(TABLE_CUSTOMER) != perWarehouseMB.end());
        UNIT_ASSERT(perWarehouseMB.find(TABLE_ORDER_LINE) != perWarehouseMB.end());
        UNIT_ASSERT(perWarehouseMB.find(TABLE_HISTORY) != perWarehouseMB.end());
        UNIT_ASSERT(perWarehouseMB.find(TABLE_OORDER) != perWarehouseMB.end());

        // Test that light tables are not in the mapping
        UNIT_ASSERT(perWarehouseMB.find(TABLE_WAREHOUSE) == perWarehouseMB.end());
        UNIT_ASSERT(perWarehouseMB.find(TABLE_DISTRICT) == perWarehouseMB.end());
        UNIT_ASSERT(perWarehouseMB.find(TABLE_NEW_ORDER) == perWarehouseMB.end());
    }

    Y_UNIT_TEST(ShouldExposeConstantsCorrectly) {
        // Test that exposed constants have expected values
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetDefaultMinPartitions(), 50);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetDefaultMinWarehousesPerShard(), 100);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetMinItemsPerShard(), 100);

        // Test specific table sizes
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetPerWarehouseMB(TABLE_STOCK), 45.0);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetPerWarehouseMB(TABLE_CUSTOMER), 20.1);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetPerWarehouseMB(TABLE_ORDER_LINE), 35.0);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetPerWarehouseMB(TABLE_HISTORY), 2.4);
        UNIT_ASSERT_VALUES_EQUAL(TDataSplitter::GetPerWarehouseMB(TABLE_OORDER), 1.5);
    }

    Y_UNIT_TEST(ShouldHandleItemTableCorrectly) {
        // Dedicated test for item table logic since it's special

        // Test with various warehouse counts
        std::vector<int> warehouseCounts = {1, 100, 1000, 20000};

        for (int warehouses : warehouseCounts) {
            TDataSplitter splitter(warehouses);
            int minShardCount = TDataSplitter::CalcMinParts(warehouses);
            int minItemsPerShard = TDataSplitter::GetMinItemsPerShard();

            auto itemSplits = splitter.GetSplitKeys(TABLE_ITEM);

            int itemsPerShard = ITEM_COUNT / minShardCount;
            itemsPerShard = std::max(minItemsPerShard, itemsPerShard);

            if (itemsPerShard >= ITEM_COUNT) {
                // Single shard case
                UNIT_ASSERT(itemSplits.empty());
            } else {
                // Multiple shards
                UNIT_ASSERT(!itemSplits.empty());

                // Verify split points
                int expectedSplits = (ITEM_COUNT - 1) / itemsPerShard;
                UNIT_ASSERT_VALUES_EQUAL(itemSplits.size(), expectedSplits);

                // Verify first split
                UNIT_ASSERT_VALUES_EQUAL(itemSplits[0], itemsPerShard);

                // Verify split sequence
                for (size_t i = 0; i < itemSplits.size(); ++i) {
                    int expectedSplit = itemsPerShard * (i + 1);
                    UNIT_ASSERT_VALUES_EQUAL(itemSplits[i], expectedSplit);
                    UNIT_ASSERT(itemSplits[i] < ITEM_COUNT);
                }
            }
        }
    }
}
