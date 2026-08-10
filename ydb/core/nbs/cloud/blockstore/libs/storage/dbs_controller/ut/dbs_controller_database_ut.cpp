#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_database.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_schema.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

/*
 * +----+------+----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 | Node 1 |
 * |    |      | N1 +----+           +-----+-----+--------+
 * |    |      |    | PB | --------> | SL1 | PD1 | Node 2 |
 * |    | DBG1 +----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 |        |
 * |    |      | N2 +----+           +-----+-----+ Node 3 |
 * |    |      |    | PB | --------> | SL1 | PD2 |        |
 * | P0 +------+----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 |        |
 * |    |      | N1 +----+           +-----+-----+ Node 4 |
 * |    |      |    | PB | --------> | SL1 | PD2 |        |
 * |    | DBG2 +----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 |        |
 * |    |      | N2 +----+           +-----+-----+        |
 * |    |      |    | PB | --------> | SL1 | PD2 | Node 5 |
 * +----+------+----+----+           +-----+-----+        |
 * |    |      |    | DD | --------> | SL1 | PD3 |        |
 * |    |      | N1 +----+           +-----+-----+--------+
 * |    |      |    | PB | --------> | SL1 | PD1 | Node 6 |
 * |    | DBG1 +----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 |        |
 * |    |      | N2 +----+           +-----+-----+ Node 7 |
 * |    |      |    | PB | --------> | SL1 | PD2 |        |
 * | P1 +------+----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 |        |
 * |    |      | N1 +----+           +-----+-----+ Node 8 |
 * |    |      |    | PB | --------> | SL1 | PD2 |        |
 * |    | DBG2 +----+----+           +-----+-----+--------+
 * |    |      |    | DD | --------> | SL1 | PD1 |        |
 * |    |      | N2 +----+           +-----+-----+ Node 9 |
 * |    |      |    | PB | --------> | SL1 | PD2 |        |
 * +----+------+----+----+           +-----+-----+--------+
 */
static TVector<NProto::TPartitionDDisks> MakePartitionsDDisks()
{
    const auto addRecord = [](auto* dbg,
                              std::tuple<ui32, ui32, ui32> ddiskId,
                              std::tuple<ui32, ui32, ui32> pBufferId)
    {
        auto* ddiskIds = dbg->AddDDiskIds();
        auto* ddisk = ddiskIds->MutableDDisk();
        ddisk->SetNodeId(std::get<0>(ddiskId));
        ddisk->SetPDiskId(std::get<1>(ddiskId));
        ddisk->SetDDiskSlotId(std::get<2>(ddiskId));
        auto* pBuffer = ddiskIds->MutablePersistentBuffer();
        pBuffer->SetNodeId(std::get<0>(pBufferId));
        pBuffer->SetPDiskId(std::get<1>(pBufferId));
        pBuffer->SetDDiskSlotId(std::get<2>(pBufferId));
    };

    TVector<NProto::TPartitionDDisks> payload(2);
    // P #1, DBG #1
    {
        auto* dbg = payload[0].AddDirectBlockGroupsDDisks();
        addRecord(dbg, {1, 1, 1}, {2, 1, 1});
        addRecord(dbg, {3, 1, 1}, {3, 2, 1});
    }
    // P #1, DBG #2
    {
        auto* dbg = payload[0].AddDirectBlockGroupsDDisks();
        addRecord(dbg, {4, 1, 1}, {4, 2, 1});
        addRecord(dbg, {5, 1, 1}, {5, 2, 1});
    }
    // P #2, DBG #1
    {
        auto* dbg = payload[1].AddDirectBlockGroupsDDisks();
        addRecord(dbg, {5, 3, 1}, {6, 1, 1});
        addRecord(dbg, {7, 1, 1}, {7, 2, 1});
    }
    // P #2, DBG #2
    {
        auto* dbg = payload[1].AddDirectBlockGroupsDDisks();
        addRecord(dbg, {8, 1, 1}, {8, 2, 1});
        addRecord(dbg, {9, 1, 1}, {9, 2, 1});
    }

    return payload;
}

Y_UNIT_TEST_SUITE(TDbsControllerDatabaseTest)
{
    Y_UNIT_TEST(ShouldInitSchema)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);
                partitionDb.InitSchema();
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);
                const auto it =
                    partitionDb.Table<TDbsControllerSchema::DDiskMap>()
                        .All()
                        .Select();
                UNIT_ASSERT(it.IsReady());
                UNIT_ASSERT(!it.IsValid());   // must be 0 rows
            });
    }

    Y_UNIT_TEST(ShouldHandleFillTabletRecords)
    {
        TTestExecutor executor;

        auto payload = MakePartitionsDDisks();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.FillTabletRecords(0, payload[0]);
                partitionDb.FillTabletRecords(1, payload[1]);
            });

        size_t recordsCount = 0;
        THashSet<ui64> uniqueTabletIds;
        THashSet<ui32> uniqueNodes;
        THashSet<std::tuple<ui32, ui32, ui32>> uniqueDDisks;
        THashSet<std::tuple<ui32, ui32, ui32>> uniquePBuffers;

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                using TTable = TDbsControllerSchema::DDiskMap;

                TDbsControllerDatabase partitionDb(db);
                auto it = partitionDb.Table<TTable>().All().Select();

                UNIT_ASSERT(it.IsReady());
                while (it.IsValid()) {
                    ++recordsCount;
                    uniqueTabletIds.insert(it.GetValue<TTable::TabletId>());
                    uniqueNodes.insert(it.GetValue<TTable::NodeId>());
                    if (it.GetValue<TTable::IsPBuffer>()) {
                        uniquePBuffers.insert(std::make_tuple<ui32, ui32, ui32>(
                            it.GetValue<TTable::NodeId>(),
                            it.GetValue<TTable::PDiskId>(),
                            it.GetValue<TTable::DDiskSlotId>()));
                    } else {
                        uniqueDDisks.insert(std::make_tuple<ui32, ui32, ui32>(
                            it.GetValue<TTable::NodeId>(),
                            it.GetValue<TTable::PDiskId>(),
                            it.GetValue<TTable::DDiskSlotId>()));
                    }
                    it.Next();
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(16, recordsCount);
        UNIT_ASSERT_VALUES_EQUAL(2, uniqueTabletIds.size());
        UNIT_ASSERT_VALUES_EQUAL(9, uniqueNodes.size());
        UNIT_ASSERT_VALUES_EQUAL(8, uniqueDDisks.size());
        UNIT_ASSERT_VALUES_EQUAL(8, uniquePBuffers.size());

        size_t inverseRecordsCount = 0;
        THashSet<ui64> inverseUniqueTabletIds;
        THashSet<ui32> inverseUniqueNodes;
        THashSet<std::tuple<ui32, ui32, ui32>> inverseUniqueDDiskIds;

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                using TTable = TDbsControllerSchema::InverseDDiskMap;
                TDbsControllerDatabase partitionDb(db);
                auto it = partitionDb.Table<TTable>().All().Select();

                UNIT_ASSERT(it.IsReady());
                while (it.IsValid()) {
                    ++inverseRecordsCount;
                    inverseUniqueTabletIds.insert(
                        it.GetValue<TTable::TabletId>());
                    inverseUniqueNodes.insert(it.GetValue<TTable::NodeId>());
                    inverseUniqueDDiskIds.insert(
                        std::make_tuple<ui32, ui32, ui32>(
                            it.GetValue<TTable::NodeId>(),
                            it.GetValue<TTable::PDiskId>(),
                            it.GetValue<TTable::DDiskSlotId>()));

                    it.Next();
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(16, inverseRecordsCount);
        UNIT_ASSERT_VALUES_EQUAL(2, inverseUniqueTabletIds.size());
        UNIT_ASSERT_VALUES_EQUAL(9, inverseUniqueNodes.size());
        UNIT_ASSERT_VALUES_EQUAL(16, inverseUniqueDDiskIds.size());
    }

    Y_UNIT_TEST(ShouldStoreAndLoadTabletRecords)
    {
        TTestExecutor executor;

        auto payload = MakePartitionsDDisks();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.FillTabletRecords(0, payload[0]);
                partitionDb.FillTabletRecords(1, payload[1]);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);

                TVector<ui32> tabletZeroNodes;
                partitionDb.GetNodesForTablet(0, tabletZeroNodes);
                std::ranges::sort(tabletZeroNodes);
                UNIT_ASSERT_VALUES_EQUAL(5, tabletZeroNodes.size());
                UNIT_ASSERT_VALUES_EQUAL(1, tabletZeroNodes[0]);
                UNIT_ASSERT_VALUES_EQUAL(2, tabletZeroNodes[1]);
                UNIT_ASSERT_VALUES_EQUAL(3, tabletZeroNodes[2]);
                UNIT_ASSERT_VALUES_EQUAL(4, tabletZeroNodes[3]);
                UNIT_ASSERT_VALUES_EQUAL(5, tabletZeroNodes[4]);

                TVector<ui32> tabletOneNodes;
                partitionDb.GetNodesForTablet(1, tabletOneNodes);
                std::ranges::sort(tabletOneNodes);
                UNIT_ASSERT_VALUES_EQUAL(5, tabletOneNodes.size());
                UNIT_ASSERT_VALUES_EQUAL(5, tabletOneNodes[0]);
                UNIT_ASSERT_VALUES_EQUAL(6, tabletOneNodes[1]);
                UNIT_ASSERT_VALUES_EQUAL(7, tabletOneNodes[2]);
                UNIT_ASSERT_VALUES_EQUAL(8, tabletOneNodes[3]);
                UNIT_ASSERT_VALUES_EQUAL(9, tabletOneNodes[4]);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                const TVector<TVector<ui64>> expectedValues = {
                    {0},
                    {0},
                    {0},
                    {0},
                    {0, 1},
                    {1},
                    {1},
                    {1},
                    {1},
                };

                TDbsControllerDatabase partitionDb(db);

                for (ui32 i = 0; i < expectedValues.size(); ++i) {
                    const ui32 nodeId = i + 1;
                    TVector<ui64> tabletIds;
                    partitionDb.GetTabletsForNode(nodeId, tabletIds);
                    std::ranges::sort(tabletIds);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedValues[i].size(),
                        tabletIds.size(),
                        "nodeId = " << nodeId);
                    for (ui32 j = 0; j < expectedValues[i].size(); ++j) {
                        UNIT_ASSERT_VALUES_EQUAL_C(
                            expectedValues[i][j],
                            tabletIds[j],
                            "nodeId = " << nodeId);
                    }
                }
            });
    }

    Y_UNIT_TEST(ShouldClearTabletRecords)
    {
        TTestExecutor executor;

        auto payload = MakePartitionsDDisks();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.FillTabletRecords(0, payload[0]);
                partitionDb.FillTabletRecords(1, payload[1]);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);

                TVector<ui32> tabletZeroNodes;
                partitionDb.GetNodesForTablet(0, tabletZeroNodes);
                std::ranges::sort(tabletZeroNodes);
                UNIT_ASSERT_VALUES_EQUAL(5, tabletZeroNodes.size());

                TVector<ui32> tabletOneNodes;
                partitionDb.GetNodesForTablet(1, tabletOneNodes);
                std::ranges::sort(tabletOneNodes);
                UNIT_ASSERT_VALUES_EQUAL(5, tabletOneNodes.size());

                TVector<ui64> tabletIds;
                partitionDb.GetTabletsForNode(5, tabletIds);
                UNIT_ASSERT_VALUES_EQUAL(2, tabletIds.size());
            });

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);
                TVector<TDbsControllerDatabase::TRecordKey> recordKeys;
                partitionDb.GetRecordKeysForTablet(1, recordKeys);
                partitionDb.ClearRecords(recordKeys);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase partitionDb(db);

                TVector<ui32> tabletZeroNodes;
                partitionDb.GetNodesForTablet(0, tabletZeroNodes);
                UNIT_ASSERT_VALUES_EQUAL(5, tabletZeroNodes.size());

                TVector<ui32> tabletOneNodes;
                partitionDb.GetNodesForTablet(1, tabletOneNodes);
                UNIT_ASSERT_VALUES_EQUAL(0, tabletOneNodes.size());

                TVector<ui64> tabletIds;
                partitionDb.GetTabletsForNode(5, tabletIds);
                UNIT_ASSERT_VALUES_EQUAL(1, tabletIds.size());
            });
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
