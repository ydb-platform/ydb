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
 * |    |      |    | PB | --------> |     |     | Node 5 |
 * +----+------+----+----+           | SL1 | PD2 |        |
 * |    |      |    | DD | --------> |     |     |        |
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

namespace {

auto MakeDirectPayload()
{
    const auto record = [](const std::initializer_list<std::pair<
                               std::tuple<ui32, ui32, ui32>,
                               std::tuple<ui32, ui32, ui32>>>& ddisks)
    {
        NProto::TDirectBlockGroupDDisks protoRecord;
        for (const auto& [ddId, pbId]: ddisks) {
            auto* ddiskIds = protoRecord.AddDDiskIds();
            {
                auto* id = ddiskIds->MutableDDisk();
                id->SetNodeId(std::get<0>(ddId));
                id->SetPDiskId(std::get<1>(ddId));
                id->SetDDiskSlotId(std::get<2>(ddId));
            }
            {
                auto* id = ddiskIds->MutablePersistentBuffer();
                id->SetNodeId(std::get<0>(pbId));
                id->SetPDiskId(std::get<1>(pbId));
                id->SetDDiskSlotId(std::get<2>(pbId));
            }
        }
        return protoRecord;
    };
    return THashMap<std::tuple<ui64, ui64>, NProto::TDirectBlockGroupDDisks>{
        {{0, 0}, record({{{1, 1, 1}, {2, 1, 1}}, {{3, 1, 1}, {3, 2, 1}}})},
        {{0, 1}, record({{{4, 1, 1}, {4, 2, 1}}, {{5, 1, 1}, {5, 2, 1}}})},
        {{1, 0}, record({{{5, 2, 1}, {6, 1, 1}}, {{7, 1, 1}, {7, 2, 1}}})},
        {{1, 1}, record({{{8, 1, 1}, {8, 2, 1}}, {{9, 1, 1}, {9, 2, 1}}})},
    };
}

auto MakeInversePayload()
{
    const auto record = [](const std::initializer_list<
                            std::pair<ui64, std::initializer_list<ui64>>>& dbgs)
    {
        NProto::TDDiskDirectBlockGroups protoRecord;
        for (const auto [tabletId, dbgIds]: dbgs) {
            auto* perTablet = protoRecord.AddPartitionDirectBlockGroups();
            perTablet->SetPartitionTabletId(tabletId);
            for (const auto dbgIndex: dbgIds) {
                perTablet->AddDirectBlockGroupIndex(dbgIndex);
            }
        }
        return protoRecord;
    };
    return THashMap<
        std::tuple<ui32, ui32, ui32>,
        NProto::TDDiskDirectBlockGroups>{
        {{1, 1, 1}, record({{0, {0}}})},
        {{2, 1, 1}, record({{0, {0}}})},
        {{3, 1, 1}, record({{0, {0}}})},
        {{3, 2, 1}, record({{0, {0}}})},
        {{4, 1, 1}, record({{0, {1}}})},
        {{4, 2, 1}, record({{0, {1}}})},
        {{5, 1, 1}, record({{0, {1}}})},
        {{5, 2, 1}, record({{0, {1}}, {1, {0}}})},
        {{6, 1, 1}, record({{1, {0}}})},
        {{7, 1, 1}, record({{1, {0}}})},
        {{7, 2, 1}, record({{1, {0}}})},
        {{8, 1, 1}, record({{1, {1}}})},
        {{8, 2, 1}, record({{1, {1}}})},
        {{9, 1, 1}, record({{1, {1}}})},
        {{9, 2, 1}, record({{1, {1}}})},
    };
}

}   // namespace

Y_UNIT_TEST_SUITE(TDbsControllerDatabaseTest)
{
    Y_UNIT_TEST(ShouldInitSchema)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                const auto it =
                    dbsControllerDb.Table<TDbsControllerSchema::DirectMap>()
                        .All()
                        .Select();
                UNIT_ASSERT(it.IsReady());
                UNIT_ASSERT(!it.IsValid());   // must be 0 rows
            });
    }

    Y_UNIT_TEST(ShouldStoreAndLoadDirectRecords)
    {
        TTestExecutor executor;

        auto payload = MakeDirectPayload();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: payload) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                for (const auto& [id, expectedRecord]: payload) {
                    NProto::TDirectBlockGroupDDisks record;
                    dbsControllerDb.LoadDirectRecord(id, record);
                    TString recordStr;
                    {
                        const bool success =
                            record.SerializeToString(&recordStr);
                        UNIT_ASSERT_C(success, "id = " << id);
                    }

                    TString expectedRecordStr;
                    {
                        const bool success = expectedRecord.SerializeToString(
                            &expectedRecordStr);
                        Y_ABORT_UNLESS(success);
                    }

                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedRecordStr,
                        recordStr,
                        "id = " << id);
                }
            });
    }

    Y_UNIT_TEST(ShouldStoreAndLoadInverseRecords)
    {
        TTestExecutor executor;

        auto payload = MakeInversePayload();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: payload) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                for (const auto& [id, expectedRecord]: payload) {
                    NProto::TDDiskDirectBlockGroups record;
                    dbsControllerDb.LoadInverseRecord(id, record);
                    TString recordStr;
                    {
                        const bool success =
                            record.SerializeToString(&recordStr);
                        UNIT_ASSERT_C(success, "id = " << id);
                    }

                    TString expectedRecordStr;
                    {
                        const bool success = expectedRecord.SerializeToString(
                            &expectedRecordStr);
                        Y_ABORT_UNLESS(success);
                    }

                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedRecordStr,
                        recordStr,
                        "id = " << id);
                }
            });
    }

    Y_UNIT_TEST(ShouldListTabletRecords)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
                for (const auto& [id, record]: MakeInversePayload()) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        TVector<TDbsControllerDatabase::TDirectKey> directKeys;
        TVector<TDbsControllerDatabase::TInverseKey> inverseKeys;

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                dbsControllerDb.GetRecordKeysForTablet(
                    0,
                    directKeys,
                    inverseKeys);

                UNIT_ASSERT_VALUES_EQUAL(2, directKeys.size());
                UNIT_ASSERT_VALUES_EQUAL(8, inverseKeys.size());

                for (const auto& [tabletId, directBlockGroupIndex]: directKeys)
                {
                    auto it =
                        dbsControllerDb.Table<TDbsControllerSchema::DirectMap>()
                            .Key(tabletId, directBlockGroupIndex)
                            .Select();
                    UNIT_ASSERT_C(
                        it.IsOk(),
                        "id = <" << tabletId << ", " << directBlockGroupIndex
                                 << ">");
                }

                for (const auto& [nodeId, pdiskId, slotId]: inverseKeys) {
                    auto it = dbsControllerDb
                                  .Table<TDbsControllerSchema::InverseMap>()
                                  .Key(nodeId, pdiskId, slotId)
                                  .Select();
                    UNIT_ASSERT_C(
                        it.IsOk(),
                        "id = <" << nodeId << ", " << pdiskId << ", " << slotId
                                 << ">");
                }
            });
    }

    Y_UNIT_TEST(ShouldRemoveRecords)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
                for (const auto& [id, record]: MakeInversePayload()) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                UNIT_ASSERT(
                    dbsControllerDb.Table<TDbsControllerSchema::DirectMap>()
                        .Key(0, 0)
                        .Select()
                        .IsOk());

                UNIT_ASSERT(
                    dbsControllerDb.Table<TDbsControllerSchema::InverseMap>()
                        .Key(1, 1, 1)
                        .Select()
                        .IsOk());
            });

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                dbsControllerDb.Table<TDbsControllerSchema::DirectMap>()
                    .Key(0, 0)
                    .Delete();
                dbsControllerDb.Table<TDbsControllerSchema::InverseMap>()
                    .Key(1, 1, 1)
                    .Delete();
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                UNIT_ASSERT(
                    !dbsControllerDb.Table<TDbsControllerSchema::DirectMap>()
                         .Key(0, 0)
                         .Select()
                         .IsOk());

                UNIT_ASSERT(
                    !dbsControllerDb.Table<TDbsControllerSchema::InverseMap>()
                         .Key(1, 1, 1)
                         .Select()
                         .IsOk());
            });
    }

    Y_UNIT_TEST(ShouldCalculateLogicalNodesCount)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                TVector<TDbsControllerDatabase::TDirectKey> directKeys;
                TVector<TDbsControllerDatabase::TInverseKey> inverseKeys;

                dbsControllerDb.GetRecordKeysForTablet(
                    0,
                    directKeys,
                    inverseKeys);

                UNIT_ASSERT_VALUES_EQUAL(2, directKeys.size());

                for (const auto& id: directKeys) {
                    std::optional<ui64> count;
                    const bool ok =
                        dbsControllerDb.GetLogicalNodesCount(id, count);
                    UNIT_ASSERT_C(ok, "id = " << id);
                    UNIT_ASSERT_C(count.has_value(), "id = " << id);
                    UNIT_ASSERT_VALUES_EQUAL_C(2, count.value(), "id = " << id);
                }
            });
    }

    Y_UNIT_TEST(ShouldReturnPartitionsForGivenNode)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
                for (const auto& [id, record]: MakeInversePayload()) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                const auto expected =
                    THashMap<std::tuple<ui32>, std::vector<ui64>>{
                        {{1}, {0}},
                        {{2}, {0}},
                        {{3}, {0}},
                        {{4}, {0}},
                        {{5}, {0, 1}},
                        {{6}, {1}},
                        {{7}, {1}},
                        {{8}, {1}},
                        {{9}, {1}},
                    };

                for (const auto& [id, expectedTablets]: expected) {
                    TVector<ui64> actualTablets;
                    const bool ok = dbsControllerDb.GetPartitionsForNode(
                        std::get<0>(id),
                        actualTablets);
                    UNIT_ASSERT_C(ok, "id = " << id);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedTablets.size(),
                        actualTablets.size(),
                        "id = " << id);
                    for (size_t i = 0; i < expectedTablets.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL_C(
                            expectedTablets[i],
                            actualTablets[i],
                            "id = " << id << ", i = " << i);
                    }
                }
            });
    }

    Y_UNIT_TEST(ShouldReturnPartitionsForGivenPDisk)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
                for (const auto& [id, record]: MakeInversePayload()) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                const auto expected =
                    THashMap<std::tuple<ui32, ui32>, std::vector<ui64>>{
                        {{1, 1}, {0}},
                        {{2, 1}, {0}},
                        {{3, 1}, {0}},
                        {{3, 2}, {0}},
                        {{4, 1}, {0}},
                        {{4, 2}, {0}},
                        {{5, 1}, {0}},
                        {{5, 2}, {0, 1}},
                        {{6, 1}, {1}},
                        {{7, 1}, {1}},
                        {{7, 2}, {1}},
                        {{8, 1}, {1}},
                        {{8, 2}, {1}},
                        {{9, 1}, {1}},
                        {{9, 2}, {1}},
                    };

                for (const auto& [id, expectedTablets]: expected) {
                    TVector<ui64> actualTablets;
                    const bool ok = dbsControllerDb.GetPartitionsForPDisk(
                        std::get<0>(id),
                        std::get<1>(id),
                        actualTablets);
                    UNIT_ASSERT_C(ok, "id = " << id);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedTablets.size(),
                        actualTablets.size(),
                        "id = " << id);
                    for (size_t i = 0; i < expectedTablets.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL_C(
                            expectedTablets[i],
                            actualTablets[i],
                            "id = " << id << ", i = " << i);
                    }
                }
            });
    }

    Y_UNIT_TEST(ShouldReturnPartitionsForGivenDDisk)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
                for (const auto& [id, record]: MakeInversePayload()) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);

                const auto expected =
                    THashMap<std::tuple<ui32, ui32, ui32>, std::vector<ui64>>{
                        {{1, 1, 1}, {0}},
                        {{2, 1, 1}, {0}},
                        {{3, 1, 1}, {0}},
                        {{3, 2, 1}, {0}},
                        {{4, 1, 1}, {0}},
                        {{4, 2, 1}, {0}},
                        {{5, 1, 1}, {0}},
                        {{5, 2, 1}, {0, 1}},
                        {{6, 1, 1}, {1}},
                        {{7, 1, 1}, {1}},
                        {{7, 2, 1}, {1}},
                        {{8, 1, 1}, {1}},
                        {{8, 2, 1}, {1}},
                        {{9, 1, 1}, {1}},
                        {{9, 2, 1}, {1}},
                    };

                for (const auto& [id, expectedTablets]: expected) {
                    TVector<ui64> actualTablets;
                    const bool ok = dbsControllerDb.GetPartitionsForDDisk(
                        std::get<0>(id),
                        std::get<1>(id),
                        std::get<2>(id),
                        actualTablets);
                    UNIT_ASSERT_C(ok, "id = " << id);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedTablets.size(),
                        actualTablets.size(),
                        "id = " << id);
                    for (size_t i = 0; i < expectedTablets.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL_C(
                            expectedTablets[i],
                            actualTablets[i],
                            "id = " << id << ", i = " << i);
                    }
                }
            });
    }

    Y_UNIT_TEST(ShouldReturnAffectedDBGsWithNodeCounts)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TDbsControllerDatabase dbsControllerDb(db);
                dbsControllerDb.InitSchema();
                for (const auto& [id, record]: MakeDirectPayload()) {
                    dbsControllerDb.StoreDirectRecord(id, record);
                }
                for (const auto& [id, record]: MakeInversePayload()) {
                    dbsControllerDb.StoreInverseRecord(id, record);
                }
            });

        auto testNodesSubset =
            [&](const TVector<ui32>& nodes,
                const THashMap<TDbsControllerDatabase::TDirectKey, ui64>&
                    expected,
                const TString& note)
        {
            const auto comm = TStringBuilder()
                              << "nodes = " << nodes << ", note = " << note;
            executor.ReadTx(
                [&](NKikimr::NTable::TDatabase& db)
                {
                    TDbsControllerDatabase dbsControllerDb(db);

                    THashMap<TDbsControllerDatabase::TDirectKey, ui64> actual;

                    const bool ok =
                        dbsControllerDb.GetAffectedDBGsWithNodeCounts(
                            nodes,
                            actual);
                    UNIT_ASSERT_C(ok, comm);

                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expected.size(),
                        actual.size(),
                        comm);
                    for (const auto& [key, nodesCount]: expected) {
                        UNIT_ASSERT_C(
                            actual.contains(key),
                            comm << ", key = " << key);
                        UNIT_ASSERT_VALUES_EQUAL_C(
                            nodesCount,
                            actual.at(key),
                            comm << ", key = " << key);
                    }
                });
        };

        testNodesSubset(
            {5},
            {
                {{0, 1}, 1},
                {{1, 0}, 1},
            },
            "Node with 2 tablets");

        testNodesSubset(
            {8, 9},
            {
                {{1, 1}, 0},
            },
            "Fully covered DBG");

        testNodesSubset(
            {3, 9},
            {
                {{0, 0}, 1},
                {{1, 1}, 1},
            },
            "Two half-covered DBG");

        testNodesSubset(
            {1, 2},
            {
                {{0, 0}, 1},
            },
            "Two different nodes in one logical node");
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController

template <>
inline void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32>& vec)
{
    o << "[ ";
    bool isFirst = true;
    for (const auto& x: vec) {
        if (!isFirst) {
            o << ", ";
        }
        isFirst = false;
        o << x;
    }
    o << "]";
}
