#include "part_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

using TDirectBlockGroupsConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;

using NYdb::NBS::NBlockStore::NStorage::TTestExecutor;

bool LoadState(
    NKikimr::NTable::TDatabase& db,
    TMaybe<NKikimrBlockStore::TVolumeConfig>& volumeConfig,
    TMaybe<TDirectBlockGroupsConnections>& directBlockGroupsConnections)
{
    TPartitionDatabase partitionDb(db);
    return partitionDb.ReadVolumeConfig(volumeConfig) &&
           partitionDb.ReadDirectBlockGroupsConnections(
               directBlockGroupsConnections);
}

NKikimrBlockStore::TVolumeConfig MakeSampleVolumeConfig()
{
    NKikimrBlockStore::TVolumeConfig cfg;
    cfg.SetDiskId("disk-1");
    cfg.SetBlockSize(4096);
    cfg.SetVersion(7);
    return cfg;
}

TDirectBlockGroupsConnections MakeSampleDirectBlockGroupsConnections()
{
    TDirectBlockGroupsConnections msg;
    auto* group = msg.AddDirectBlockGroupConnections();
    auto* conn = group->AddConnections();
    conn->MutableDDiskId()->SetNodeId(11);
    conn->MutableDDiskId()->SetPDiskId(22);
    conn->MutableDDiskId()->SetDDiskSlotId(33);
    conn->MutablePersistentBufferDDiskId()->SetNodeId(99);
    conn->MutablePersistentBufferDDiskId()->SetPDiskId(88);
    conn->MutablePersistentBufferDDiskId()->SetDDiskSlotId(77);
    return msg;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionDatabaseTest)
{
    Y_UNIT_TEST(ShouldInitSchema)
    {
        TTestExecutor executor;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TMaybe<NKikimrBlockStore::TVolumeConfig> volumeConfig;
                TMaybe<TDirectBlockGroupsConnections> connections;
                UNIT_ASSERT(LoadState(db, volumeConfig, connections));
                UNIT_ASSERT(!volumeConfig.Defined());
                UNIT_ASSERT(!connections.Defined());
            });
    }

    Y_UNIT_TEST(ShouldStoreAndReadVolumeConfig)
    {
        TTestExecutor executor;
        const auto written = MakeSampleVolumeConfig();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreVolumeConfig(written);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TMaybe<NKikimrBlockStore::TVolumeConfig> volumeConfig;
                TMaybe<TDirectBlockGroupsConnections> connections;
                UNIT_ASSERT(LoadState(db, volumeConfig, connections));
                UNIT_ASSERT(volumeConfig.Defined());
                UNIT_ASSERT(!connections.Defined());
                UNIT_ASSERT_VALUES_EQUAL(
                    written.GetDiskId(),
                    volumeConfig->GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(
                    written.GetBlockSize(),
                    volumeConfig->GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    written.GetVersion(),
                    volumeConfig->GetVersion());
            });
    }

    Y_UNIT_TEST(ShouldStoreAndReadPartitionIdsAsDirectBlockGroupsConnections)
    {
        TTestExecutor executor;
        const auto written = MakeSampleDirectBlockGroupsConnections();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreDirectBlockGroupsConnections(written);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TMaybe<NKikimrBlockStore::TVolumeConfig> volumeConfig;
                TMaybe<TDirectBlockGroupsConnections> connections;
                UNIT_ASSERT(LoadState(db, volumeConfig, connections));
                UNIT_ASSERT(!volumeConfig.Defined());
                UNIT_ASSERT(connections.Defined());
                UNIT_ASSERT_VALUES_EQUAL(
                    written.SerializeAsString(),
                    connections->SerializeAsString());
            });
    }

    Y_UNIT_TEST(ShouldLoadStateAfterStoreVolumeConfigAndStorePartitionIds)
    {
        TTestExecutor executor;
        const auto volumeWritten = MakeSampleVolumeConfig();
        const auto connectionsWritten =
            MakeSampleDirectBlockGroupsConnections();

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreVolumeConfig(volumeWritten);
                partitionDb.StoreDirectBlockGroupsConnections(
                    connectionsWritten);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TMaybe<NKikimrBlockStore::TVolumeConfig> volumeConfig;
                TMaybe<TDirectBlockGroupsConnections> connections;
                UNIT_ASSERT(LoadState(db, volumeConfig, connections));
                UNIT_ASSERT(volumeConfig.Defined());
                UNIT_ASSERT(connections.Defined());
                UNIT_ASSERT_VALUES_EQUAL(
                    volumeWritten.GetDiskId(),
                    volumeConfig->GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(
                    connectionsWritten.SerializeAsString(),
                    connections->SerializeAsString());
            });
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
