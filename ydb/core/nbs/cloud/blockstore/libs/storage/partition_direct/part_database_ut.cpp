#include "part_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
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
    TMaybe<TDirectBlockGroupsConnections>& directBlockGroupsConnections,
    TVector<TVChunkConfig>& vChunkConfigs)
{
    TPartitionDatabase partitionDb(db);
    return partitionDb.ReadVolumeConfig(volumeConfig) &&
           partitionDb.ReadDirectBlockGroupsConnections(
               directBlockGroupsConnections) &&
           partitionDb.ReadAllVChunkConfigs(vChunkConfigs);
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
                TVector<TVChunkConfig> vChunkConfigs;
                UNIT_ASSERT(
                    LoadState(db, volumeConfig, connections, vChunkConfigs));
                UNIT_ASSERT(!volumeConfig.Defined());
                UNIT_ASSERT(!connections.Defined());
                UNIT_ASSERT(vChunkConfigs.empty());
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
                TVector<TVChunkConfig> vChunkConfigs;
                UNIT_ASSERT(
                    LoadState(db, volumeConfig, connections, vChunkConfigs));
                UNIT_ASSERT(volumeConfig.Defined());
                UNIT_ASSERT(!connections.Defined());
                UNIT_ASSERT(vChunkConfigs.empty());
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
                TVector<TVChunkConfig> vChunkConfigs;
                UNIT_ASSERT(
                    LoadState(db, volumeConfig, connections, vChunkConfigs));
                UNIT_ASSERT(!volumeConfig.Defined());
                UNIT_ASSERT(connections.Defined());
                UNIT_ASSERT(vChunkConfigs.empty());
                UNIT_ASSERT_VALUES_EQUAL(
                    written.SerializeAsString(),
                    connections->SerializeAsString());
            });
    }

    Y_UNIT_TEST(ShouldStoreAndReadVChunkConfigsPerRow)
    {
        TTestExecutor executor;

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                for (ui32 i = 0; i < 3; ++i) {
                    partitionDb.StoreVChunkConfig(TVChunkConfig::Make(i));
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TMaybe<NKikimrBlockStore::TVolumeConfig> volumeConfig;
                TMaybe<TDirectBlockGroupsConnections> connections;
                TVector<TVChunkConfig> vChunkConfigs;
                UNIT_ASSERT(
                    LoadState(db, volumeConfig, connections, vChunkConfigs));
                UNIT_ASSERT(!volumeConfig.Defined());
                UNIT_ASSERT(!connections.Defined());
                UNIT_ASSERT_VALUES_EQUAL(3u, vChunkConfigs.size());

                for (size_t i = 0; i < vChunkConfigs.size(); ++i) {
                    const auto& cfg = vChunkConfigs[i];
                    UNIT_ASSERT(cfg.IsValid());
                    UNIT_ASSERT_VALUES_EQUAL(
                        static_cast<ui32>(i),
                        cfg.VChunkIndex);
                    const auto expected = TVChunkConfig::Make(i);
                    UNIT_ASSERT(expected.PBufferHosts == cfg.PBufferHosts);
                    UNIT_ASSERT(expected.DDiskHosts == cfg.DDiskHosts);
                    UNIT_ASSERT(expected.EnabledHosts == cfg.EnabledHosts);
                }
            });
    }

    Y_UNIT_TEST(ShouldOverwriteVChunkConfigOnRepeatedStore)
    {
        TTestExecutor executor;

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreVChunkConfig(TVChunkConfig::Make(5));
            });

        auto updated = TVChunkConfig::Make(5);
        updated.PBufferHosts.SetRole(0, EHostRole::None);

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.StoreVChunkConfig(updated);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TVector<TVChunkConfig> vChunkConfigs;
                UNIT_ASSERT(partitionDb.ReadAllVChunkConfigs(vChunkConfigs));
                UNIT_ASSERT_VALUES_EQUAL(1u, vChunkConfigs.size());

                const auto& stored = vChunkConfigs[0];
                UNIT_ASSERT_VALUES_EQUAL(
                    updated.VChunkIndex,
                    stored.VChunkIndex);
                UNIT_ASSERT(updated.PBufferHosts == stored.PBufferHosts);
                UNIT_ASSERT(updated.DDiskHosts == stored.DDiskHosts);
                UNIT_ASSERT(updated.EnabledHosts == stored.EnabledHosts);
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
                TVector<TVChunkConfig> vChunkConfigs;
                UNIT_ASSERT(
                    LoadState(db, volumeConfig, connections, vChunkConfigs));
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
