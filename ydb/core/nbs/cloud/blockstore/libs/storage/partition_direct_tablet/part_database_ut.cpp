#include "part_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

using TDirectBlockGroupsConnections =
    ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupsConnections;
using TAddHostInProgress =
    ::NYdb::NBS::PartitionDirect::NProto::TAddHostInProgress;

using NYdb::NBS::NBlockStore::NStorage::TTestExecutor;

bool LoadState(
    NKikimr::NTable::TDatabase& db,
    TMaybe<NKikimrBlockStore::TVolumeConfig>& volumeConfig,
    TMaybe<TDirectBlockGroupsConnections>& directBlockGroupsConnections,
    TVChunkConfigs& vChunkConfigs)
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

TDirtyMapStateProto MakeSampleDirtyMapState(ui32 stateGeneration)
{
    TDirtyMapStateProto state;
    state.SetStateGeneration(stateGeneration);

    auto* ddiskState = state.AddDDiskStates();
    auto* ahead = ddiskState->MutableAhead();
    ahead->SetRunLengthEncoding("ahead-rle");
    auto* behind = ddiskState->MutableBehind();
    behind->SetBitMask("behind-bit-mask");

    auto* secondDDiskState = state.AddDDiskStates();
    auto* secondAhead = secondDDiskState->MutableAhead();
    secondAhead->SetRunLengthEncoding("second-ahead-rle");

    return state;
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
                TVChunkConfigs vChunkConfigs;
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
                TVChunkConfigs vChunkConfigs;
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
                TVChunkConfigs vChunkConfigs;
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

    Y_UNIT_TEST(ShouldStoreReadAndClearAddHostInProgress)
    {
        TTestExecutor executor;

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
            });

        // Absent right after init.
        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TMaybe<TAddHostInProgress> loaded;
                UNIT_ASSERT(partitionDb.ReadAddHostInProgress(loaded));
                UNIT_ASSERT(!loaded.Defined());
            });

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TAddHostInProgress intent;
                intent.SetDirectBlockGroupId(3);
                intent.SetNewHostIndex(5);
                intent.SetDBGConnectionsConfigGeneration(7);
                partitionDb.StoreAddHostInProgress(intent);
            });

        // Read back what was stored.
        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TMaybe<TAddHostInProgress> loaded;
                UNIT_ASSERT(partitionDb.ReadAddHostInProgress(loaded));
                UNIT_ASSERT(loaded.Defined());
                UNIT_ASSERT_VALUES_EQUAL(3u, loaded->GetDirectBlockGroupId());
                UNIT_ASSERT_VALUES_EQUAL(5u, loaded->GetNewHostIndex());
                UNIT_ASSERT_VALUES_EQUAL(
                    7u,
                    loaded->GetDBGConnectionsConfigGeneration());
            });

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.ClearAddHostInProgress();
            });

        // Absent again after clear.
        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TMaybe<TAddHostInProgress> loaded;
                UNIT_ASSERT(partitionDb.ReadAddHostInProgress(loaded));
                UNIT_ASSERT(!loaded.Defined());
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
                    partitionDb.StoreVChunkConfig(TVChunkConfig::MakeDefault(
                        i,
                        DirectBlockGroupHostCount,
                        DefaultPrimaryCount));
                }
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TMaybe<NKikimrBlockStore::TVolumeConfig> volumeConfig;
                TMaybe<TDirectBlockGroupsConnections> connections;
                TVChunkConfigs vChunkConfigs;
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
                        cfg.GetVChunkIndex());
                    const auto expected = TVChunkConfig::MakeDefault(
                        i,
                        DirectBlockGroupHostCount,
                        DefaultPrimaryCount);
                    UNIT_ASSERT(
                        expected.GetDesiredPBuffers() ==
                        cfg.GetDesiredPBuffers());
                    UNIT_ASSERT(
                        expected.GetSecondaryPBuffers() ==
                        cfg.GetSecondaryPBuffers());
                    UNIT_ASSERT(
                        expected.GetTemporaryOfflinePBuffers() ==
                        cfg.GetTemporaryOfflinePBuffers());
                    UNIT_ASSERT(expected.GetDDisks() == cfg.GetDDisks());
                    UNIT_ASSERT(
                        expected.GetHealthyDDisks() == cfg.GetHealthyDDisks());
                    UNIT_ASSERT(
                        expected.GetDisabledHosts() == cfg.GetDisabledHosts());
                    UNIT_ASSERT_VALUES_EQUAL(
                        expected.DebugPrint(),
                        cfg.DebugPrint());
                }
            });
    }

    Y_UNIT_TEST(ShouldOverwriteVChunkConfigOnRepeatedStore)
    {
        const ui32 vChunkIndex = 11;
        TTestExecutor executor;

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreVChunkConfig(TVChunkConfig::MakeDefault(
                    vChunkIndex,
                    DirectBlockGroupHostCount,
                    DefaultPrimaryCount));
            });

        auto updated = TVChunkConfig::MakeDefault(
            vChunkIndex,
            DirectBlockGroupHostCount,
            DefaultPrimaryCount);
        updated.EvacuateHost(0);

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
                TVChunkConfigs vChunkConfigs;
                UNIT_ASSERT(partitionDb.ReadAllVChunkConfigs(vChunkConfigs));
                UNIT_ASSERT_VALUES_EQUAL(1u, vChunkConfigs.size());

                const auto& stored = vChunkConfigs[vChunkIndex];
                UNIT_ASSERT(
                    updated.GetDesiredPBuffers() ==
                    stored.GetDesiredPBuffers());
                UNIT_ASSERT(
                    updated.GetSecondaryPBuffers() ==
                    stored.GetSecondaryPBuffers());
                UNIT_ASSERT(
                    updated.GetTemporaryOfflinePBuffers() ==
                    stored.GetTemporaryOfflinePBuffers());
                UNIT_ASSERT(updated.GetDDisks() == stored.GetDDisks());
                UNIT_ASSERT(
                    updated.GetHealthyDDisks() == stored.GetHealthyDDisks());
                UNIT_ASSERT(
                    updated.GetDisabledHosts() == stored.GetDisabledHosts());
                UNIT_ASSERT_VALUES_EQUAL(
                    updated.DebugPrint(),
                    stored.DebugPrint());
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
                TVChunkConfigs vChunkConfigs;
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

    Y_UNIT_TEST(ShouldReturnEmptyDirtyMapStatesWhenAbsent)
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
                TPartitionDatabase partitionDb(db);
                TMap<ui32, TDirtyMapStateProto> loaded;
                UNIT_ASSERT(partitionDb.ReadAllDirtyMapStates(loaded));
                UNIT_ASSERT(loaded.empty());
            });
    }

    Y_UNIT_TEST(ShouldStoreAndReadDirtyMapState)
    {
        TTestExecutor executor;
        const auto written = MakeSampleDirtyMapState(7);

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreDirtyMapState(42, written);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TMap<ui32, TDirtyMapStateProto> loaded;
                UNIT_ASSERT(partitionDb.ReadAllDirtyMapStates(loaded));
                UNIT_ASSERT_VALUES_EQUAL(1u, loaded.size());
                UNIT_ASSERT(loaded.contains(42));

                const auto& state = loaded.at(42);
                UNIT_ASSERT_VALUES_EQUAL(
                    written.SerializeAsString(),
                    state.SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(7u, state.GetStateGeneration());
                UNIT_ASSERT_VALUES_EQUAL(2, state.DDiskStatesSize());
            });
    }

    Y_UNIT_TEST(ShouldStoreDirtyMapStatePerVChunkIndependently)
    {
        TTestExecutor executor;
        const auto first = MakeSampleDirtyMapState(1);
        const auto second = MakeSampleDirtyMapState(2);

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreDirtyMapState(0, first);
                partitionDb.StoreDirtyMapState(1, second);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TMap<ui32, TDirtyMapStateProto> loaded;
                UNIT_ASSERT(partitionDb.ReadAllDirtyMapStates(loaded));
                UNIT_ASSERT_VALUES_EQUAL(2u, loaded.size());

                UNIT_ASSERT(loaded.contains(0));
                UNIT_ASSERT_VALUES_EQUAL(1u, loaded.at(0).GetStateGeneration());

                UNIT_ASSERT(loaded.contains(1));
                UNIT_ASSERT_VALUES_EQUAL(2u, loaded.at(1).GetStateGeneration());

                // A vchunk that was never written must be absent from the map.
                UNIT_ASSERT(!loaded.contains(2));
            });
    }

    Y_UNIT_TEST(ShouldOverwriteDirtyMapStateOnRepeatedStore)
    {
        TTestExecutor executor;

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.InitSchema();
                partitionDb.StoreDirtyMapState(5, MakeSampleDirtyMapState(1));
            });

        const auto updated = MakeSampleDirtyMapState(99);

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                partitionDb.StoreDirtyMapState(5, updated);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TPartitionDatabase partitionDb(db);
                TMap<ui32, TDirtyMapStateProto> loaded;
                UNIT_ASSERT(partitionDb.ReadAllDirtyMapStates(loaded));
                UNIT_ASSERT_VALUES_EQUAL(1u, loaded.size());
                UNIT_ASSERT(loaded.contains(5));

                const auto& state = loaded.at(5);
                UNIT_ASSERT_VALUES_EQUAL(99u, state.GetStateGeneration());
                UNIT_ASSERT_VALUES_EQUAL(
                    updated.SerializeAsString(),
                    state.SerializeAsString());
            });
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
