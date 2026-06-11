#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/region.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 BlocksPerRegion = RegionSize / DefaultBlockSize;
constexpr ui64 DefaultVChunkSize = RegionSize / DirectBlockGroupsCount;
const TString DDiskPoolName = "ddp1";
const TString PersistentBufferDDiskPoolName = "ddp1";
const ui64 PartitionTabletId = MakeTabletID(1, 0, 1);

////////////////////////////////////////////////////////////////////////////////

struct TScopedNbsService: TDisableCopyMove
{
    explicit TScopedNbsService(const NKikimrConfig::TNbsConfig& nbsConfig)
    {
        CreateNbsService(nbsConfig);
        StartNbsService();
    }

    ~TScopedNbsService()
    {
        StopNbsService();
    }
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TScopedNbsService SetupStorage(
    TEnvironmentSetup& env,
    EWriteMode writeMode,
    TDuration writeHedgingDelay = TDuration::Seconds(1),
    ui64 pbufferCleanupLsnStep = 0,
    ui32 syncRequestsBatchSize = 0)
{
    env.CreateBoxAndPool();
    env.Sim(TDuration::Seconds(30));

    {
        NKikimrBlobStorage::TConfigRequest request;
        auto* cmd = request.AddCommand()->MutableDefineDDiskPool();
        cmd->SetBoxId(1);
        cmd->SetName(DDiskPoolName);
        auto* g = cmd->MutableGeometry();
        g->SetRealmLevelBegin(10);
        g->SetRealmLevelEnd(20);
        g->SetDomainLevelBegin(10);
        g->SetDomainLevelEnd(40);
        g->SetNumFailRealms(1);
        g->SetNumFailDomainsPerFailRealm(5);
        g->SetNumVDisksPerFailDomain(1);
        cmd->AddPDiskFilter()->AddProperty()->SetType(
            NKikimrBlobStorage::EPDiskType::ROT);
        cmd->SetNumDDiskGroups(3);
        auto res = env.Invoke(request);
        UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
    }

    // Setup NBS service with storage config
    NKikimrConfig::TNbsConfig nbsConfig;
    auto* storageConfig = nbsConfig.MutableNbsStorageConfig();
    storageConfig->SetDDiskPoolName(DDiskPoolName);
    storageConfig->SetPersistentBufferDDiskPoolName(
        PersistentBufferDDiskPoolName);
    storageConfig->SetWriteMode(GetProtoWriteMode(writeMode));
    storageConfig->SetVChunkSize(DefaultVChunkSize);
    storageConfig->SetWriteHedgingDelay(writeHedgingDelay.MicroSeconds());
    storageConfig->SetPBufferCleanupLsnStep(pbufferCleanupLsnStep);
    if (syncRequestsBatchSize) {
        storageConfig->SetSyncRequestsBatchSize(syncRequestsBatchSize);
    }

    return TScopedNbsService(nbsConfig);
}

NKikimrBlockStore::TVolumeConfig CreateVolumeConfig(ui64 blockCount)
{
    NKikimrBlockStore::TVolumeConfig volumeConfig;
    volumeConfig.SetDiskId("test-volume");
    volumeConfig.SetBlockSize(4096);
    volumeConfig.SetStoragePoolName(DDiskPoolName);
    auto* partition = volumeConfig.AddPartitions();
    partition->SetBlockCount(blockCount);
    return volumeConfig;
}

void WaitForTabletBoot(TEnvironmentSetup& env)
{
    // Create tablet like in SetupTablet()
    env.Runtime->CreateTestBootstrapper(
        TTestActorSystem::CreateTestTabletInfo(
            PartitionTabletId,
            TTabletTypes::Unknown,
            env.Settings.Erasure.GetErasure(),
            env.GroupId,
            3),   // NumChannels
        [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
        { return new TPartitionActor(tablet, info); },
        env.Settings.ControllerNodeId);

    // Wait for tablet to boot
    bool working = true;
    env.Runtime->Sim(
        [&] { return working; },
        [&](IEventHandle& event)
        { working = event.GetTypeRewrite() != TEvTablet::EvBoot; });
}

ui64 CreatePartitionTablet(TEnvironmentSetup& env, ui64 blockCount = 32768)
{
    WaitForTabletBoot(env);

    // Send volume config update
    auto volumeConfig = CreateVolumeConfig(blockCount);
    auto updateEvent =
        std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
    updateEvent->Record.MutableVolumeConfig()->CopyFrom(volumeConfig);
    updateEvent->Record.SetTxId(1);

    const TActorId& edge = env.Runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);

    env.Runtime->SendToPipe(
        PartitionTabletId,
        edge,
        updateEvent.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());

    // Wait for response
    auto response = env.WaitForEdgeActorEvent<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>(edge);
    UNIT_ASSERT(response->Get()->Record.GetStatus() == NKikimrBlockStore::OK);

    // Wait for partition to allocate DDisk group
    env.Sim(TDuration::Seconds(10));

    return PartitionTabletId;
}

TActorId GetLoadActorAdapterActorId(
    TEnvironmentSetup& env,
    ui64 partitionTabletId,
    const TActorId& edge)
{
    auto request =
        std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdRequest>();
    env.Runtime->SendToPipe(
        partitionTabletId,
        edge,
        request.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());

    auto res = env.WaitForEdgeActorEvent<
        TEvService::TEvGetLoadActorAdapterActorIdResponse>(edge, false);
    UNIT_ASSERT(res);
    UNIT_ASSERT(!res->Get()->Record.GetActorId().empty());
    NActors::TActorId loadActorAdapter;
    loadActorAdapter.Parse(
        res->Get()->Record.GetActorId().data(),
        res->Get()->Record.GetActorId().size());
    return loadActorAdapter;
}

void WriteBlock(
    TEnvironmentSetup& env,
    const TActorId& loadActorAdapter,
    const TActorId& edge,
    ui64 index,
    const TString& data)
{
    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(index);
    request->Record.MutableBlocks()->AddBuffers(data);

    env.Runtime->Send(
        new IEventHandle(loadActorAdapter, edge, request.release()),
        edge.NodeId());

    auto res = env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
        edge,
        false);
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        res->Get()->Record.GetError().GetCode(),
        FormatError(res->Get()->Record.GetError()));
}

TString ReadBlock(
    TEnvironmentSetup& env,
    const TActorId& loadActorAdapter,
    const TActorId& edge,
    ui64 index)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetStartIndex(index);
    request->Record.SetBlocksCount(1);

    env.Runtime->Send(
        new IEventHandle(loadActorAdapter, edge, request.release()),
        edge.NodeId());

    auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
        edge,
        false);
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        res->Get()->Record.GetError().GetCode(),
        FormatError(res->Get()->Record.GetError()));
    UNIT_ASSERT_VALUES_EQUAL(1, res->Get()->Record.GetBlocks().BuffersSize());
    return res->Get()->Record.GetBlocks().GetBuffers(0);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

void BasicWriteRead(EWriteMode writeMode)
{
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(
        NKikimrServices::NBS_PARTITION,
        NActors::NLog::PRI_DEBUG);

    auto scopedService = SetupStorage(env, writeMode);

    auto partition = CreatePartitionTablet(env);

    const TActorId& edge = runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);

    auto loadActorAdapter = GetLoadActorAdapterActorId(env, partition, edge);

    // Read not written block
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(0);
        request->Record.SetBlocksCount(1);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            res->Get()->Record.GetError().GetCode(),
            FormatError(res->Get()->Record.GetError()));
        UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
        UNIT_ASSERT(
            res->Get()->Record.MutableBlocks()->GetBuffers(0) ==
            TString(4096, 0));
    }

    auto syncRequestsCount = 0;
    runtime->FilterFunction =
        [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
    {
        if (ev->GetTypeRewrite() ==
            NDDisk::TEvSyncWithPersistentBuffer::EventType)
        {
            if (syncRequestsCount++ < 3) {
                runtime->Schedule(
                    TDuration::Seconds(10),
                    ev.release(),
                    nullptr,
                    nodeId);

                return false;
            }
        }

        return true;
    };

    auto expectedData = TString(1024, 'A') + TString(1024, 'B') +
                        TString(1024, 'C') + TString(1024, 'D');
    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(1);
        request->Record.MutableBlocks()->AddBuffers(expectedData);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res =
            env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                edge,
                false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
    }

    // Read written block from persistent buffer
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(1);
        request->Record.SetBlocksCount(1);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            res->Get()->Record.GetError().GetCode(),
            FormatError(res->Get()->Record.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            res->Get()->Record.GetBlocks().BuffersSize());
        UNIT_ASSERT_VALUES_EQUAL(
            res->Get()->Record.GetBlocks().GetBuffers(0),
            expectedData);
    }

    // Read not written block
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(0);
        request->Record.SetBlocksCount(1);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
        UNIT_ASSERT(
            res->Get()->Record.MutableBlocks()->GetBuffers(0) ==
            TString(4096, 0));
    }

    env.Sim(TDuration::Seconds(60));

    // Read written block from ddisk
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(1);
        request->Record.SetBlocksCount(1);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
        UNIT_ASSERT_VALUES_EQUAL(
            res->Get()->Record.MutableBlocks()->GetBuffers(0),
            expectedData);
    }
}

void ShouldWriteAndReadBlocksInDifferentRegions(EWriteMode writeMode)
{
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(
        NKikimrServices::NBS_PARTITION,
        NActors::NLog::PRI_DEBUG);

    auto scopedService = SetupStorage(env, writeMode);

    const ui64 blockCount = 3 * BlocksPerRegion;
    auto partition = CreatePartitionTablet(env, blockCount);

    const TActorId& edge = runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);

    auto loadActorAdapter = GetLoadActorAdapterActorId(env, partition, edge);

    // Write one block at the start of each of 3 regions
    const ui64 regionBlockIndices[] = {
        0,
        BlocksPerRegion,
        2 * BlocksPerRegion,
    };
    TString expectedData[4] = {
        TString(1024, 'A') + TString(1024, 'B') + TString(1024, 'C') +
            TString(1024, 'D'),
        TString(1024, 'E') + TString(1024, 'F') + TString(1024, 'G') +
            TString(1024, 'H'),
        TString(1024, 'I') + TString(1024, 'J') + TString(1024, 'K') +
            TString(1024, 'L'),
    };

    for (int i = 0; i < 3; ++i) {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(regionBlockIndices[i]);
        request->Record.MutableBlocks()->AddBuffers(expectedData[i]);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res =
            env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                edge,
                false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
    }

    // Wait for sync and erase
    env.Sim(TDuration::Seconds(10));

    // Read back each block and verify
    for (int i = 0; i < 3; ++i) {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(regionBlockIndices[i]);
        request->Record.SetBlocksCount(1);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
        UNIT_ASSERT_VALUES_EQUAL(
            res->Get()->Record.MutableBlocks()->GetBuffers(0),
            expectedData[i]);
    }
}

void RandomWrites(EWriteMode writeMode)
{
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(
        NKikimrServices::NBS_PARTITION,
        NActors::NLog::PRI_DEBUG);

    auto scopedService = SetupStorage(env, writeMode);

    const ui64 blockCount = 3 * BlocksPerRegion;
    auto partition = CreatePartitionTablet(env, blockCount);

    const TActorId& edge = runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);

    auto loadActorAdapter = GetLoadActorAdapterActorId(env, partition, edge);

    const ui32 numRandomWrites = 200;
    THashMap<ui64, TString> expectedDataByBlockIndex;
    for (ui32 i = 0; i < numRandomWrites; ++i) {
        const ui64 blockIndex = RandomNumber<ui64>(blockCount);
        TString data =
            NUnitTest::RandomString(DefaultBlockSize, RandomNumber<ui32>());
        expectedDataByBlockIndex[blockIndex] = data;

        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.MutableBlocks()->AddBuffers(std::move(data));

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res =
            env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                edge,
                false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
    }

    env.Sim(TDuration::Seconds(10));

    for (const auto& [blockIndex, expectedData]: expectedDataByBlockIndex) {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
        UNIT_ASSERT_VALUES_EQUAL(
            res->Get()->Record.MutableBlocks()->GetBuffers(0),
            expectedData);
    }
}

void ShouldWriteAndReadMultipleBlocks(EWriteMode writeMode)
{
    TEnvironmentSetup env{{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    }};
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(
        NKikimrServices::NBS_PARTITION,
        NActors::NLog::PRI_DEBUG);

    auto scopedService = SetupStorage(env, writeMode);

    auto partition = CreatePartitionTablet(env);

    const TActorId& edge = runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);

    auto loadActorAdapter = GetLoadActorAdapterActorId(env, partition, edge);

    TString expectedData =
        NUnitTest::RandomString(DefaultBlockSize * 128, RandomNumber<ui32>());

    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(100);
        request->Record.MutableBlocks()->AddBuffers(expectedData);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res =
            env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                edge,
                false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
    }

    env.Sim(TDuration::Seconds(10));

    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(100);
        request->Record.SetBlocksCount(128);

        runtime->Send(
            new IEventHandle(loadActorAdapter, edge, request.release()),
            edge.NodeId());

        auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
            edge,
            false);
        UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
        UNIT_ASSERT_VALUES_EQUAL(
            res->Get()->Record.MutableBlocks()->GetBuffers(0),
            expectedData);
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TPartitionDirectTest)
{
    Y_UNIT_TEST(MultipleInit)
    {
        {
            TEnvironmentSetup env{{
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            }};
            auto& runtime = env.Runtime;
            runtime->SetLogPriority(
                NKikimrServices::NBS_PARTITION,
                NActors::NLog::PRI_DEBUG);

            auto scopedService =
                SetupStorage(env, EWriteMode::DirectPBuffersFilling);
        }
        {
            TEnvironmentSetup env{{
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            }};
            auto& runtime = env.Runtime;
            runtime->SetLogPriority(
                NKikimrServices::NBS_PARTITION,
                NActors::NLog::PRI_DEBUG);

            auto scopedService =
                SetupStorage(env, EWriteMode::DirectPBuffersFilling);
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyAllocateDirectBlockGroups)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService =
            SetupStorage(env, EWriteMode::DirectPBuffersFilling);

        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType)
            {
                auto* msg = ev->Get<
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.QueriesSize(), 32);
                for (size_t i = 0; i < 32; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        msg->Record.GetQueries(i).GetDirectBlockGroupId(),
                        i);
                    UNIT_ASSERT_VALUES_EQUAL(
                        msg->Record.GetQueries(i).GetTargetNumVChunks(),
                        5);
                }
            }

            return true;
        };

        CreatePartitionTablet(
            env,
            4 * BlocksPerRegion + 1   // blockCount
        );
    }

    Y_UNIT_TEST(BasicWriteReadPBufferReplication)
    {
        BasicWriteRead(EWriteMode::PBufferReplication);
    }

    Y_UNIT_TEST(BasicWriteReadDirectPBufferFilling)
    {
        BasicWriteRead(EWriteMode::DirectPBuffersFilling);
    }

    Y_UNIT_TEST(ShouldWriteAndReadBlocksInDifferentRegionsPBufferReplication)
    {
        ShouldWriteAndReadBlocksInDifferentRegions(
            EWriteMode::PBufferReplication);
    }

    Y_UNIT_TEST(ShouldWriteAndReadBlocksInDifferentRegionsDirectPBufferFilling)
    {
        ShouldWriteAndReadBlocksInDifferentRegions(
            EWriteMode::DirectPBuffersFilling);
    }

    Y_UNIT_TEST(RandomWritesPBufferReplication)
    {
        RandomWrites(EWriteMode::PBufferReplication);
    }

    Y_UNIT_TEST(RandomWritesDirectPBufferFilling)
    {
        RandomWrites(EWriteMode::DirectPBuffersFilling);
    }

    Y_UNIT_TEST(ShouldWriteAndReadMultipleBlocksPBufferReplication)
    {
        ShouldWriteAndReadMultipleBlocks(EWriteMode::PBufferReplication);
    }

    Y_UNIT_TEST(ShouldWriteAndReadMultipleBlocksDirectPBufferFilling)
    {
        ShouldWriteAndReadMultipleBlocks(EWriteMode::DirectPBuffersFilling);
    }

    // Test implementation for PBufferReplication write mode
    Y_UNIT_TEST(WriteToManyPBuffersFallback)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::PBufferReplication);

        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        auto loadActorAdapter =
            GetLoadActorAdapterActorId(env, partition, edge);

        bool alreadyOnce{};
        size_t singleWriteRequestsCounter{};
        runtime->FilterFunction =
            [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() ==
                NDDisk::TEvWritePersistentBuffer::EventType)
            {
                ++singleWriteRequestsCounter;
                return true;
            }

            if (ev->GetTypeRewrite() ==
                NDDisk::TEvWritePersistentBuffersResult::EventType)
            {
                if (!alreadyOnce) {
                    alreadyOnce = true;

                    auto* msg =
                        ev->Get<NDDisk::TEvWritePersistentBuffersResult>();
                    auto& pb0Result = (*msg->Record.MutableResult())[0];
                    pb0Result.MutableResult()->SetStatus(
                        NKikimrBlobStorage::NDDisk::TReplyStatus_E_ERROR);
                    auto& pb1Result = (*msg->Record.MutableResult())[1];
                    pb1Result.MutableResult()->SetStatus(
                        NKikimrBlobStorage::NDDisk::TReplyStatus_E_ERROR);

                    runtime->Schedule(
                        TDuration::Seconds(3),
                        ev.release(),
                        nullptr,
                        nodeId);

                    return false;
                }
            }

            return true;
        };

        auto expectedData = TString(1024, 'A') + TString(1024, 'B') +
                            TString(1024, 'C') + TString(1024, 'D');
        {
            auto request =
                std::make_unique<TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(expectedData);

            runtime->Send(
                new IEventHandle(loadActorAdapter, edge, request.release()),
                edge.NodeId());

            auto res =
                env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                    edge,
                    false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);

            // 2 - the number of errors which we set in this test
            // 3 - the number of TEvWritePersistentBuffer requests in the
            // blobstorage's implementation of TEvWritePersistentBuffers.
            // This test will fail in case of the implementation's changing - we
            // will have to fix it.
            UNIT_ASSERT_VALUES_EQUAL(singleWriteRequestsCounter, 2 + 3);
        }

        // Read written block from persistent buffer
        {
            auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.SetBlocksCount(1);

            runtime->Send(
                new IEventHandle(loadActorAdapter, edge, request.release()),
                edge.NodeId());

            auto res =
                env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
                    edge,
                    false);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                res->Get()->Record.GetError().GetCode(),
                FormatError(res->Get()->Record.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                res->Get()->Record.GetBlocks().BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(
                res->Get()->Record.GetBlocks().GetBuffers(0),
                expectedData);
        }
    }

    Y_UNIT_TEST(ShouldWriteAndReadFromHandoffPersistentBuffers)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService =
            SetupStorage(env, EWriteMode::DirectPBuffersFilling);

        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        auto loadActorAdapter =
            GetLoadActorAdapterActorId(env, partition, edge);

        auto writeRequestsCount = 0;
        auto readRequestsCount = 0;
        runtime->FilterFunction =
            [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() ==
                NDDisk::TEvWritePersistentBuffer::EventType)
            {
                if (writeRequestsCount++ < 2) {
                    return false;
                }
            }

            if (ev->GetTypeRewrite() ==
                NDDisk::TEvReadPersistentBuffer::EventType)
            {
                if (readRequestsCount++ < 1) {
                    auto response =
                        std::make_unique<NDDisk::TEvReadPersistentBufferResult>(
                            NKikimrBlobStorage::NDDisk::
                                TReplyStatus_E_INCORRECT_REQUEST,
                            "Disk not found");

                    runtime->Send(
                        new IEventHandle(
                            ev->Sender,
                            ev->Recipient,
                            response.release(),
                            0,
                            ev->Cookie),
                        nodeId);

                    return false;
                }
            }

            return true;
        };

        auto expectedData = TString(1024, 'A') + TString(1024, 'B') +
                            TString(1024, 'C') + TString(1024, 'D');
        {
            auto request =
                std::make_unique<TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(expectedData);

            runtime->Send(
                new IEventHandle(loadActorAdapter, edge, request.release()),
                edge.NodeId());

            auto res =
                env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                    edge,
                    false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        }

        // Read written block from persistent buffer
        {
            auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.SetBlocksCount(1);

            runtime->Send(
                new IEventHandle(loadActorAdapter, edge, request.release()),
                edge.NodeId());

            auto res =
                env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
                    edge,
                    false);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                res->Get()->Record.GetError().GetCode(),
                FormatError(res->Get()->Record.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                res->Get()->Record.GetBlocks().BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(
                res->Get()->Record.GetBlocks().GetBuffers(0),
                expectedData);
        }
    }

    Y_UNIT_TEST(ShouldRestorePartitionAfterRestart)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::PBufferReplication);

        auto partition = CreatePartitionTablet(env);

        auto expectedData = TString(1024, 'A') + TString(1024, 'B') +
                            TString(1024, 'C') + TString(1024, 'D');

        {
            const TActorId& edge = runtime->AllocateEdgeActor(
                env.Settings.ControllerNodeId,
                __FILE__,
                __LINE__);

            auto loadActorAdapter =
                GetLoadActorAdapterActorId(env, partition, edge);

            auto request =
                std::make_unique<TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(expectedData);

            runtime->Send(
                new IEventHandle(loadActorAdapter, edge, request.release()),
                edge.NodeId());

            auto res =
                env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
                    edge,
                    false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        }

        {
            env.RestartNode(env.Settings.ControllerNodeId);
            env.Sim(TDuration::Seconds(1));
        }

        WaitForTabletBoot(env);
        // Wait for tablet to be restored
        env.Sim(TDuration::Seconds(10));

        {
            const TActorId& edge = runtime->AllocateEdgeActor(
                env.Settings.ControllerNodeId,
                __FILE__,
                __LINE__);

            auto loadActorAdapter =
                GetLoadActorAdapterActorId(env, partition, edge);

            auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.SetBlocksCount(1);

            runtime->Send(
                new IEventHandle(loadActorAdapter, edge, request.release()),
                edge.NodeId());

            auto res =
                env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
                    edge,
                    false);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                res->Get()->Record.GetError().GetCode(),
                FormatError(res->Get()->Record.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                res->Get()->Record.GetBlocks().BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(
                res->Get()->Record.GetBlocks().GetBuffers(0),
                expectedData);
        }
    }

    // PBuffer cleanup: once the write LSN advances by PBufferCleanupLsnStep the
    // tablet barrier-erases PBuffer records up to the cleanup bound. Drive two
    // write batches and assert a real barrier-erase (TEvErasePersistentBuffer
    // with Lsn > 0) reaches the persistent buffer, with no data lost.
    Y_UNIT_TEST(ShouldBarrierErasePBufferOnCleanup)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(
            env,
            EWriteMode::DirectPBuffersFilling,
            TDuration::Seconds(1),
            /*pbufferCleanupLsnStep=*/4,
            /*syncRequestsBatchSize=*/1);

        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        auto loadActorAdapter =
            GetLoadActorAdapterActorId(env, partition, edge);

        TVector<ui64> barrierEraseLsns;
        runtime->FilterFunction =
            [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() ==
                NDDisk::TEvErasePersistentBuffer::EventType)
            {
                barrierEraseLsns.push_back(
                    ev->Get<NDDisk::TEvErasePersistentBuffer>()
                        ->Record.GetLsn());
            }
            return true;
        };

        constexpr ui64 BlockCount = 16;
        TVector<TString> data(BlockCount);
        for (ui64 i = 0; i < BlockCount; ++i) {
            data[i] = NUnitTest::RandomString(DefaultBlockSize, i);
        }

        // First batch: let it flush+erase so the cleanup floor moves past
        // lsn 1 (a barrier of lsn 0 is suppressed by design).
        for (ui64 i = 0; i < BlockCount / 2; ++i) {
            WriteBlock(env, loadActorAdapter, edge, i, data[i]);
        }
        env.Sim(TDuration::Seconds(10));

        // Second batch: still in flight when cleanup triggers, so the barrier
        // fires at (oldest-in-flight lsn - 1) > 0.
        for (ui64 i = BlockCount / 2; i < BlockCount; ++i) {
            WriteBlock(env, loadActorAdapter, edge, i, data[i]);
        }
        env.Sim(TDuration::Seconds(10));

        ui64 maxBarrierLsn = 0;
        for (const ui64 lsn: barrierEraseLsns) {
            if (lsn > maxBarrierLsn) {
                maxBarrierLsn = lsn;
            }
        }
        // A real barrier reached the PBuffer and never erased the newest write
        // (exact lsn is timing-dependent in this sustained-flow test).
        UNIT_ASSERT_C(
            maxBarrierLsn > 0 && maxBarrierLsn < BlockCount,
            "barrier lsn outside (0, " << BlockCount << "): " << maxBarrierLsn);

        // Nothing extra was erased: every block still reads back correctly.
        for (ui64 i = 0; i < BlockCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                ReadBlock(env, loadActorAdapter, edge, i),
                data[i]);
        }
    }

    // PBuffer cleanup must never barrier-erase a record that has not been
    // per-record erased yet (still in the dirty map) - even one already
    // flushed to DDisk; erasing it out from under the dirty map desyncs it
    // from the PBuffer. We let an initial batch flush+erase (advancing the
    // floor so the barrier fires), then hold back every per-record erase and
    // assert the barrier stops at the floor, never reaching the un-erased
    // records.
    Y_UNIT_TEST(ShouldNotBarrierEraseUnerasedRecords)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(
            env,
            EWriteMode::DirectPBuffersFilling,
            TDuration::Seconds(1),
            /*pbufferCleanupLsnStep=*/2,
            /*syncRequestsBatchSize=*/1);

        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        auto loadActorAdapter =
            GetLoadActorAdapterActorId(env, partition, edge);

        bool holdErases = false;
        TVector<ui64> barrierEraseLsns;
        runtime->FilterFunction =
            [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev)
        {
            const auto type = ev->GetTypeRewrite();
            if (type == NDDisk::TEvErasePersistentBuffer::EventType) {
                barrierEraseLsns.push_back(
                    ev->Get<NDDisk::TEvErasePersistentBuffer>()
                        ->Record.GetLsn());
            } else if (
                holdErases &&
                type == NDDisk::TEvBatchErasePersistentBuffer::EventType)
            {
                return false;   // flush succeeds, but the record is never
                                // erased
            }
            return true;
        };

        // lsn == cumulative write count (one lsn per write on a fresh volume),
        // so the first ErasedCount writes get lsns 1..ErasedCount and the held
        // records get lsns > ErasedCount.
        constexpr ui64 ErasedCount = 4;
        constexpr ui64 UnerasedCount = 8;
        TVector<TString> data(ErasedCount + UnerasedCount);
        for (ui64 i = 0; i < data.size(); ++i) {
            data[i] = NUnitTest::RandomString(DefaultBlockSize, 1000 + i);
        }

        // Erased batch: flush + per-record erase, advancing the cleanup floor
        // past lsn 1 so the barrier can fire.
        for (ui64 i = 0; i < ErasedCount; ++i) {
            WriteBlock(env, loadActorAdapter, edge, i, data[i]);
        }
        env.Sim(TDuration::Seconds(10));

        // From now on records flush but are never erased -> they stay in the
        // dirty map, so the barrier must not advance past them.
        holdErases = true;
        for (ui64 i = ErasedCount; i < ErasedCount + UnerasedCount; ++i) {
            WriteBlock(env, loadActorAdapter, edge, i, data[i]);
        }
        env.Sim(TDuration::Seconds(5));

        ui64 maxBarrierLsn = 0;
        for (const ui64 lsn: barrierEraseLsns) {
            if (lsn > maxBarrierLsn) {
                maxBarrierLsn = lsn;
            }
        }
        // Floor pinned at ErasedCount+1, so the barrier lands exactly at
        // ErasedCount and never reaches the un-erased records.
        UNIT_ASSERT_VALUES_EQUAL(maxBarrierLsn, ErasedCount);

        // The un-erased records still read back correctly.
        for (ui64 i = ErasedCount; i < ErasedCount + UnerasedCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                ReadBlock(env, loadActorAdapter, edge, i),
                data[i]);
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
