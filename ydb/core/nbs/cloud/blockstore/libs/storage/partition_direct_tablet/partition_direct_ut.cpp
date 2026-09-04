#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/region.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_cleanup_actor.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct_actor.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/library/actors/core/mon.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <set>

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

[[nodiscard]] NKikimrConfig::TNbsConfig CreateNbsConfig(
    EWriteMode writeMode,
    TDuration writeHedgingDelay = TDuration::Seconds(1),
    ui64 pbufferCleanupLsnStep = 0,
    ui32 syncRequestsBatchSize = 0)
{
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

    return nbsConfig;
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] std::unique_ptr<TScopedNbsService> SetupStorage(
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
    return std::make_unique<TScopedNbsService>(CreateNbsConfig(
        writeMode,
        writeHedgingDelay,
        pbufferCleanupLsnStep,
        syncRequestsBatchSize));
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

TActorId WaitForTabletBoot(TEnvironmentSetup& env)
{
    // Create tablet like in SetupTablet()
    const TActorId bootstrapperId = env.Runtime->CreateTestBootstrapper(
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

    return bootstrapperId;
}

ui64 CreatePartitionTablet(
    TEnvironmentSetup& env,
    ui64 blockCount = 32768,
    TActorId* outBootstrapperId = nullptr)
{
    const TActorId createdBootstrapperId = WaitForTabletBoot(env);
    if (outBootstrapperId) {
        *outBootstrapperId = createdBootstrapperId;
    }

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

TPersistResultFuture SendVChunkConfigUpdate(
    TEnvironmentSetup& env,
    ui64 partitionTabletId,
    ui32 vChunkIndex)
{
    auto config = TVChunkConfig::MakeDefault(
        vChunkIndex,
        DirectBlockGroupHostCount,
        DefaultPrimaryCount);

    auto request =
        std::make_unique<TEvPartitionDirectPrivate::TEvUpdateVChunkConfig>(
            std::move(config));
    auto future = request->UpdateCompleted.GetFuture();

    const TActorId sender = env.Runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);
    env.Runtime->SendToPipe(
        partitionTabletId,
        sender,
        request.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());
    env.Runtime->DestroyActor(sender);

    return future;
}

TPersistResultFuture SendDirtyMapStateUpdate(
    TEnvironmentSetup& env,
    ui64 partitionTabletId,
    ui32 vChunkIndex,
    ui32 stateGeneration)
{
    TDirtyMapStateProto state;
    state.SetStateGeneration(stateGeneration);

    auto request =
        std::make_unique<TEvPartitionDirectPrivate::TEvUpdateDirtyMapState>(
            vChunkIndex,
            std::move(state));
    auto future = request->UpdateCompleted.GetFuture();

    const TActorId sender = env.Runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);
    env.Runtime->SendToPipe(
        partitionTabletId,
        sender,
        request.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());
    env.Runtime->DestroyActor(sender);

    return future;
}

NProto::TError DeletePartition(
    TEnvironmentSetup& env,
    ui64 partitionTabletId,
    const TActorId& edge)
{
    auto request = std::make_unique<TEvService::TEvDeletePartitionRequest>();
    env.Runtime->SendToPipe(
        partitionTabletId,
        edge,
        request.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());

    auto res =
        env.WaitForEdgeActorEvent<TEvService::TEvDeletePartitionResponse>(
            edge,
            false);
    UNIT_ASSERT(res);
    return res->Get()->GetError();
}

NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult
SendBscDirectBlockGroupOperation(
    TEnvironmentSetup& env,
    ui64 tabletId,
    ui64 directBlockGroupId,
    const std::function<void(
        NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup::
            TDirectBlockGroupOperation*)>& fill)
{
    auto ev = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    auto& r = ev->Record;
    r.SetDDiskPoolName(DDiskPoolName);
    r.SetPersistentBufferDDiskPoolName(PersistentBufferDDiskPoolName);
    r.SetTabletId(tabletId);
    auto* op = r.AddDirectBlockGroupOperations();
    op->SetDirectBlockGroupId(directBlockGroupId);
    fill(op);

    const TActorId edge = env.Runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);
    env.Runtime->SendToPipe(
        MakeBSControllerID(),
        edge,
        ev.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());
    auto response = env.WaitForEdgeActorEvent<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(edge);
    UNIT_ASSERT(response);
    return response->Get()->Record;
}

void StopFastPathService(
    TEnvironmentSetup& env,
    ui64 partitionTabletId,
    const TActorId& edge)
{
    auto request = std::make_unique<
        TEvPartitionDirectPrivate::TEvFastPathServiceShutdown>();
    env.Runtime->SendToPipe(
        partitionTabletId,
        edge,
        request.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());

    auto res = env.WaitForEdgeActorEvent<
        TEvPartitionDirectPrivate::TEvFastPathServiceStopped>(edge, false);
    UNIT_ASSERT(res);
}

TActorId TryGetLoadActorAdapterActorId(
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
    NActors::TActorId loadActorAdapter;
    const auto& actorIdStr = res->Get()->Record.GetActorId();
    UNIT_ASSERT(loadActorAdapter.Parse(actorIdStr.data(), actorIdStr.size()));
    return loadActorAdapter;
}

TActorId GetLoadActorAdapterActorId(
    TEnvironmentSetup& env,
    ui64 partitionTabletId,
    const TActorId& edge)
{
    auto loadActorAdapter =
        TryGetLoadActorAdapterActorId(env, partitionTabletId, edge);
    UNIT_ASSERT(loadActorAdapter);
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

void WriteBlockExpectFailure(
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
        new IEventHandle(
            loadActorAdapter,
            edge,
            request.release(),
            IEventHandle::FlagTrackDelivery),
        edge.NodeId());

    auto ev = env.Runtime->WaitForEdgeActorEvent({edge});
    if (ev->GetTypeRewrite() == TEvService::TEvWriteBlocksResponse::EventType) {
        const auto& error =
            ev->Get<TEvService::TEvWriteBlocksResponse>()->Record.GetError();
        UNIT_ASSERT_C(error.GetCode() != S_OK, FormatError(error));
        return;
    }

    UNIT_ASSERT_VALUES_EQUAL(
        NActors::TEvents::TEvUndelivered::EventType,
        ev->GetTypeRewrite());
}

TString DDiskKey(const NKikimrBlobStorage::NDDisk::TDDiskId& id)
{
    return TStringBuilder() << id.GetNodeId() << ":" << id.GetPDiskId() << ":"
                            << id.GetDDiskSlotId();
}

ui32 UniqueDDiskCount(const TVector<NKikimrBlobStorage::NDDisk::TDDiskId>& ids)
{
    THashSet<TString> keys;
    for (const auto& id: ids) {
        keys.insert(DDiskKey(id));
    }
    return keys.size();
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
        if (ev->GetTypeRewrite() == NDDisk::TEvSync::EventType) {
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

    StopFastPathService(env, partition, edge);
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

    StopFastPathService(env, partition, edge);
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

    StopFastPathService(env, partition, edge);
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

            auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
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

            auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
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

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);

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

        const ui64 partition = CreatePartitionTablet(
            env,
            4 * BlocksPerRegion + 1   // blockCount
        );

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        StopFastPathService(env, partition, edge);
    }

    Y_UNIT_TEST(ShouldRequestDDiskAllocationForAddedHost)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);

        // The add-host allocation is the only request that uses
        // DirectBlockGroupOperations (the initial bulk allocation uses
        // Queries).
        ui32 addHostRequestCount = 0;
        ui32 addHostNumDDisks = 0;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType)
            {
                auto* msg = ev->Get<
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
                if (msg->Record.DirectBlockGroupOperationsSize() > 0) {
                    ++addHostRequestCount;
                    addHostNumDDisks =
                        msg->Record.GetDirectBlockGroupOperations(0)
                            .GetDefineDirectBlockGroup()
                            .GetNumDDisks();
                }
            }
            return true;
        };

        const ui64 partition = CreatePartitionTablet(env);

        // Drop the throwaway sender right after sending: its strict edge actor
        // would otherwise abort on the pipe notifications during the
        // free-running Sim. The payload is still delivered by the pipe client.
        const TActorId sender = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        env.Runtime->SendToPipe(
            partition,
            sender,
            new TEvPartitionDirectPrivate::TEvAddHostToDBG(0, 0),
            0,
            TTestActorSystem::GetPipeConfigWithRetries());
        runtime->DestroyActor(sender);

        env.Sim(TDuration::Seconds(10));

        // The add persisted its intent and asked BSController to grow the group
        // to DirectBlockGroupHostCount + 1 DDisks.
        UNIT_ASSERT_VALUES_EQUAL(1u, addHostRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(DirectBlockGroupHostCount + 1),
            addHostNumDDisks);
    }

    Y_UNIT_TEST(ShouldReplayInFlightAddHostAfterRestart)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);

        ui32 addHostRequestCount = 0;
        bool dropNextAddHostResult = false;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            const auto type = ev->GetTypeRewrite();
            if (type ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType)
            {
                auto* msg = ev->Get<
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
                if (msg->Record.DirectBlockGroupOperationsSize() > 0) {
                    ++addHostRequestCount;
                }
            }
            // Drop the first add-host result so the connection is never
            // persisted (the intent stays), forcing a replay on restart.
            if (dropNextAddHostResult &&
                type ==
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
                        EventType)
            {
                dropNextAddHostResult = false;
                return false;
            }
            return true;
        };

        const ui64 partition = CreatePartitionTablet(env);

        // Trigger an add whose BSController result is dropped: the intent is
        // persisted but the connection is not.
        dropNextAddHostResult = true;
        {
            const TActorId sender = runtime->AllocateEdgeActor(
                env.Settings.ControllerNodeId,
                __FILE__,
                __LINE__);
            env.Runtime->SendToPipe(
                partition,
                sender,
                new TEvPartitionDirectPrivate::TEvAddHostToDBG(0, 0),
                0,
                TTestActorSystem::GetPipeConfigWithRetries());
            runtime->DestroyActor(sender);
        }
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1u, addHostRequestCount);

        // Restart: the persisted intent must be replayed.
        {
            scopedService.reset();
            env.RestartNode(env.Settings.ControllerNodeId);
            env.Sim(TDuration::Seconds(1));
            scopedService = std::make_unique<TScopedNbsService>(
                CreateNbsConfig(EWriteMode::DirectWrite));
        }
        WaitForTabletBoot(env);
        env.Sim(TDuration::Seconds(10));

        // The replay re-sent the BSController allocation request.
        UNIT_ASSERT_VALUES_EQUAL(2u, addHostRequestCount);
    }

    Y_UNIT_TEST(ShouldBatchDirtyMapStateUpdates)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        const ui64 partition = CreatePartitionTablet(env);

        TVector<std::unique_ptr<IEventHandle>> blockedCommits;
        THashSet<ui32> releasedCommitSteps;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() == TEvTablet::TEvCommit::EventType) {
                auto* msg = ev->Get<TEvTablet::TEvCommit>();
                if (msg->TabletID == partition &&
                    !releasedCommitSteps.contains(msg->Step))
                {
                    blockedCommits.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };

        auto releaseCommit = [&](size_t index)
        {
            UNIT_ASSERT_C(
                index < blockedCommits.size() && blockedCommits[index],
                "commit is not blocked");
            auto* msg = blockedCommits[index]->Get<TEvTablet::TEvCommit>();
            releasedCommitSteps.insert(msg->Step);
            runtime->Send(
                std::move(blockedCommits[index]),
                env.Settings.ControllerNodeId);
        };

        auto first = SendDirtyMapStateUpdate(env, partition, 0, 1);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1u, blockedCommits.size());
        UNIT_ASSERT(!first.HasValue());

        TVector<TPersistResultFuture> batched;
        batched.push_back(SendDirtyMapStateUpdate(env, partition, 1, 2));
        batched.push_back(SendDirtyMapStateUpdate(env, partition, 2, 3));
        batched.push_back(SendDirtyMapStateUpdate(env, partition, 3, 4));
        env.Sim(TDuration::Seconds(1));

        // While the first transaction is in flight, the remaining updates do
        // not start their own transactions.
        UNIT_ASSERT_VALUES_EQUAL(1u, blockedCommits.size());
        for (const auto& future: batched) {
            UNIT_ASSERT(!future.HasValue());
        }

        releaseCommit(0);
        env.Sim(TDuration::Seconds(1));

        // All pending updates are persisted by one transaction. Its promises
        // stay unresolved until the common commit completes.
        UNIT_ASSERT_VALUES_EQUAL(2u, blockedCommits.size());
        UNIT_ASSERT(first.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(EPersistResult::Success, first.GetValue());
        for (const auto& future: batched) {
            UNIT_ASSERT(!future.HasValue());
        }

        releaseCommit(1);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2u, blockedCommits.size());
        for (const auto& future: batched) {
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                EPersistResult::Success,
                future.GetValue());
        }

        // Completion of a batch resets the in-flight state: a later update
        // starts and completes a new transaction normally.
        auto next = SendDirtyMapStateUpdate(env, partition, 4, 5);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(3u, blockedCommits.size());
        UNIT_ASSERT(!next.HasValue());

        releaseCommit(2);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(next.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(EPersistResult::Success, next.GetValue());

        runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(ShouldBatchVChunkConfigUpdates)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        const ui64 partition = CreatePartitionTablet(env);

        TVector<std::unique_ptr<IEventHandle>> blockedCommits;
        THashSet<ui32> releasedCommitSteps;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() == TEvTablet::TEvCommit::EventType) {
                auto* msg = ev->Get<TEvTablet::TEvCommit>();
                if (msg->TabletID == partition &&
                    !releasedCommitSteps.contains(msg->Step))
                {
                    blockedCommits.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };

        auto releaseCommit = [&](size_t index)
        {
            UNIT_ASSERT_C(
                index < blockedCommits.size() && blockedCommits[index],
                "commit is not blocked");
            auto* msg = blockedCommits[index]->Get<TEvTablet::TEvCommit>();
            releasedCommitSteps.insert(msg->Step);
            runtime->Send(
                std::move(blockedCommits[index]),
                env.Settings.ControllerNodeId);
        };

        auto first = SendVChunkConfigUpdate(env, partition, 0);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1u, blockedCommits.size());
        UNIT_ASSERT(!first.HasValue());

        TVector<TPersistResultFuture> batched;
        batched.push_back(SendVChunkConfigUpdate(env, partition, 1));
        batched.push_back(SendVChunkConfigUpdate(env, partition, 2));
        batched.push_back(SendVChunkConfigUpdate(env, partition, 3));
        env.Sim(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1u, blockedCommits.size());
        for (const auto& future: batched) {
            UNIT_ASSERT(!future.HasValue());
        }

        releaseCommit(0);
        env.Sim(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2u, blockedCommits.size());
        UNIT_ASSERT(first.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(EPersistResult::Success, first.GetValue());
        for (const auto& future: batched) {
            UNIT_ASSERT(!future.HasValue());
        }

        releaseCommit(1);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2u, blockedCommits.size());
        for (const auto& future: batched) {
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                EPersistResult::Success,
                future.GetValue());
        }

        auto next = SendVChunkConfigUpdate(env, partition, 4);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(3u, blockedCommits.size());
        UNIT_ASSERT(!next.HasValue());

        releaseCommit(2);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(next.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(EPersistResult::Success, next.GetValue());
    }

    Y_UNIT_TEST(ShouldFailStateUpdatesWhenPartitionStops)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        TActorId bootstrapperId;
        const ui64 partition =
            CreatePartitionTablet(env, 32768, &bootstrapperId);

        TVector<std::unique_ptr<IEventHandle>> blockedCommitResults;
        bool bootstrapperDeathObserved = false;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() == TEvTablet::TEvCommitResult::EventType) {
                auto* msg = ev->Get<TEvTablet::TEvCommitResult>();
                if (msg->TabletID == partition) {
                    blockedCommitResults.push_back(std::move(ev));
                    return false;
                }
            }

            if (ev->GetTypeRewrite() == TEvTablet::TEvTabletDead::EventType) {
                auto* msg = ev->Get<TEvTablet::TEvTabletDead>();
                if (msg->TabletID == partition &&
                    ev->GetRecipientRewrite() == bootstrapperId)
                {
                    bootstrapperDeathObserved = true;
                    return false;
                }
            }
            return true;
        };

        auto executingConfig = SendVChunkConfigUpdate(env, partition, 0);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1u, blockedCommitResults.size());

        auto pendingConfig = SendVChunkConfigUpdate(env, partition, 1);
        auto executingDirtyMap = SendDirtyMapStateUpdate(env, partition, 0, 1);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2u, blockedCommitResults.size());

        auto pendingDirtyMap = SendDirtyMapStateUpdate(env, partition, 1, 2);
        env.Sim(TDuration::Seconds(1));

        UNIT_ASSERT(!executingConfig.HasValue());
        UNIT_ASSERT(!pendingConfig.HasValue());
        UNIT_ASSERT(!executingDirtyMap.HasValue());
        UNIT_ASSERT(!pendingDirtyMap.HasValue());

        const TActorId sender = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        runtime->SendToPipe(
            partition,
            sender,
            new TEvPartitionDirectPrivate::TEvPoison("test shutdown"),
            0,
            TTestActorSystem::GetPipeConfigWithRetries());
        runtime->DestroyActor(sender);

        env.Sim(TDuration::Seconds(10));

        UNIT_ASSERT(bootstrapperDeathObserved);
        for (const auto& future:
             {executingConfig,
              pendingConfig,
              executingDirtyMap,
              pendingDirtyMap})
        {
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                EPersistResult::Cancelled,
                future.GetValue());
        }
    }

    Y_UNIT_TEST(BasicWriteReadPBufferReplication)
    {
        BasicWriteRead(EWriteMode::IndirectWrite);
    }

    Y_UNIT_TEST(BasicWriteReadDirectPBufferFilling)
    {
        BasicWriteRead(EWriteMode::DirectWrite);
    }

    Y_UNIT_TEST(ShouldWriteAndReadBlocksInDifferentRegionsPBufferReplication)
    {
        ShouldWriteAndReadBlocksInDifferentRegions(EWriteMode::IndirectWrite);
    }

    Y_UNIT_TEST(ShouldWriteAndReadBlocksInDifferentRegionsDirectPBufferFilling)
    {
        ShouldWriteAndReadBlocksInDifferentRegions(EWriteMode::DirectWrite);
    }

    Y_UNIT_TEST(RandomWritesPBufferReplication)
    {
        RandomWrites(EWriteMode::IndirectWrite);
    }

    Y_UNIT_TEST(RandomWritesDirectPBufferFilling)
    {
        RandomWrites(EWriteMode::DirectWrite);
    }

    Y_UNIT_TEST(ShouldWriteAndReadMultipleBlocksPBufferReplication)
    {
        ShouldWriteAndReadMultipleBlocks(EWriteMode::IndirectWrite);
    }

    Y_UNIT_TEST(ShouldWriteAndReadMultipleBlocksDirectPBufferFilling)
    {
        ShouldWriteAndReadMultipleBlocks(EWriteMode::DirectWrite);
    }

    // Test implementation for IndirectWrite write mode
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

        // set big writeHedgingDelay for test pure fallback to direct writes
        auto scopedService = SetupStorage(
            env,
            EWriteMode::IndirectWrite,
            TDuration::Seconds(10));

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

        StopFastPathService(env, partition, edge);
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

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);

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

        StopFastPathService(env, partition, edge);
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

        auto scopedService = SetupStorage(env, EWriteMode::IndirectWrite);

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
            scopedService.reset();

            env.RestartNode(env.Settings.ControllerNodeId);
            env.Sim(TDuration::Seconds(1));

            scopedService = std::make_unique<TScopedNbsService>(
                CreateNbsConfig(EWriteMode::IndirectWrite));
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
            EWriteMode::DirectWrite,
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

        StopFastPathService(env, partition, edge);
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
            EWriteMode::DirectWrite,
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

        StopFastPathService(env, partition, edge);
    }

    // The tablet-wide cleanup barrier is broadcast per DBG, and many DBGs of
    // the tablet share the same pbuffer endpoints (each node/pdisk/slot keeps
    // one barrier per tabletId). Without dedup a single cleanup tick sends the
    // same barrier lsn to a shared endpoint once per DBG on it - a
    // non-advancing MoveBarrier that DDisk logs as an error. Assert each
    // pbuffer receives any given barrier lsn at most once.
    Y_UNIT_TEST(ShouldNotResendSameBarrierToPBuffer)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(
            env,
            EWriteMode::DirectWrite,
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

        // Barrier lsn of every TEvErasePersistentBuffer, grouped by the pbuffer
        // it was sent to.
        TMap<TActorId, TVector<ui64>> barrierLsnsByRecipient;
        runtime->FilterFunction =
            [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() ==
                NDDisk::TEvErasePersistentBuffer::EventType)
            {
                barrierLsnsByRecipient[ev->GetRecipientRewrite()].push_back(
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

        // Two batches so the cleanup floor moves past lsn 1 and a non-zero
        // barrier fires while later writes are still in flight - each cleanup
        // tick fans the same bound out to the shared pbuffer endpoints.
        for (ui64 i = 0; i < BlockCount / 2; ++i) {
            WriteBlock(env, loadActorAdapter, edge, i, data[i]);
        }
        env.Sim(TDuration::Seconds(10));
        for (ui64 i = BlockCount / 2; i < BlockCount; ++i) {
            WriteBlock(env, loadActorAdapter, edge, i, data[i]);
        }
        env.Sim(TDuration::Seconds(10));

        // A non-zero barrier actually reached a pbuffer (so the check is not
        // vacuous), and no pbuffer received the same barrier lsn twice.
        ui64 maxBarrierLsn = 0;
        for (const auto& [recipient, lsns]: barrierLsnsByRecipient) {
            THashSet<ui64> seen;
            for (const ui64 lsn: lsns) {
                UNIT_ASSERT_C(
                    seen.insert(lsn).second,
                    "pbuffer " << recipient << " got barrier lsn " << lsn
                               << " more than once");
                maxBarrierLsn = Max(maxBarrierLsn, lsn);
            }
        }
        UNIT_ASSERT_C(
            maxBarrierLsn > 0,
            "no non-zero barrier reached a pbuffer");

        // No data lost.
        for (ui64 i = 0; i < BlockCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                ReadBlock(env, loadActorAdapter, edge, i),
                data[i]);
        }

        StopFastPathService(env, partition, edge);
    }

    Y_UNIT_TEST(MonitoringPageRenders)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        const ui64 tabletId = CreatePartitionTablet(env);

        const TActorId edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        runtime->SendToPipe(
            tabletId,
            edge,
            new NActors::NMon::TEvRemoteHttpInfo(
                "/app?TabletID=" + ToString(tabletId)),
            0,
            TTestActorSystem::GetPipeConfigWithRetries());

        auto response =
            env.WaitForEdgeActorEvent<NActors::NMon::TEvRemoteHttpInfoRes>(
                edge);
        UNIT_ASSERT(response);

        const TString& html = response->Get()->Html;
        UNIT_ASSERT(!html.empty());
        UNIT_ASSERT_STRING_CONTAINS(html, "partition_direct tablet");
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
    }

    Y_UNIT_TEST(ChaosMonitoringPageUpdatesNodeState)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        const ui64 tabletId = CreatePartitionTablet(env);
        const TActorId edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        const auto query = [&](TString request, HTTP_METHOD method)
        {
            runtime->SendToPipe(
                tabletId,
                edge,
                new NActors::NMon::TEvRemoteHttpInfo(request, method),
                0,
                TTestActorSystem::GetPipeConfigWithRetries());

            auto response =
                env.WaitForEdgeActorEvent<NActors::NMon::TEvRemoteHttpInfoRes>(
                    edge,
                    false);
            UNIT_ASSERT(response);
            return response->Get()->Html;
        };

        constexpr ui32 NodeId = 100500;
        const TString requestPrefix =
            "/app?TabletID=" + ToString(tabletId) + "&page=chaos";

        const TString initial = query(requestPrefix, HTTP_METHOD_GET);
        UNIT_ASSERT_STRING_CONTAINS(initial, "DBG #0");

        const TString disable = query(
            requestPrefix + "&action=disable&node=" + ToString(NodeId) +
                "&dbg=0",
            HTTP_METHOD_POST);
        UNIT_ASSERT_STRING_CONTAINS(disable, "configuration updated");

        const TString disabled = query(requestPrefix, HTTP_METHOD_GET);
        UNIT_ASSERT_STRING_CONTAINS(disabled, "Node 100500");
        UNIT_ASSERT_STRING_CONTAINS(
            disabled,
            "action=enable&node=100500&dbg=0");

        const TString disableAll = query(
            requestPrefix + "&action=disable&node=" + ToString(NodeId) +
                "&dbg=all",
            HTTP_METHOD_POST);
        UNIT_ASSERT_STRING_CONTAINS(disableAll, "configuration updated");

        const TString allDisabled = query(requestPrefix, HTTP_METHOD_GET);
        UNIT_ASSERT_STRING_CONTAINS(
            allDisabled,
            "action=enable&node=100500&dbg=all");

        const TString enable = query(
            requestPrefix + "&action=enable&node=" + ToString(NodeId) +
                "&dbg=all",
            HTTP_METHOD_POST);
        UNIT_ASSERT_STRING_CONTAINS(enable, "configuration updated");

        const TString enabled = query(requestPrefix, HTTP_METHOD_GET);
        UNIT_ASSERT_STRING_CONTAINS(
            enabled,
            "action=disable&node=100500&dbg=0");
    }

    Y_UNIT_TEST(StandardTabletPageRenders)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        const ui64 tabletId = CreatePartitionTablet(env);

        const TActorId edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        // Empty path (not "/app") renders the standard flat-tablet page.
        runtime->SendToPipe(
            tabletId,
            edge,
            new NActors::NMon::TEvRemoteHttpInfo(
                "?TabletID=" + ToString(tabletId)),
            0,
            TTestActorSystem::GetPipeConfigWithRetries());

        auto response =
            env.WaitForEdgeActorEvent<NActors::NMon::TEvRemoteHttpInfoRes>(
                edge);
        UNIT_ASSERT(response);

        const TString& html = response->Get()->Html;
        // The standard tablet page: the info block, the Restart action, and the
        // "App" link to this tablet's own monitoring page.
        UNIT_ASSERT_STRING_CONTAINS(html, "Tablet generation");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "RestartTabletID=" + ToString(tabletId));
        UNIT_ASSERT_STRING_CONTAINS(html, "tablets/app?");
    }

    Y_UNIT_TEST(ShouldSuicideOnPoisonByBlockedGeneration)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        TActorId bootstrapperId;
        const ui64 tabletId =
            CreatePartitionTablet(env, 32768, &bootstrapperId);

        const TActorId edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        // Observe the tablet death via TEvTabletDead.
        bool bootstrapperDeathObserved = false;
        bool partitionDeathDelivered = false;
        const auto isExpectedTabletDeath = [&](IEventHandle& ev)
        {
            if (ev.GetTypeRewrite() != TEvTablet::TEvTabletDead::EventType) {
                return false;
            }

            const auto* msg = ev.Get<TEvTablet::TEvTabletDead>();
            if (msg->TabletID != tabletId) {
                return false;
            }

            UNIT_ASSERT_VALUES_EQUAL(
                TEvTablet::TEvTabletDead::ReasonPill,
                msg->Reason);
            return true;
        };
        runtime->FilterFunction =
            [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
        {
            Y_UNUSED(nodeId);
            if (isExpectedTabletDeath(*ev) &&
                ev->GetRecipientRewrite() == bootstrapperId)
            {
                bootstrapperDeathObserved = true;
                // Do not let the bootstrapper start a new tablet incarnation:
                // this test only verifies that the current one dies.
                return false;
            }
            if (ev->GetRecipientRewrite() == edge) {
                return false;
            }
            return true;
        };

        runtime->SendToPipe(
            tabletId,
            edge,
            std::make_unique<TEvPartitionDirectPrivate::TEvPoison>("test")
                .release(),
            0,
            TTestActorSystem::GetPipeConfigWithRetries());

        runtime->Sim(
            [&]
            { return !bootstrapperDeathObserved || !partitionDeathDelivered; },
            [&](IEventHandle& ev)
            {
                if (isExpectedTabletDeath(ev) &&
                    ev.GetRecipientRewrite() != bootstrapperId)
                {
                    partitionDeathDelivered = true;
                }
            });

        UNIT_ASSERT(bootstrapperDeathObserved);
        UNIT_ASSERT(partitionDeathDelivered);
    }

    Y_UNIT_TEST(ShouldRestartOnTabletPipePoison)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        TActorId bootstrapperId;
        const ui64 tabletId =
            CreatePartitionTablet(env, 32768, &bootstrapperId);

        const TActorId edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        bool bootstrapperDeathObserved = false;
        bool rebootObserved = false;
        const auto isExpectedTabletDeath = [&](IEventHandle& ev)
        {
            if (ev.GetTypeRewrite() != TEvTablet::TEvTabletDead::EventType) {
                return false;
            }

            const auto* msg = ev.Get<TEvTablet::TEvTabletDead>();
            if (msg->TabletID != tabletId) {
                return false;
            }

            UNIT_ASSERT_VALUES_EQUAL(
                TEvTablet::TEvTabletDead::ReasonPill,
                msg->Reason);
            return true;
        };
        runtime->FilterFunction =
            [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
        {
            Y_UNUSED(nodeId);
            if (isExpectedTabletDeath(*ev) &&
                ev->GetRecipientRewrite() == bootstrapperId)
            {
                bootstrapperDeathObserved = true;
            }
            if (ev->GetRecipientRewrite() == edge) {
                return false;
            }
            return true;
        };

        // Same event RestartTablet sends over the tablet pipe.
        runtime->SendToPipe(
            tabletId,
            edge,
            new TEvents::TEvPoison(),
            0,
            TTestActorSystem::GetPipeConfigWithRetries());

        runtime->Sim(
            [&] { return !bootstrapperDeathObserved || !rebootObserved; },
            [&](IEventHandle& ev)
            {
                if (ev.GetTypeRewrite() == TEvTablet::EvBoot &&
                    bootstrapperDeathObserved)
                {
                    rebootObserved = true;
                }
            });

        UNIT_ASSERT(bootstrapperDeathObserved);
        UNIT_ASSERT(rebootObserved);
    }

    Y_UNIT_TEST(ShouldKeepOtherDBGConnectionsWhenAddingHosts)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);

        // Record every add-host allocation round-trip. The request's NumDDisks
        // is the partition's belief about the group's size (current + 1); the
        // result's DDiskId count is BSController's post-op group size. An add
        // to one DBG must not affect either side's view of any other DBG.
        struct TAddHostRoundTrip
        {
            ui64 DbgId = 0;
            ui32 RequestedNumDDisks = 0;
            ui32 ResultNumDDisks = 0;
        };

        TVector<TAddHostRoundTrip> roundTrips;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev)
        {
            const auto type = ev->GetTypeRewrite();
            if (type ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType)
            {
                auto* msg = ev->Get<
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
                if (msg->Record.DirectBlockGroupOperationsSize() == 1) {
                    const auto& op =
                        msg->Record.GetDirectBlockGroupOperations(0);
                    roundTrips.push_back({
                        .DbgId = op.GetDirectBlockGroupId(),
                        .RequestedNumDDisks =
                            op.GetDefineDirectBlockGroup().GetNumDDisks(),
                    });
                }
            }
            if (type ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
                    EventType)
            {
                auto* msg =
                    ev->Get<TEvBlobStorage::
                                TEvControllerAllocateDDiskBlockGroupResult>();
                if (msg->Record.DirectBlockGroupsSize() == 1 &&
                    !roundTrips.empty())
                {
                    roundTrips.back().ResultNumDDisks = static_cast<ui32>(
                        msg->Record.GetDirectBlockGroups(0).DDiskIdSize());
                }
            }
            return true;
        };

        const ui64 partition = CreatePartitionTablet(env);

        // See ShouldRequestDDiskAllocationForAddedHost for the throwaway
        // sender.
        auto addHost = [&](size_t dbgId, ui32 dbgConnectionsConfigGeneration)
        {
            const TActorId sender = runtime->AllocateEdgeActor(
                env.Settings.ControllerNodeId,
                __FILE__,
                __LINE__);
            env.Runtime->SendToPipe(
                partition,
                sender,
                new TEvPartitionDirectPrivate::TEvAddHostToDBG(
                    dbgId,
                    dbgConnectionsConfigGeneration),
                0,
                TTestActorSystem::GetPipeConfigWithRetries());
            runtime->DestroyActor(sender);
            env.Sim(TDuration::Seconds(10));
        };

        const auto defaultCount = static_cast<ui32>(DirectBlockGroupHostCount);

        // Grow DBG 0, then DBG 1: each add sees its own group at DBG
        // connections config generation 0 and grows only it.
        addHost(0, 0);
        addHost(1, 0);

        UNIT_ASSERT_VALUES_EQUAL(2u, roundTrips.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, roundTrips[0].DbgId);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 1,
            roundTrips[0].RequestedNumDDisks);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 1,
            roundTrips[0].ResultNumDDisks);
        UNIT_ASSERT_VALUES_EQUAL(1u, roundTrips[1].DbgId);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 1,
            roundTrips[1].RequestedNumDDisks);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 1,
            roundTrips[1].ResultNumDDisks);

        // A third add probes DBG 0 after DBG 1's add: the partition must still
        // carry DBG 0's grown connections (request 7), and BSController must
        // still hold its 6-disk group (result 7). DBG 0 is at DBG
        // connections config generation 1 after its own add; DBG 1's add did
        // not touch it.
        addHost(0, 1);

        UNIT_ASSERT_VALUES_EQUAL(3u, roundTrips.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, roundTrips[2].DbgId);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 2,
            roundTrips[2].RequestedNumDDisks);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 2,
            roundTrips[2].ResultNumDDisks);

        // Restart and probe DBG 0 again: the persisted connections must carry
        // all three adds. Only the request is asserted - it is sent before
        // (and regardless of) BSController's capacity for one more disk.
        {
            scopedService.reset();
            env.RestartNode(env.Settings.ControllerNodeId);
            env.Sim(TDuration::Seconds(1));
            scopedService = std::make_unique<TScopedNbsService>(
                CreateNbsConfig(EWriteMode::DirectWrite));
        }
        WaitForTabletBoot(env);
        env.Sim(TDuration::Seconds(10));

        addHost(0, 2);

        UNIT_ASSERT_VALUES_EQUAL(4u, roundTrips.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, roundTrips[3].DbgId);
        UNIT_ASSERT_VALUES_EQUAL(
            defaultCount + 3,
            roundTrips[3].RequestedNumDDisks);
    }

    Y_UNIT_TEST(ShouldDeletePartition)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> allocatedDDisks;
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> allocatedPBuffers;
        ui32 deallocateRequestCount = 0;

        // Wipe traffic: Max-lsn barrier erase hits every PBuffer;
        // DeleteTabletChunks hits every DDisk. Match request↔result by
        // (sender, cookie): request.Sender / result.Recipient are the
        // cleanup actor. Do not use the DDisk service id — RegisterService
        // aliases it to a different actor SelfId that appears as result.Sender.
        using TTransportCookie = std::pair<TActorId, ui64>;
        THashSet<TTransportCookie> pendingWipeBarriers;
        THashSet<TTransportCookie> wipeBarrierOks;
        THashSet<TTransportCookie> pendingDeleteChunks;
        THashSet<TTransportCookie> deleteChunksOks;
        bool captureWipeTraffic = false;
        bool deallocateBeforeWipeDone = false;
        bool deleteChunksBeforeWipeDone = false;
        ui32 deallocateOpSize = 0;
        bool deallocateOpMalformed = false;

        runtime->FilterFunction =
            [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev)
        {
            const ui32 type = ev->GetTypeRewrite();
            if (type ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::
                    EventType)
            {
                auto* msg =
                    ev->Get<TEvBlobStorage::
                                TEvControllerAllocateDDiskBlockGroupResult>();
                if (msg->Record.GetStatus() == NKikimrProto::OK &&
                    allocatedDDisks.empty())
                {
                    for (const auto& response: msg->Record.GetResponses()) {
                        for (const auto& node: response.GetNodes()) {
                            if (node.HasDDiskId()) {
                                allocatedDDisks.push_back(node.GetDDiskId());
                            }
                            if (node.HasPersistentBufferDDiskId()) {
                                allocatedPBuffers.push_back(
                                    node.GetPersistentBufferDDiskId());
                            }
                        }
                    }
                }
            }
            if (type ==
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::EventType)
            {
                auto* msg = ev->Get<
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
                if (msg->Record.DirectBlockGroupOperationsSize() > 0 &&
                    msg->Record.GetDirectBlockGroupOperations(0)
                        .HasDefineDirectBlockGroup() &&
                    msg->Record.GetDirectBlockGroupOperations(0)
                            .GetDefineDirectBlockGroup()
                            .GetNumDDisks() == 0)
                {
                    if (!pendingDeleteChunks.empty()) {
                        deallocateBeforeWipeDone = true;
                    }
                    ++deallocateRequestCount;
                    deallocateOpSize =
                        msg->Record.DirectBlockGroupOperationsSize();
                    for (const auto& op:
                         msg->Record.GetDirectBlockGroupOperations())
                    {
                        if (!op.HasDefineDirectBlockGroup()) {
                            deallocateOpMalformed = true;
                            continue;
                        }
                        const auto& def = op.GetDefineDirectBlockGroup();
                        if (def.GetNumDDisks() != 0 ||
                            def.GetNumChunksPerDDisk() != 0 ||
                            def.GetNumPersistentBuffers() != 0)
                        {
                            deallocateOpMalformed = true;
                        }
                    }
                }
            }
            if (captureWipeTraffic) {
                if (type == NDDisk::TEvErasePersistentBuffer::EventType) {
                    const auto& record =
                        ev->Get<NDDisk::TEvErasePersistentBuffer>()->Record;
                    // Partition wipe sends Max<ui64>(); background cleanup uses
                    // a finite watermark and must not be counted here.
                    if (record.GetLsn() == Max<ui64>()) {
                        pendingWipeBarriers.insert({ev->Sender, ev->Cookie});
                    }
                }
                if (type == NDDisk::TEvErasePersistentBufferResult::EventType) {
                    const TTransportCookie key{
                        ev->GetRecipientRewrite(),
                        ev->Cookie};
                    if (pendingWipeBarriers.contains(key)) {
                        const auto& record =
                            ev->Get<NDDisk::TEvErasePersistentBufferResult>()
                                ->Record;
                        if (record.GetStatus() ==
                            NKikimrBlobStorage::NDDisk::TReplyStatus::OK)
                        {
                            wipeBarrierOks.insert(key);
                        }
                        pendingWipeBarriers.erase(key);
                    }
                }
                if (type == NDDisk::TEvDeleteTabletChunks::EventType) {
                    if (!pendingWipeBarriers.empty()) {
                        deleteChunksBeforeWipeDone = true;
                    }
                    pendingDeleteChunks.insert({ev->Sender, ev->Cookie});
                }
                if (type == NDDisk::TEvDeleteTabletChunksResult::EventType) {
                    const TTransportCookie key{
                        ev->GetRecipientRewrite(),
                        ev->Cookie};
                    if (pendingDeleteChunks.contains(key)) {
                        const auto& record =
                            ev->Get<NDDisk::TEvDeleteTabletChunksResult>()
                                ->Record;
                        if (record.GetStatus() ==
                            NKikimrBlobStorage::NDDisk::TReplyStatus::OK)
                        {
                            deleteChunksOks.insert(key);
                        }
                        pendingDeleteChunks.erase(key);
                    }
                }
            }
            return true;
        };

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        auto loadActorAdapter =
            GetLoadActorAdapterActorId(env, partition, edge);
        const TString data = NUnitTest::RandomString(DefaultBlockSize, 42);
        WriteBlock(env, loadActorAdapter, edge, 0, data);

        UNIT_ASSERT(!allocatedDDisks.empty());
        UNIT_ASSERT(!allocatedPBuffers.empty());

        captureWipeTraffic = true;
        const auto error = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(1u, deallocateRequestCount);
        UNIT_ASSERT(!deallocateBeforeWipeDone);
        UNIT_ASSERT(!deleteChunksBeforeWipeDone);
        UNIT_ASSERT_VALUES_EQUAL(DirectBlockGroupsCount, deallocateOpSize);
        UNIT_ASSERT(!deallocateOpMalformed);

        UNIT_ASSERT(!TryGetLoadActorAdapterActorId(env, partition, edge));
        WriteBlockExpectFailure(
            env,
            loadActorAdapter,
            edge,
            0,
            NUnitTest::RandomString(DefaultBlockSize, 7));

        // Every unique allocated PBuffer got a Max-lsn barrier erase and
        // replied OK.
        UNIT_ASSERT_VALUES_EQUAL_C(
            UniqueDDiskCount(allocatedPBuffers),
            wipeBarrierOks.size(),
            "wipe barrier-erase OK replies vs unique allocated PBuffers");
        UNIT_ASSERT_C(
            pendingWipeBarriers.empty(),
            "unanswered wipe barrier-erase keys: "
                << pendingWipeBarriers.size());

        // Every unique allocated DDisk got DeleteTabletChunks and replied OK.
        UNIT_ASSERT_VALUES_EQUAL_C(
            UniqueDDiskCount(allocatedDDisks),
            deleteChunksOks.size(),
            "DeleteTabletChunks OK replies vs unique allocated DDisks");
        UNIT_ASSERT_C(
            pendingDeleteChunks.empty(),
            "unanswered DeleteTabletChunks keys: "
                << pendingDeleteChunks.size());

        // BSController must have released every previously allocated entity:
        // re-deleting any of them returns NOT_FOUND.
        {
            const auto& ddisk = allocatedDDisks.front();
            auto rr = SendBscDirectBlockGroupOperation(
                env,
                partition,
                /*directBlockGroupId=*/0,
                [&](auto* op)
                { op->AddDeleteDDisks()->MutableDDiskId()->CopyFrom(ddisk); });
            UNIT_ASSERT_VALUES_EQUAL_C(
                NKikimrProto::NOT_FOUND,
                rr.GetStatus(),
                rr.GetErrorReason());
        }
        {
            const auto& pbuffer = allocatedPBuffers.front();
            auto rr = SendBscDirectBlockGroupOperation(
                env,
                partition,
                /*directBlockGroupId=*/0,
                [&](auto* op)
                {
                    op->AddDeletePersistentBuffers()
                        ->MutablePersistentBufferId()
                        ->CopyFrom(pbuffer);
                });
            UNIT_ASSERT_VALUES_EQUAL_C(
                NKikimrProto::NOT_FOUND,
                rr.GetStatus(),
                rr.GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldDeletePartitionIdempotently)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);

        {
            const auto error = DeletePartition(env, partition, edge);
            UNIT_ASSERT_VALUES_EQUAL_C(0u, error.GetCode(), FormatError(error));
        }
        {
            // Second delete after teardown finished is a no-op.
            const auto error = DeletePartition(env, partition, edge);
            UNIT_ASSERT_VALUES_EQUAL_C(0u, error.GetCode(), FormatError(error));
        }

        UNIT_ASSERT(!TryGetLoadActorAdapterActorId(env, partition, edge));
    }

    Y_UNIT_TEST(ShouldFailDeleteWhenBscPipeBreaksDuringDeallocate)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);

        TActorId lastBscPipeClient;
        TActorId lastBscPipeOwner;
        bool injectPipeBreak = true;

        runtime->FilterFunction =
            [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
        {
            const ui32 type = ev->GetTypeRewrite();
            if (type == TEvTabletPipe::TEvClientConnected::EventType) {
                const auto* msg = ev->Get<TEvTabletPipe::TEvClientConnected>();
                if (msg->TabletId == MakeBSControllerID()) {
                    lastBscPipeClient = msg->ClientId;
                    lastBscPipeOwner = ev->GetRecipientRewrite();
                }
            }
            if (injectPipeBreak &&
                type == TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::
                            EventType)
            {
                auto* msg = ev->Get<
                    TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
                if (msg->Record.DirectBlockGroupOperationsSize() > 0 &&
                    msg->Record.GetDirectBlockGroupOperations(0)
                        .HasDefineDirectBlockGroup() &&
                    msg->Record.GetDirectBlockGroupOperations(0)
                            .GetDefineDirectBlockGroup()
                            .GetNumDDisks() == 0 &&
                    lastBscPipeClient && lastBscPipeOwner)
                {
                    injectPipeBreak = false;
                    // Drop the deallocate and fail the in-flight delete via
                    // the cleanup actor that owns the BSC pipe.
                    runtime->Schedule(
                        TDuration::MilliSeconds(1),
                        new IEventHandle(
                            lastBscPipeOwner,
                            lastBscPipeClient,
                            new TEvTabletPipe::TEvClientDestroyed(
                                MakeBSControllerID(),
                                lastBscPipeClient,
                                TActorId())),
                        nullptr,
                        nodeId);
                    return false;
                }
            }
            return true;
        };

        auto partition = CreatePartitionTablet(env);
        UNIT_ASSERT(lastBscPipeOwner);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        Y_UNUSED(GetLoadActorAdapterActorId(env, partition, edge));

        const auto error = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));

        // A retry after the pipe failure must still be able to finish.
        const auto error2 = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, error2.GetCode(), FormatError(error2));
    }

    Y_UNIT_TEST(ShouldFailDeleteWhenPBufferEraseIsOverloaded)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        bool injectOverload = true;
        THashSet<std::pair<TActorId, ui64>> wipeCookies;
        runtime->FilterFunction =
            [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev)
        {
            const ui32 type = ev->GetTypeRewrite();
            if (type == NDDisk::TEvErasePersistentBuffer::EventType) {
                const auto& record =
                    ev->Get<NDDisk::TEvErasePersistentBuffer>()->Record;
                if (record.GetLsn() == Max<ui64>()) {
                    wipeCookies.insert({ev->Sender, ev->Cookie});
                }
                return true;
            }
            if (!injectOverload ||
                type != NDDisk::TEvErasePersistentBufferResult::EventType)
            {
                return true;
            }
            const std::pair<TActorId, ui64> key{
                ev->GetRecipientRewrite(),
                ev->Cookie};
            if (!wipeCookies.contains(key)) {
                return true;
            }
            auto* msg = ev->Get<NDDisk::TEvErasePersistentBufferResult>();
            if (msg->Record.GetStatus() !=
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK)
            {
                return true;
            }
            injectOverload = false;
            msg->Record.SetStatus(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED);
            msg->Record.SetErrorReason("injected overload");
            return true;
        };

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        Y_UNUSED(GetLoadActorAdapterActorId(env, partition, edge));

        const auto error = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));

        const auto error2 = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, error2.GetCode(), FormatError(error2));
    }

    Y_UNIT_TEST(ShouldFailDeleteWhenDDiskDeleteChunksIsUndelivered)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        bool injectUndelivery = true;
        runtime->FilterFunction =
            [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
        {
            if (!injectUndelivery) {
                return true;
            }
            if (ev->GetTypeRewrite() !=
                NDDisk::TEvDeleteTabletChunks::EventType)
            {
                return true;
            }
            injectUndelivery = false;
            runtime->Send(
                new IEventHandle(
                    ev->Sender,
                    ev->GetRecipientRewrite(),
                    new NActors::TEvents::TEvUndelivered(
                        NDDisk::TEvDeleteTabletChunks::EventType,
                        NActors::TEvents::TEvUndelivered::Disconnected),
                    0,
                    ev->Cookie),
                nodeId);
            return false;
        };

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        Y_UNUSED(GetLoadActorAdapterActorId(env, partition, edge));

        const auto error = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));

        const auto error2 = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, error2.GetCode(), FormatError(error2));
    }

    Y_UNIT_TEST(ShouldReplyToConcurrentDeletePartitionRequests)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        auto partition = CreatePartitionTablet(env);

        const TActorId& edge1 = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        const TActorId& edge2 = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        Y_UNUSED(GetLoadActorAdapterActorId(env, partition, edge1));

        auto sendDelete = [&](const TActorId& edge)
        {
            auto request =
                std::make_unique<TEvService::TEvDeletePartitionRequest>();
            runtime->SendToPipe(
                partition,
                edge,
                request.release(),
                0,
                TTestActorSystem::GetPipeConfigWithRetries());
        };

        sendDelete(edge1);
        sendDelete(edge2);

        // Wait on both edges together so pipe ClientConnected on the second
        // edge is consumed instead of crashing an unattended edge actor.
        std::set<TActorId> ids{edge1, edge2};
        THashMap<TActorId, NProto::TError> errors;
        while (errors.size() < 2) {
            auto ev = runtime->WaitForEdgeActorEvent(ids);
            if (ev->GetTypeRewrite() !=
                TEvService::TEvDeletePartitionResponse::EventType)
            {
                continue;
            }
            errors[ev->GetRecipientRewrite()] =
                ev->Get<TEvService::TEvDeletePartitionResponse>()->GetError();
        }

        for (const auto& [actorId, error]: errors) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                0u,
                error.GetCode(),
                FormatError(error) << " from " << actorId);
        }
    }

    Y_UNIT_TEST(ShouldFailDeleteWhenWipeReplyIsLost)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NActors::NLog::PRI_DEBUG);

        bool dropResult = true;
        runtime->FilterFunction =
            [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev)
        {
            if (!dropResult) {
                return true;
            }
            if (ev->GetTypeRewrite() !=
                NDDisk::TEvDeleteTabletChunksResult::EventType)
            {
                return true;
            }
            dropResult = false;
            return false;
        };

        auto scopedService = SetupStorage(env, EWriteMode::DirectWrite);
        auto partition = CreatePartitionTablet(env);

        const TActorId& edge = runtime->AllocateEdgeActor(
            env.Settings.ControllerNodeId,
            __FILE__,
            __LINE__);
        Y_UNUSED(GetLoadActorAdapterActorId(env, partition, edge));

        const auto error = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TIMEOUT,
            error.GetCode(),
            FormatError(error));
        UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "timed out");

        const auto error2 = DeletePartition(env, partition, edge);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, error2.GetCode(), FormatError(error2));
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
