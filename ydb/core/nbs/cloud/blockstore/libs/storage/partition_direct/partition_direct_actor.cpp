#include "partition_direct_actor.h"

#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/region.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>

#include <util/system/fs.h>
#include <unistd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NActors;

namespace {

TString GetDDiskConnectionsFilePath(ui64 tabletId) {
    TString res = "/tmp/partition_actor_ddisk_connections";
    res += "_" + ToString(tabletId);
    res += "_" + ToString(getpid());
    res += ".tmp";
    return res;
}

TString SerializeTabletInfo(
    const TPartitionIds& ids,
    const NYdb::NBS::NProto::TStorageServiceConfig& storageConfig,
    const NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    ::NYdb::NBS::PartitionDirect::NProto::TTabletInfo tabletInfo;
    tabletInfo.MutableStorageConfig()->CopyFrom(storageConfig);
    tabletInfo.MutableVolumeConfig()->CopyFrom(volumeConfig);

    Y_ASSERT(ids.size() == TPartitionActor::NumDirectBlockGroups);
    auto* connectionsInfo = tabletInfo.MutableConnectionsInfo();

    for (const auto& id: ids) {
        auto* connectionInfo = connectionsInfo->Add();
        Y_ASSERT(id.DdiskIds.size() == id.PersistentBufferDDiskIds.size());

        auto* connections = connectionInfo->MutableConnections();
        for (size_t j = 0; j < id.DdiskIds.size(); ++j) {
            auto* connection = connections->Add();

            auto* ddiskId = connection->MutableDDiskId();
            id.DdiskIds[j].Serialize(ddiskId);

            auto* pbDDiskId = connection->MutablePersistentBufferDDiskId();
            id.PersistentBufferDDiskIds[j].Serialize(pbDDiskId);
        }
    }
    Y_ASSERT(
        tabletInfo.GetConnectionsInfo().size() ==
        TPartitionActor::NumDirectBlockGroups);

    TString serialized;
    if (!tabletInfo.SerializeToString(&serialized)) {
        Y_ABORT_S("Failed to serialize protobuf ddisk connections");
    }

    return serialized;
}

void DeserializeTabletInfo(
    const TString& serialized, TPartitionIds& ids,
    NYdb::NBS::NProto::TStorageServiceConfig& storageConfig,
    NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    ids.clear();
    ::NYdb::NBS::PartitionDirect::NProto::TTabletInfo tabletInfo;
    if (!tabletInfo.ParseFromString(serialized)) {
        Y_ABORT_S("Failed to deserialize protobuf ddisk connections");
    }

    storageConfig.CopyFrom(tabletInfo.GetStorageConfig());
    volumeConfig.CopyFrom(tabletInfo.GetVolumeConfig());

    Y_ASSERT(
        tabletInfo.GetConnectionsInfo().size() ==
        TPartitionActor::NumDirectBlockGroups);

    for (const auto& connectionInfo: tabletInfo.GetConnectionsInfo()) {
        ids.push_back({});
        auto& ddiskIds = ids.back().DdiskIds;
        auto& persistentBufferDDiskIds = ids.back().PersistentBufferDDiskIds;
        for (const auto& connection: connectionInfo.GetConnections()) {
            ddiskIds.emplace_back(connection.GetDDiskId());
            persistentBufferDDiskIds.emplace_back(
                connection.GetPersistentBufferDDiskId());
        }
    }
}

} // namespace

TPartitionActor::TPartitionActor(const TActorId& tablet,
                                 NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NKikimr::NMiniKQL::TMiniKQLFactory)
{
    LOG_INFO(NActors::TActivationContext::AsActorContext(),
             NKikimrServices::NBS_PARTITION,
             "TPartitionActor: initialization started");
}

void TPartitionActor::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TPartitionActor::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx) {
    Become(&TThis::StateWork);

    LOG_INFO(NActors::TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
        "Started NBS partition: actor id %s", SelfId().ToString().data());

    // Initialize StorageConfig from NBS service
    if (auto nbsService = GetNbsService()) {
        const auto& nbsConfig = nbsService->GetConfig();
        if (nbsConfig.has_nbsstorageconfig()) {
            StorageConfig.CopyFrom(nbsConfig.nbsstorageconfig());
            LOG_INFO(NActors::TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Initialized StorageConfig from NBS service config");
        }
    }

    if (HaveStoredTabletInfo()) {
        DdiskBlockGroupAllocated = true;
        LOG_INFO(NActors::TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "DDisks connection info has been found");

        TPartitionIds ids;
        LoadTabletInfo(ctx, ids);
        Start(ctx, std::move(ids));
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}

void TPartitionActor::ReportTabletState(const TActorContext& ctx)
{
    auto service = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        STATE_WORK);

    NYdb::NBS::Send(ctx, service, std::move(request));
}

void TPartitionActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Pipe client %s server %s connected to volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Pipe client %s server %s disconnected from volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Pipe client %s server %s got destroyed for volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

////////////////////////////////////////////////////////////////////////////////
void TPartitionActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev) {
    StateInitImpl(ev, SelfId());
}

bool TPartitionActor::HaveStoredTabletInfo()
{
    return NFs::Exists(GetDDiskConnectionsFilePath(TabletID()));
}

TVector<IDirectBlockGroupPtr> TPartitionActor::CreateDirectBlockGroups(
    TPartitionIds ids)
{
    Y_ASSERT(ids.size() == NumDirectBlockGroups);
    TVector<IDirectBlockGroupPtr> directBlockGroups;

    for (size_t i = 0; i < NumDirectBlockGroups; i++) {
        auto ddiskIds = std::move(ids[i].DdiskIds);
        auto persistentBufferDDiskIds = std::move(ids[i].PersistentBufferDDiskIds);

        directBlockGroups.emplace_back(
            std::make_shared<TDirectBlockGroup>(
                TActivationContext::ActorSystem(),
                TabletID(),
                1,   // generation
                i,   // directBlockGroupIndex
                std::move(ddiskIds), std::move(persistentBufferDDiskIds),
                VolumeConfig.GetBlockSize(),
                VolumeConfig.GetPartitions(0).GetBlockCount(),
                3   // syncRequestsBatchSize
                ));
        directBlockGroups[i]->EstablishConnections({});
    }

    return directBlockGroups;
}

void TPartitionActor::LoadTabletInfo(
    const NActors::TActorContext& ctx,
    TPartitionIds &ids)
{
    LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
             "Trying to restore DDisks Ids");

    TFileInput input(GetDDiskConnectionsFilePath(TabletID()));
    TString serialized = input.ReadAll();

    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
        "TabletInfo serialized size on load: " << serialized.size());

    DeserializeTabletInfo(serialized, ids, StorageConfig, VolumeConfig);

    LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
             "Restored %d DDisks Ids", ids[0].DdiskIds.size());
}

// TODO заменить на работу с локальной базой таблетки
void TPartitionActor::StoreTabletInfo(
    const NActors::TActorContext& ctx,
    const TPartitionIds& ids)
{
    Y_UNUSED(ctx);

    LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
        "Trying to Store %d DDisks Ids", ids[0].DdiskIds.size());

    TString serialized = SerializeTabletInfo(ids, StorageConfig, VolumeConfig);
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
        "TabletInfo serialized size on store: " << serialized.size());

    TFileOutput output(GetDDiskConnectionsFilePath(TabletID()));
    output.Write(serialized);


    // TODO вынести в юнит тест
    {
        LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
                 "Trying to test store/load code");
        TPartitionIds helpIds;
        DeserializeTabletInfo(serialized, helpIds, StorageConfig, VolumeConfig);

        Y_ASSERT(helpIds.size() == ids.size());
        for (size_t i = 0; i < helpIds.size(); ++i) {
            const auto& d1 = helpIds[i].DdiskIds;
            const auto& d2 = ids[i].DdiskIds;
            Y_ASSERT(d1.size() == d2.size());
            for (size_t j = 0; j < d1.size(); ++j) {
                Y_ASSERT((d1[j] <=> d2[j]) == 0);
            }

            const auto& pb1 = helpIds[i].PersistentBufferDDiskIds;
            const auto& pb2 = ids[i].PersistentBufferDDiskIds;
            Y_ASSERT(pb1.size() == pb2.size());
            for (size_t j = 0; j < pb1.size(); ++j) {
                Y_ASSERT((pb1[j] <=> pb2[j]) == 0);
            }
        }
        LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
                 "test store/load code was successful");
    }
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::CreateBSControllerPipeClient(const NActors::TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig.GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig.GetPersistentBufferDDiskPoolName());

    // TODO: fill with tablet id
    request->Record.SetTabletId(TabletID());

    for (size_t i = 0; i < 32; i++) {
        auto* query = request->Record.AddQueries();
        query->SetDirectBlockGroupId(i);

        // TODO: fill with target num v chunks. vchunk is 128MB. let us use 1 vchunk
        // since disk size will be 128MB.
        query->SetTargetNumVChunks(1);
    }

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TPartitionActor::Start(
    const NActors::TActorContext& ctx,
    TPartitionIds ids)
{
    LOG_INFO(NActors::TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "starting partition_direct");

    TVector<IDirectBlockGroupPtr> directBlockGroups = CreateDirectBlockGroups(std::move(ids));
    auto region = std::make_shared<TRegion>(std::move(directBlockGroups));
    auto fastPathService = std::make_shared<TFastPathService>(
        TActivationContext::ActorSystem(),
        TabletID(),
        1,   // generation
        std::move(region), StorageConfig, AppData()->Counters);

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, fastPathService);

    {
        auto service = GetNbsService();

        TString diskId = VolumeConfig.GetDiskId();
        ui32 blockSize = VolumeConfig.GetBlockSize();
        ui64 blockCount = 0;
        for (const auto& p: VolumeConfig.GetPartitions()) {
            blockCount += p.GetBlockCount();
        }

        TString socketPath = "/tmp/" + diskId + ".sock";
        NVhost::TStorageOptions options{.DiskId = diskId,
                                        .ClientId = "client-1",
                                        .BlockSize = blockSize,
                                        .BlocksCount = blockCount,
                                        .VhostQueuesCount = 1};
        service->VhostServer->StartEndpoint(std::move(socketPath),
                                            fastPathService, options);
    }

    LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
             "Started NBS partition LoadActorAdapter: actor id %s",
             LoadActorAdapter.ToString().data());
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        msg->Record.DebugString().data());

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(msg->Record.GetResponses().size() == NumDirectBlockGroups);

        TPartitionIds ids;
        ids.resize(NumDirectBlockGroups);
        for (size_t i = 0; i < NumDirectBlockGroups; i++) {
            const auto& response = msg->Record.GetResponses()[i];
            Y_ABORT_UNLESS(response.GetDirectBlockGroupId() == i);
            Y_ABORT_UNLESS(response.GetActualNumVChunks() == 1);

            TVector<NBsController::TDDiskId>& ddiskIds = ids[i].DdiskIds;
            TVector<NBsController::TDDiskId>& persistentBufferDDiskIds =
                ids[i].PersistentBufferDDiskIds;
            for (const auto& node: response.GetNodes()) {
                ddiskIds.emplace_back(node.GetDDiskId());
                persistentBufferDDiskIds.emplace_back(
                    node.GetPersistentBufferDDiskId());
            }
        }

        DdiskBlockGroupAllocated = true;
        StoreTabletInfo(ctx, ids);
        Start(ctx, std::move(ids));
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
}

void TPartitionActor::HandleGetLoadActorAdapterActorId(
    const TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdResponse>();
    response->ActorId = LoadActorAdapter.ToString();
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Handle UpdateVolumeConfig request"
                << ", tabletId: " << TabletID()
                << ", txId: " << msg->Record.GetTxId()
                << ", sender: " << ev->Sender
                << ", version: " << msg->Record.GetVolumeConfig().GetVersion());

    if (DdiskBlockGroupAllocated) {
        LOG_ERROR_S(TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "PartitionDirectActor already has ddisk connections");

        auto response = std::make_unique<
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
        response->Record.SetStatus(NKikimrBlockStore::ERROR);
        ctx.Send(ev->Sender, response.release());
        return;
    }

    // Store DDiskPoolName from StoragePoolName
    if (msg->Record.GetVolumeConfig().HasStoragePoolName()) {
        const TString& storagePoolName = msg->Record.GetVolumeConfig().GetStoragePoolName();
        StorageConfig.SetDDiskPoolName(storagePoolName);
        StorageConfig.SetPersistentBufferDDiskPoolName(storagePoolName);

        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                    "Updated DDiskPoolName from StoragePoolName"
                    << ", tabletId: " << TabletID()
                    << ", storagePoolName: " << storagePoolName);
    }

    // Store volume config
    VolumeConfig.CopyFrom(msg->Record.GetVolumeConfig());

    // Send response back to volume
    auto response = std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(msg->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Sending UpdateVolumeConfig response"
                << ", tabletId: " << TabletID()
                << ", txId: " << response->Record.GetTxId()
                << ", status: OK");

    ctx.Send(ev->Sender, response.release());

    // TODO: make separate state
    AllocateDDiskBlockGroup(ctx);
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleControllerAllocateDDiskBlockGroupResult);
        HFunc(
            TEvService::TEvGetLoadActorAdapterActorIdRequest,
            HandleGetLoadActorAdapterActorId);
        HFunc(NKikimr::TEvBlockStore::TEvUpdateVolumeConfig, HandleUpdateVolumeConfig);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                        << " event: " << ev->ToString());
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
