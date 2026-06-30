#include "partition_direct_actor.h"

#include "direct_block_group_impl.h"
#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/mon.h>

#include <util/system/fs.h>

#include <unistd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NActors;

TPartitionActor::TPartitionActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletBase<TPartitionActor>(
          tablet,
          NKikimr::TTabletStorageInfoPtr(info),
          nullptr)
    , LogTitle{GetCycleCount(), TLogTitle::TPartitionDirect{.TabletId = TabletID()}}
    , StorageConfig(GetNbsService()->StorageConfig)
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s TPartitionActor: initialization started",
        LogTitle.GetWithTime().c_str());
}

TPartitionActor::~TPartitionActor() = default;

void TPartitionActor::PassAway()
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "TPartitionActor: before detach");
}

void TPartitionActor::OnDetach(const TActorContext& ctx)
{
    Die(ctx);
}

void TPartitionActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Started NBS partition: actor id %s",
        LogTitle.GetWithTime().c_str(),
        SelfId().ToString().data());

    if (!Executor()->GetStats().IsFollower()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Executing InitSchema transaction",
            LogTitle.GetWithTime().c_str());
        ExecuteTx(ctx, CreateTx<TInitSchema>());
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
}

void TPartitionActor::ReportTabletState(const TActorContext& ctx)
{
    auto service =
        NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<
        NNodeWhiteboard::TEvWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
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
        "%s Pipe client %s server %s connected to volume",
        LogTitle.GetWithTime().c_str(),
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
        "%s Pipe client %s server %s disconnected from volume",
        LogTitle.GetWithTime().c_str(),
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
        "%s Pipe client %s server %s got destroyed for volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev)
{
    StateInitImpl(ev, SelfId());
}

TVector<IDirectBlockGroupPtr> TPartitionActor::CreateDirectBlockGroups(
    TDirectBlockGroupsConnections directBlockGroupsConnections)
{
    const auto nbsService = GetNbsService();
    TVector<IDirectBlockGroupPtr> directBlockGroups;
    auto executors =
        nbsService->ExecutorPool.GetExecutors(DirectBlockGroupsCount);

    for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
        const auto& conn =
            directBlockGroupsConnections.GetDirectBlockGroupConnections(i);
        TVector<NBsController::TDDiskId> ddiskIds;
        for (const auto& connection: conn.GetConnections()) {
            ddiskIds.push_back(
                NBsController::TDDiskId(connection.GetDDiskId()));
        }
        TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
        for (const auto& connection: conn.GetConnections()) {
            persistentBufferDDiskIds.push_back(NBsController::TDDiskId(
                connection.GetPersistentBufferDDiskId()));
        }

        auto directBlockGroup = std::make_shared<TDirectBlockGroup>(
            TActivationContext::ActorSystem(),
            nbsService->StorageConfig,
            executors[i],
            VolumeConfig.GetDiskId(),
            TabletID(),
            Executor()->Generation(),   // generation
            i,                          // direct block group index
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            std::make_unique<NTransport::TICStorageTransport>(
                TActivationContext::ActorSystem()));

        directBlockGroups.emplace_back(std::move(directBlockGroup));
    }

    return directBlockGroups;
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::CreateBSControllerPipeClient(
    const NActors::TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig->GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig->GetPersistentBufferDDiskPoolName());

    // TODO: fill with tablet id
    request->Record.SetTabletId(TabletID());

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionsCount =
        AlignUp(blockCount * VolumeConfig.GetBlockSize(), RegionSize) /
        RegionSize;

    for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
        auto* query = request->Record.AddQueries();
        query->SetDirectBlockGroupId(i);
        query->SetTargetNumVChunks(regionsCount);
    }

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TPartitionActor::Start(
    const NActors::TActorContext& ctx,
    TDirectBlockGroupsConnections directBlockGroupsConnections,
    TVector<TVChunkConfig> vChunkConfigs)
{
    LogTitle.SetDiskId(VolumeConfig.GetDiskId());
    LogTitle.SetGeneration(Executor()->Generation());

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Starting",
        LogTitle.GetWithTime().c_str());

    auto nbsService = GetNbsService();
    Y_ABORT_UNLESS(nbsService);
    Y_ABORT_UNLESS(nbsService->Scheduler);
    Y_ABORT_UNLESS(nbsService->Timer);

    TVChunkConfigByIndex vChunkConfigsByIndex;
    vChunkConfigsByIndex.reserve(vChunkConfigs.size());
    for (const auto& cfg: vChunkConfigs) {
        vChunkConfigsByIndex[cfg.GetVChunkIndex()] = cfg;
    }

    DirectBlockGroupsConnections = directBlockGroupsConnections;

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    FastPathService = std::make_shared<TFastPathService>(
        TActivationContext::ActorSystem(),
        SelfId(),
        TabletID(),
        VolumeConfig.GetDiskId(),
        blockCount,
        VolumeConfig.GetBlockSize(),
        CreateDirectBlockGroups(std::move(directBlockGroupsConnections)),
        std::move(vChunkConfigsByIndex),
        StorageConfig,
        nbsService->Scheduler,
        nbsService->Timer,
        AppData()->Counters);

    // Synchronous start mode - requests pass as the initial quorum of Locked
    // DDisk sessions across all DBGs is achieved.
    // TODO: make optional via StorageConfig after implementation of async mode.
    FastPathService->Run().Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = SelfId()]   //
        (const NThreading::TFuture<void>&) mutable
        {
            // This callback runs OUTSIDE the actor thread - on the DBG's
            // executor-thread
            auto event = std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceReady>();
            actorSystem->Send(selfId, event.release());
        });
}

void TPartitionActor::HandleFastPathServiceReady(
    const TEvPartitionDirectPrivate::TEvFastPathServiceReady::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s All DBGs reached initial locked quorum, opening endpoint",
        LogTitle.GetWithTime().c_str());

    // Re-send the BSC request for an add-host in flight at the last restart
    // (no live add can be in flight this early). BSController is idempotent.
    if (AddHostInFlight.has_value()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Replaying in-flight AddHost dbgId=%lu newHostIndex=%u",
            LogTitle.GetWithTime().c_str(),
            AddHostInFlight->DirectBlockGroupId,
            static_cast<ui32>(AddHostInFlight->NewHostIndex));
        SendAllocateDDiskForAddHost(
            ctx,
            AddHostInFlight->DirectBlockGroupId,
            AddHostInFlight->NewHostIndex);
    }

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, FastPathService);

    {
        auto service = GetNbsService();

        const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
        TString socketPath = "/tmp/" + VolumeConfig.GetDiskId() + ".sock";
        NVhost::TStorageOptions options{
            .DiskId = VolumeConfig.GetDiskId(),
            .ClientId = "client-1",
            .BlockSize = VolumeConfig.GetBlockSize(),
            .StripeSize = StorageConfig->GetStripeSize(),
            .BlocksCount = blockCount,
            .VChunkSize = StorageConfig->GetVChunkSize(),
            .VhostQueuesCount = StorageConfig->GetVhostQueuesCount()};
        service->VhostServer->StartEndpoint(
            std::move(socketPath),
            FastPathService,
            FastPathService,
            options);
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Started NBS LoadActorAdapter: %s",
        LogTitle.GetWithTime().c_str(),
        LoadActorAdapter.ToString().c_str());
}

void TPartitionActor::HandleFastPathServiceShutdown(
    const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!FastPathService) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s FastPathService is not started",
            LogTitle.GetWithTime().c_str());
        Send(
            ctx.SelfID,
            std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceStopped>(),
            0,   //   flags
            ev->Cookie);

        Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceStopped>());

        return;
    }

    auto onStop = FastPathService->Stop();
    onStop.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         recipient = ev->Sender,
         cookie = ev->Cookie]   //
        (const NThreading::TFuture<void>& f)
        {
            Y_UNUSED(f);
            {
                auto event = std::make_unique<
                    TEvPartitionDirectPrivate::TEvFastPathServiceStopped>();
                actorSystem->Send(
                    selfId,
                    event.release(),
                    0,   // flags
                    cookie);
            }
            {
                auto event = std::make_unique<
                    TEvPartitionDirectPrivate::TEvFastPathServiceStopped>();
                actorSystem->Send(
                    recipient,
                    event.release(),
                    0,   // flags
                    cookie);
            }
        });
}

void TPartitionActor::HandleFastPathServiceStopped(
    const TEvPartitionDirectPrivate::TEvFastPathServiceStopped::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s FastPathService stopped",
        LogTitle.GetWithTime().c_str());
}

NProto::TError TPartitionActor::ValidateAddHostAllocation(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& msg,
    size_t dbgId) const
{
    const auto& record = msg.Record;

    // A genuine BSController failure (could not allocate: capacity, fault
    // domains). Retriable - the add is kept and retried on recovery, never
    // aborted (an abort would crash-loop on replay).
    if (record.GetStatus() != NKikimrProto::EReplyStatus::OK) {
        return MakeError(
            E_REJECTED,
            TStringBuilder()
                << "BSController error: " << record.GetErrorReason());
    }

    // Structural invariant of a successful response (needed to read the
    // per-group outcome below): a violation aborts, it is not a runtime
    // failure.
    Y_ABORT_UNLESS(
        record.DirectBlockGroupsSize() == 1,
        "BSController returned %d DirectBlockGroups, expected 1",
        record.DirectBlockGroupsSize());

    const auto& group = record.GetDirectBlockGroups(0);
    Y_ABORT_UNLESS(
        group.GetDirectBlockGroupId() == dbgId,
        "BSController response is for a different DBG");

    // A per-group BSController failure - also retriable.
    if (group.GetError()) {
        return MakeError(
            E_REJECTED,
            "BSController reported an error for this DirectBlockGroup");
    }

    return {};
}

void TPartitionActor::ExtractAddHostDDisks(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& msg,
    size_t dbgId,
    ui32 expectedCurrent,
    NKikimrBlobStorage::NDDisk::TDDiskId& newDDiskId,
    NKikimrBlobStorage::NDDisk::TDDiskId& newPBufferId) const
{
    // Called only for a granted response (ValidateAddHostAllocation passed),
    // so its shape is a structural invariant - any mismatch aborts.
    const auto& group = msg.Record.GetDirectBlockGroups(0);
    Y_ABORT_UNLESS(
        static_cast<ui32>(group.DDiskIdSize()) == expectedCurrent + 1 &&
            static_cast<ui32>(group.PersistentBufferDDiskIdSize()) ==
                expectedCurrent + 1,
        "BSController returned %d ddisks / %d pbuffers, expected %u",
        group.DDiskIdSize(),
        group.PersistentBufferDDiskIdSize(),
        expectedCurrent + 1);

    newDDiskId = group.GetDDiskId(expectedCurrent);
    newPBufferId = group.GetPersistentBufferDDiskId(expectedCurrent);

    const TString newDDiskIdBytes = newDDiskId.SerializeAsString();
    const TString newPBufferIdBytes = newPBufferId.SerializeAsString();
    for (const auto& conn:
         DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId)
             .GetConnections())
    {
        Y_ABORT_UNLESS(
            conn.GetDDiskId().SerializeAsString() != newDDiskIdBytes &&
                conn.GetPersistentBufferDDiskId().SerializeAsString() !=
                    newPBufferIdBytes,
            "BSController returned a DDisk/PBuffer already in this DBG");
    }
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.DebugString().data());

    if (DdiskBlockGroupAllocated) {
        const size_t dbgId = ev->Cookie;
        if (!AddHostInFlight.has_value() ||
            AddHostInFlight->DirectBlockGroupId != dbgId)
        {
            LOG_WARN(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s AddHost response for unexpected dbgId=%lu (stale)",
                LogTitle.GetWithTime().c_str(),
                dbgId);
            return;
        }

        const ui32 expectedCurrent = AddHostInFlight->NewHostIndex;
        const auto newHostIndex = AddHostInFlight->NewHostIndex;
        NTabletPipe::CloseClient(ctx, AddHostInFlight->BSPipeClient);

        if (auto error = ValidateAddHostAllocation(*msg, dbgId);
            HasError(error))
        {
            // Not cancelled: the intent stays persisted, so it is retried on
            // the next recovery until BSController grants the DDisk.
            LOG_WARN(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s AddHost (dbgId=%lu) not completed, kept for retry on "
                "recovery: %s",
                LogTitle.GetWithTime().c_str(),
                dbgId,
                error.GetMessage().c_str());
            return;
        }

        NKikimrBlobStorage::NDDisk::TDDiskId newDDiskId;
        NKikimrBlobStorage::NDDisk::TDDiskId newPBufferId;
        ExtractAddHostDDisks(
            *msg,
            dbgId,
            expectedCurrent,
            newDDiskId,
            newPBufferId);

        TDirectBlockGroupsConnections updated = DirectBlockGroupsConnections;
        auto* dbgConn = updated.MutableDirectBlockGroupConnections(dbgId);
        auto* connection = dbgConn->AddConnections();
        connection->MutableDDiskId()->CopyFrom(newDDiskId);
        connection->MutablePersistentBufferDDiskId()->CopyFrom(newPBufferId);
        DirectBlockGroupsConnections = updated;

        ExecuteTx(
            ctx,
            CreateTx<TAddHostToDBG>(
                std::move(updated),
                dbgId,
                newHostIndex,
                std::move(newDDiskId),
                std::move(newPBufferId)));
        return;
    }

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(
            msg->Record.GetResponses().size() == DirectBlockGroupsCount);

        TDirectBlockGroupsConnections ids;
        for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
            auto* directBlockGroupConnections =
                ids.AddDirectBlockGroupConnections();
            const auto& response = msg->Record.GetResponses()[i];
            for (const auto& node: response.GetNodes()) {
                auto* connection =
                    directBlockGroupConnections->AddConnections();
                connection->MutableDDiskId()->CopyFrom(node.GetDDiskId());
                connection->MutablePersistentBufferDDiskId()->CopyFrom(
                    node.GetPersistentBufferDDiskId());
            }
        }

        DdiskBlockGroupAllocated = true;
        ExecuteTx(ctx, CreateTx<TStorePartitionIds>(std::move(ids)));
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
            LogTitle.GetWithTime().c_str(),
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
}

void TPartitionActor::RejectAddHost(
    const NActors::TActorContext& ctx,
    size_t dbgId,
    const TString& message)
{
    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s AddHost failed (dbgId=%lu): %s",
        LogTitle.GetWithTime().c_str(),
        dbgId,
        message.c_str());

    // Notify the DBG that asked for the host. Skipped for an out-of-range dbgId
    // - there is no such DBG to notify.
    const auto& directBlockGroups = FastPathService->GetDirectBlockGroups();
    if (dbgId >= directBlockGroups.size()) {
        return;
    }
    auto dbgPtr = directBlockGroups[dbgId];
    auto executor = dbgPtr->GetExecutor();
    executor->ExecuteSimple([dbgPtr, message]()
                            { dbgPtr->OnAddHostFailed(message); });
}

bool TPartitionActor::ValidateAddHostToDBGRequest(
    const TActorContext& ctx,
    size_t dbgId)
{
    if (AddHostInFlight.has_value()) {
        RejectAddHost(ctx, dbgId, "Another AddHost is already in progress");
        return false;
    }

    if (dbgId >=
        static_cast<size_t>(
            DirectBlockGroupsConnections.DirectBlockGroupConnectionsSize()))
    {
        RejectAddHost(
            ctx,
            dbgId,
            TStringBuilder() << "DirectBlockGroupId out of range: " << dbgId);
        return false;
    }

    // Authoritative AddHost gate: reads the persisted connection count under
    // the single-in-flight guard above, so it cannot overshoot MaxHostCount or
    // race a concurrent add. The DBG's own DDiskConnections lags, so it cannot
    // gate.
    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
    const auto currentSize = static_cast<ui32>(dbgConn.GetConnections().size());

    if (currentSize >= MaxHostCount) {
        RejectAddHost(
            ctx,
            dbgId,
            TStringBuilder() << "MaxHostCount=" << MaxHostCount << " reached");
        return false;
    }
    if (currentSize == 0) {
        RejectAddHost(ctx, dbgId, "AddHost on an empty DBG is not supported");
        return false;
    }

    return true;
}

void TPartitionActor::HandleAddHostToDBG(
    const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto dbgId = msg->DirectBlockGroupId;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle AddHostToDBG dbgId=%lu",
        LogTitle.GetWithTime().c_str(),
        dbgId);

    // TEvAddHostToDBG is only sent by a running FastPathService, so it (and the
    // allocated DBGs) is alive by the time we handle the request.
    Y_ABORT_UNLESS(FastPathService);

    if (!ValidateAddHostToDBGRequest(ctx, dbgId)) {
        return;
    }

    const auto& dbgConn =
        DirectBlockGroupsConnections.GetDirectBlockGroupConnections(dbgId);
    const auto currentSize = static_cast<ui32>(dbgConn.GetConnections().size());

    // Persist the intent before the BSController request (sent from the tx's
    // completion). A crash after the DDisk is allocated but before the
    // connection is persisted then leaves a durable intent, replayed on
    // restart.
    AddHostInFlight = TAddHostInFlight{
        .DirectBlockGroupId = dbgId,
        .NewHostIndex = static_cast<THostIndex>(currentSize),
    };

    ExecuteTx(
        ctx,
        CreateTx<TStartAddHost>(dbgId, static_cast<THostIndex>(currentSize)));
}

void TPartitionActor::SendAllocateDDiskForAddHost(
    const TActorContext& ctx,
    size_t dbgId,
    THostIndex newHostIndex)
{
    Y_ABORT_UNLESS(AddHostInFlight.has_value());

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionsCount =
        AlignUp(blockCount * VolumeConfig.GetBlockSize(), RegionSize) /
        RegionSize;

    const auto pipe = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
    AddHostInFlight->BSPipeClient = pipe;

    // Idempotent: NumDDisks=N+1 is the desired final state, not "add one"; a
    // re-sent request returns the same DDisk from BSController's persisted
    // allocation, so a retry (e.g. after a restart) is safe.
    const ui32 numDDisks = static_cast<ui32>(newHostIndex) + 1;
    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig->GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig->GetPersistentBufferDDiskPoolName());
    request->Record.SetTabletId(TabletID());

    auto* op = request->Record.AddDirectBlockGroupOperations();
    op->SetDirectBlockGroupId(dbgId);
    auto* define = op->MutableDefineDirectBlockGroup();
    define->SetNumDDisks(numDDisks);
    define->SetNumChunksPerDDisk(regionsCount);
    define->SetNumPersistentBuffers(numDDisks);

    NTabletPipe::SendData(ctx, pipe, request.release(), dbgId);
}

void TPartitionActor::HandleGetLoadActorAdapterActorId(
    const TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdResponse>();
    response->Record.SetActorId(LoadActorAdapter.ToString());
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request. Version: %d",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetVolumeConfig().GetVersion());

    if (DdiskBlockGroupAllocated) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Already has ddisk connections",
            LogTitle.GetWithTime().c_str());

        auto response = std::make_unique<
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
        response->Record.SetStatus(NKikimrBlockStore::ERROR);
        ctx.Send(ev->Sender, response.release());
        return;
    }

    const auto& volumeConfig = msg->Record.GetVolumeConfig();
    Y_ABORT_UNLESS(volumeConfig.PartitionsSize() == 1);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request VolumeConfig: %s",
        LogTitle.GetWithTime().c_str(),
        volumeConfig.DebugString().c_str());

    ExecuteTx(ctx, CreateTx<TStoreVolumeConfig>(volumeConfig));

    // Send response back to volume
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(msg->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    LOG_INFO(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Sending UpdateVolumeConfig response OK",
        LogTitle.GetWithTime().c_str());

    ctx.Send(ev->Sender, response.release());
}

void TPartitionActor::HandleUpdateVChunkConfig(
    const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& cfg = ev->Get()->VChunkConfig;

    LOG_DEBUG_S(
        ctx,
        NKikimrServices::NBS_PARTITION,
        LogTitle.GetWithTime().c_str()
            << " Handle UpdateVChunkConfig, vChunkIndex: "
            << cfg.GetVChunkIndex());

    ExecuteTx(ctx, CreateTx<TUpdateVChunkConfig>(std::move(cfg)));
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Processing event: %s from sender: %lu",
        LogTitle.GetWithTime().c_str(),
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
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateVChunkConfig,
            HandleUpdateVChunkConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceReady,
            HandleFastPathServiceReady);
        HFunc(TEvPartitionDirectPrivate::TEvAddHostToDBG, HandleAddHostToDBG);

        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceShutdown,
            HandleFastPathServiceShutdown);

        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceStopped,
            HandleFastPathServiceStopped);

        HFunc(NMon::TEvRemoteHttpInfo, HandleHttpInfo);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                                             << " event: " << ev->ToString());
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
