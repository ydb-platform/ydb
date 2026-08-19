#include "partition_cleanup_actor.h"

#include "partition_direct_events_private.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/protos/base.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/datetime.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError CheckDeallocateResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& msg)
{
    const auto& record = msg.Record;

    if (record.GetStatus() != NKikimrProto::OK) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "BSController deallocate failed: "
                             << record.GetErrorReason());
    }

    for (const auto& group: record.GetDirectBlockGroups()) {
        if (group.DDiskIdSize() != 0 ||
            group.PersistentBufferDDiskIdSize() != 0)
        {
            return MakeError(
                E_FAIL,
                TStringBuilder()
                    << "BSController deallocate left resources for dbgId="
                    << group.GetDirectBlockGroupId()
                    << " ddisks=" << group.DDiskIdSize()
                    << " pbuffers=" << group.PersistentBufferDDiskIdSize());
        }
    }

    return {};
}

NActors::TActorId MakePBufferServiceId(
    const NKikimrBlobStorage::NDDisk::TDDiskId& id)
{
    return MakeBlobStoragePersistentBufferId(
        id.GetNodeId(),
        id.GetPDiskId(),
        id.GetDDiskSlotId());
}

NActors::TActorId MakeDDiskServiceId(
    const NKikimrBlobStorage::NDDisk::TDDiskId& id)
{
    return MakeBlobStorageDDiskId(
        id.GetNodeId(),
        id.GetPDiskId(),
        id.GetDDiskSlotId());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TPartitionCleanupActor: public TActorBootstrapped<TPartitionCleanupActor>
{
    enum class ECleanupTarget
    {
        PBuffer,
        DDisk,
    };

    enum class ECleanupPhase
    {
        WipePBuffer,
        WipeDDisk,
    };

    struct TRequest
    {
        NActors::TActorId Target;
        ui32 NodeId = 0;
        ECleanupTarget Kind = ECleanupTarget::PBuffer;
    };

private:
    const TPartitionCleanupParams Params;
    TLogTitle LogTitle;

    THashMap<ui64, TRequest> InFlight;
    ui64 NextCookie = 1;
    ECleanupPhase Phase = ECleanupPhase::WipePBuffer;
    bool Completed = false;

    NActors::TActorId BSControllerPipeClient;

public:
    explicit TPartitionCleanupActor(TPartitionCleanupParams params)
        : Params(std::move(params))
        , LogTitle(
              GetCycleCount(),
              TLogTitle::TPartitionDirect{
                  .DiskId = Params.DiskId,
                  .TabletId = Params.TabletId,
                  .Generation = Params.Generation})
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWipe);

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Become StateWipe",
            LogTitle.GetWithTime().c_str());

        ctx.Schedule(PartitionCleanupTimeout, new TEvents::TEvWakeup());
        StartPBufferBarrierErase(ctx);
    }

private:
    STFUNC(StateWipe)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                NDDisk::TEvErasePersistentBufferResult,
                HandleErasePersistentBufferResult);
            HFunc(
                NDDisk::TEvDeleteTabletChunksResult,
                HandleDeleteTabletChunksResult);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(TEvents::TEvWakeup, HandleTimeout);
            HFunc(TEvInterconnect::TEvNodeDisconnected, HandleNodeDisconnected);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_ERROR(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "%s Unhandled event type: %u event %s",
                    LogTitle.GetWithTime().c_str(),
                    ev->GetTypeRewrite(),
                    ev->ToString().c_str());
                break;
        }
    }

    STFUNC(StateDeallocate)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
                HandleDeallocateResult);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
            HFunc(TEvents::TEvWakeup, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_ERROR(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "%s Unhandled event type: %u event %s",
                    LogTitle.GetWithTime().c_str(),
                    ev->GetTypeRewrite(),
                    ev->ToString().c_str());
                break;
        }
    }

    void StartPBufferBarrierErase(const TActorContext& ctx)
    {
        Phase = ECleanupPhase::WipePBuffer;

        const auto creds = NDDisk::TQueryCredentials::ForInternal(
            Params.TabletId,
            Params.Generation,
            std::nullopt,
            0);

        THashSet<TActorId> targets;
        for (const auto& group:
             Params.Connections.GetDirectBlockGroupConnections())
        {
            for (const auto& connection: group.GetConnections()) {
                const auto target = MakePBufferServiceId(
                    connection.GetPersistentBufferDDiskId());
                if (!targets.insert(target).second) {
                    continue;
                }
                SendWipeRequest(
                    ctx,
                    target,
                    ECleanupTarget::PBuffer,
                    std::make_unique<NDDisk::TEvErasePersistentBuffer>(
                        creds,
                        Max<ui64>()));
            }
        }

        if (InFlight.empty()) {
            StartDDiskChunkDelete(ctx);
        }
    }

    void StartDDiskChunkDelete(const TActorContext& ctx)
    {
        Phase = ECleanupPhase::WipeDDisk;

        const auto creds = NDDisk::TQueryCredentials::ForInternal(
            Params.TabletId,
            Params.Generation,
            std::nullopt,
            0);

        THashSet<TActorId> targets;
        for (const auto& group:
             Params.Connections.GetDirectBlockGroupConnections())
        {
            for (const auto& connection: group.GetConnections()) {
                const auto target = MakeDDiskServiceId(connection.GetDDiskId());
                if (!targets.insert(target).second) {
                    continue;
                }
                SendWipeRequest(
                    ctx,
                    target,
                    ECleanupTarget::DDisk,
                    std::make_unique<NDDisk::TEvDeleteTabletChunks>(creds));
            }
        }

        if (InFlight.empty()) {
            StartDeallocate(ctx);
        }
    }

    void SendWipeRequest(
        const TActorContext& ctx,
        const TActorId& target,
        ECleanupTarget kind,
        std::unique_ptr<IEventBase> event)
    {
        const ui64 cookie = NextCookie++;
        InFlight[cookie] =
            TRequest{.Target = target, .NodeId = target.NodeId(), .Kind = kind};

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Send wipe request kind=%s target=%s cookie=%lu",
            LogTitle.GetWithTime().c_str(),
            kind == ECleanupTarget::PBuffer ? "PBuffer" : "DDisk",
            target.ToString().c_str(),
            cookie);

        auto ev = std::make_unique<IEventHandle>(
            target,
            ctx.SelfID,
            event.release(),
            IEventHandle::FlagTrackDelivery |
                IEventHandle::FlagSubscribeOnSession,
            cookie);
        ctx.Send(ev.release());
    }

    void OnWipeResult(
        const TActorContext& ctx,
        ui64 cookie,
        const NProto::TError& error)
    {
        auto it = InFlight.find(cookie);
        if (it == InFlight.end()) {
            return;
        }

        InFlight.erase(it);

        if (HasError(error)) {
            Complete(ctx, error);
            return;
        }

        if (!InFlight.empty()) {
            return;
        }

        if (Phase == ECleanupPhase::WipePBuffer) {
            StartDDiskChunkDelete(ctx);
        } else {
            StartDeallocate(ctx);
        }
    }

    void HandleErasePersistentBufferResult(
        const NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleErasePersistentBufferResult cookie=%lu status=%s",
            LogTitle.GetWithTime().c_str(),
            ev->Cookie,
            NKikimrBlobStorage::NDDisk::TReplyStatus_E_Name(record.GetStatus())
                .c_str());

        OnWipeResult(ctx, ev->Cookie, TranslateError(record));
    }

    void HandleDeleteTabletChunksResult(
        const NDDisk::TEvDeleteTabletChunksResult::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleDeleteTabletChunksResult cookie=%lu status=%s",
            LogTitle.GetWithTime().c_str(),
            ev->Cookie,
            NKikimrBlobStorage::NDDisk::TReplyStatus_E_Name(record.GetStatus())
                .c_str());

        OnWipeResult(ctx, ev->Cookie, TranslateError(record));
    }

    void HandleUndelivered(
        const TEvents::TEvUndelivered::TPtr& ev,
        const TActorContext& ctx)
    {
        auto it = InFlight.find(ev->Cookie);
        if (it == InFlight.end()) {
            return;
        }

        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Wipe request undelivered target=%s cookie=%lu",
            LogTitle.GetWithTime().c_str(),
            it->second.Target.ToString().c_str(),
            ev->Cookie);

        Complete(ctx, MakeError(E_REJECTED, "wipe request undelivered"));
    }

    void HandleNodeDisconnected(
        TEvInterconnect::TEvNodeDisconnected::TPtr& ev,
        const TActorContext& ctx)
    {
        const ui32 nodeId = ev->Get()->NodeId;
        for (const auto& [cookie, request]: InFlight) {
            if (request.NodeId == nodeId) {
                LOG_ERROR(
                    ctx,
                    NKikimrServices::NBS_PARTITION,
                    "%s Node %u disconnected during wipe cookie=%lu target=%s",
                    LogTitle.GetWithTime().c_str(),
                    nodeId,
                    cookie,
                    request.Target.ToString().c_str());
                Complete(
                    ctx,
                    MakeError(E_REJECTED, "node disconnected during wipe"));
                return;
            }
        }
    }

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Complete(ctx, MakeError(E_TIMEOUT, "partition cleanup timed out"));
    }

    void StartDeallocate(const TActorContext& ctx)
    {
        Become(&TThis::StateDeallocate);

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Become StateDeallocate",
            LogTitle.GetWithTime().c_str());

        BSControllerPipeClient = ctx.Register(
            NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));

        auto request = std::make_unique<
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
        request->Record.SetDDiskPoolName(Params.DDiskPoolName);
        request->Record.SetPersistentBufferDDiskPoolName(
            Params.PersistentBufferDDiskPoolName);
        request->Record.SetTabletId(Params.TabletId);

        for (size_t i = 0; i < Params.DirectBlockGroupsCount; ++i) {
            auto* op = request->Record.AddDirectBlockGroupOperations();
            op->SetDirectBlockGroupId(i);
            auto* define = op->MutableDefineDirectBlockGroup();
            define->SetNumDDisks(0);
            define->SetNumChunksPerDDisk(0);
            define->SetNumPersistentBuffers(0);
        }

        NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
    }

    void HandleDeallocateResult(
        const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr&
            ev,
        const TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleDeallocateResult status=%s reason=%s",
            LogTitle.GetWithTime().c_str(),
            NKikimrProto::EReplyStatus_Name(record.GetStatus()).c_str(),
            record.GetErrorReason().c_str());

        Complete(ctx, CheckDeallocateResult(*ev->Get()));
    }

    void HandleConnect(
        TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        if (msg->ClientId != BSControllerPipeClient) {
            return;
        }
        if (msg->Status == NKikimrProto::OK) {
            return;
        }

        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s BSController pipe connect failed during deallocate: %s",
            LogTitle.GetWithTime().c_str(),
            NKikimrProto::EReplyStatus_Name(msg->Status).c_str());

        Complete(
            ctx,
            MakeError(
                E_REJECTED,
                "BSController pipe connect failed during deallocate"));
    }

    void HandleDisconnect(
        TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        if (msg->ClientId != BSControllerPipeClient) {
            return;
        }

        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s BSController pipe destroyed during deallocate",
            LogTitle.GetWithTime().c_str());

        Complete(
            ctx,
            MakeError(
                E_REJECTED,
                "BSController pipe destroyed during deallocate"));
    }

    void Complete(const TActorContext& ctx, NProto::TError error)
    {
        if (Completed) {
            return;
        }
        Completed = true;

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Cleanup completed: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        ctx.Send(
            Params.Parent,
            new TEvPartitionDirectPrivate::TEvPartitionCleanupCompleted(
                std::move(error)));
        PassAway();
    }

    void PassAway() override
    {
        if (BSControllerPipeClient) {
            NTabletPipe::CloseClient(
                TActivationContext::AsActorContext(),
                BSControllerPipeClient);
            BSControllerPipeClient = {};
        }
        TActorBootstrapped::PassAway();
    }
};

////////////////////////////////////////////////////////////////////////////////

IActor* CreatePartitionCleanupActor(TPartitionCleanupParams params)
{
    return new TPartitionCleanupActor(std::move(params));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
