#include "dst_remover.h"
#include "logging.h"
#include "private_events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

using namespace NSchemeShard;

class TDstRemover: public TActorBootstrapped<TDstRemover> {
    void AllocateTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateAllocateTxId"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        TxId = ev->Get()->TxId;
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        if (PendingDstPathId) {
            DetachDst();
        } else {
            DropDst();
        }
    }

    void DetachDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        auto& tx = *ev->Record.AddTransaction();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        tx.SetInternal(true);
        PendingDstPathId.ToProto(tx.MutableAlterTable()->MutablePathId());
        tx.MutableAlterTable()->MutableReplicationConfig()->SetMode(
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);

        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateDropDst);
    }

    void DropDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        auto& tx = *ev->Record.AddTransaction();
        tx.MutableDrop()->SetId(DstPathId.LocalPathId);

        switch (Kind) {
        case TReplication::ETargetKind::Table:
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
            break;
        case TReplication::ETargetKind::IndexTable:
        case TReplication::ETargetKind::Transfer:
            Y_ABORT("unreachable");
        }

        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateDropDst);
    }

    STATEFN(StateDropDst) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateDropDst"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            sFunc(TEvents::TEvWakeup, AllocateTxId);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            Y_DEBUG_ABORT_UNLESS(TxId == record.GetTxId());
            return SubscribeTx(record.GetTxId());
        case NKikimrScheme::StatusMultipleModifications:
            if (PendingDstPathId) {
                return Retry();
            }
            if (record.HasPathDropTxId()) {
                return SubscribeTx(record.GetPathDropTxId());
            } else {
                return Error(record.GetStatus(), record.GetReason());
            }
            break;
        case NKikimrScheme::StatusPathDoesNotExist:
            return Success();
        default:
            if (PendingDstPathId) {
                YDB_LOG_WARN("Retry detach dst", {"status", record.GetStatus()}, {"reason", record.GetReason()});
                return Retry();
            }
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    void SubscribeTx(ui64 txId) {
        YDB_LOG_DEBUG("Subscribe tx",
            {"txId", txId});
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        Success();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (SchemeShardId == ev->Get()->TabletId) {
            return;
        }

        Retry();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        Retry();
    }

    void Success() {
        YDB_LOG_INFO("Success");

        Send(Parent, new TEvPrivate::TEvDropDstResult(ReplicationId, TargetId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        YDB_LOG_ERROR("Error",
            {"status", status},
            {"reason", error});

        Send(Parent, new TEvPrivate::TEvDropDstResult(ReplicationId, TargetId, status, error));
        PassAway();
    }

    void Retry() {
        YDB_LOG_DEBUG("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_REMOVER;
    }

    explicit TDstRemover(
            const TActorId& parent,
            ui64 schemeShardId,
            const TActorId& proxy,
            ui64 rid,
            ui64 tid,
            TReplication::ETargetKind kind,
            const TPathId& dstPathId,
            const TPathId& pendingDstPathId)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , YdbProxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , DstPathId(dstPathId)
        , PendingDstPathId(pendingDstPathId)
        , LogPrefix(CreateActorLogPrefix("DstRemover", ReplicationId, TargetId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (PendingDstPathId) {
            return AllocateTxId();
        }
        if (!DstPathId) {
            Success();
        } else {
            switch (Kind) {
            case TReplication::ETargetKind::Table:
                return AllocateTxId();
            case TReplication::ETargetKind::IndexTable:
            case TReplication::ETargetKind::Transfer:
                // indexed table will be removed along with its indexes
                // transfer works with an existing table and removing isn`t required
                return Success();
            }
        }
    }

    STATEFN(StateBase) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateBase"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 SchemeShardId;
    const TActorId YdbProxy;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TPathId DstPathId;
    const TPathId PendingDstPathId;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    ui64 TxId = 0;
    TActorId PipeCache;

}; // TDstRemover

IActor* CreateDstRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    return CreateDstRemover(ctx.SelfID, replication->GetSchemeShardId(), replication->GetYdbProxy(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetDstPathId(),
        target->GetPendingDstPathId());
}

IActor* CreateDstRemover(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId,
        const TPathId& pendingDstPathId)
{
    return new TDstRemover(parent, schemeShardId, proxy, rid, tid, kind, dstPathId, pendingDstPathId);
}

}
