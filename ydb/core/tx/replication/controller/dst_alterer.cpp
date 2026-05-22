#include "dst_alterer.h"
#include "logging.h"
#include "private_events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

using namespace NSchemeShard;

class TDstAlterer: public TActorBootstrapped<TDstAlterer> {
    void AllocateTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"LogPrefix", LogPrefix},
            {"#_ev->Get()->ToString()", ev->Get()->ToString()});

        TxId = ev->Get()->TxId;
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        AlterDst();
    }

    void AlterDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        auto& tx = *ev->Record.AddTransaction();
        tx.SetInternal(true);

        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            DstPathId.ToProto(tx.MutableAlterTable()->MutablePathId());
            tx.MutableAlterTable()->MutableReplicationConfig()->SetMode(
                NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
            break;
        case TReplication::ETargetKind::Transfer:
            break;
        }

        Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
        Become(&TThis::StateAlterDst);
    }

    STATEFN(StateAlterDst) {
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
            {"LogPrefix", LogPrefix},
            {"#_ev->Get()->ToString()", ev->Get()->ToString()});
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            Y_DEBUG_ABORT_UNLESS(TxId == record.GetTxId());
            return SubscribeTx(record.GetTxId());
        case NKikimrScheme::StatusMultipleModifications:
            return Retry();
        default:
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    void SubscribeTx(ui64 txId) {
        YDB_LOG_DEBUG("Subscribe tx",
            {"LogPrefix", LogPrefix},
            {"txId", txId});
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"LogPrefix", LogPrefix},
            {"#_ev->Get()->ToString()", ev->Get()->ToString()});
        Success();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"LogPrefix", LogPrefix},
            {"#_ev->Get()->ToString()", ev->Get()->ToString()});

        if (SchemeShardId == ev->Get()->TabletId) {
            return;
        }

        Retry();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"LogPrefix", LogPrefix},
            {"#_ev->Get()->ToString()", ev->Get()->ToString()});
        Retry();
    }

    void Success() {
        YDB_LOG_INFO("Success",
            {"LogPrefix", LogPrefix});

        Send(Parent, new TEvPrivate::TEvAlterDstResult(ReplicationId, TargetId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        YDB_LOG_ERROR("Error",
            {"LogPrefix", LogPrefix},
            {"status", status},
            {"reason", error});

        Send(Parent, new TEvPrivate::TEvAlterDstResult(ReplicationId, TargetId, status, error));
        PassAway();
    }

    void Retry() {
        YDB_LOG_DEBUG("Retry",
            {"LogPrefix", LogPrefix});
        Schedule(RetryInterval, new TEvents::TEvWakeup);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_ALTERER;
    }

    explicit TDstAlterer(
            const TActorId& parent,
            ui64 schemeShardId,
            ui64 rid,
            ui64 tid,
            TReplication::ETargetKind kind,
            const TPathId& dstPathId,
            const TReplication::EState desiredState)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , DstPathId(dstPathId)
        , DesiredState(desiredState)
        , LogPrefix("DstAlterer", ReplicationId, TargetId)
    {
    }

    void Bootstrap() {
        switch (DesiredState) {
        case TReplication::EState::Done:
            if (!DstPathId) {
                return Success();
            } else {
                switch (Kind) {
                case TReplication::ETargetKind::Table:
                case TReplication::ETargetKind::IndexTable:
                    return AllocateTxId();
                case TReplication::ETargetKind::Transfer:
                    return Success();
                }
            }
        case TReplication::EState::Paused:
        case TReplication::EState::Ready:
        case TReplication::EState::Error:
        case TReplication::EState::Removing:
            return Success();
        }
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 SchemeShardId;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TPathId DstPathId;
    const TReplication::EState DesiredState;
    const TActorLogPrefix LogPrefix;

    ui64 TxId = 0;
    TActorId PipeCache;
    static constexpr auto RetryInterval = TDuration::Seconds(10);

}; // TDstAlterer

IActor* CreateDstAlterer(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    return CreateDstAlterer(ctx.SelfID, replication->GetSchemeShardId(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetDstPathId(), replication->GetDesiredState());
}

IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId, TReplication::EState desiredState)
{
    return new TDstAlterer(parent, schemeShardId, rid, tid, kind, dstPathId, desiredState);
}

}
