#include "dst_alterer.h"
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

class TDstAlterer: public TActorBootstrapped<TDstAlterer> {
    void AllocateTxId() {
        TxId = 0;
        WaitingForCompletion = false;
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
        Become(&TThis::StateAllocateTxId);
    }

    STATEFN(StateAllocateTxId) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState","StateAllocateTxId"});
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
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateAlterDst"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        const auto& record = ev->Get()->Record;

        // A reply from a previous proposal can arrive after its retry started.
        if (record.GetTxId() != TxId) {
            return;
        }

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            WaitingForCompletion = true;
            return SubscribeTx(record.GetTxId());
        case NKikimrScheme::StatusMultipleModifications:
        case NKikimrScheme::StatusResourceExhausted:
            // Concurrent DDL and SchemeShard in-flight limits are temporary.
            return Retry();
        case NKikimrScheme::StatusPathDoesNotExist:
            // A deleted destination has nothing left to detach.
            return Success();
        case NKikimrScheme::StatusNameConflict:
            // An already collected path id resolves to an empty path, which
            // ALTER reports as a name conflict. Confirm that it is absent.
            AlterError = record.GetReason();
            Send(PipeCache, new TEvPipeCache::TEvForward(
                new TEvSchemeShard::TEvDescribeScheme(DstPathId), SchemeShardId));
            return Become(&TThis::StateCheckDst);
        default:
            return Error(record.GetStatus(), record.GetReason());
        }
    }

    STATEFN(StateCheckDst) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateCheckDst"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (ev->Get()->GetRecord().GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
            return Success();
        }

        return Error(NKikimrScheme::StatusNameConflict, AlterError);
    }

    void SubscribeTx(ui64 txId) {
        YDB_LOG_DEBUG("Subscribe tx",
            {"txId", txId});
        Send(PipeCache, new TEvPipeCache::TEvForward(new TEvSchemeShard::TEvNotifyTxCompletion(txId), SchemeShardId));
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        if (!WaitingForCompletion || ev->Get()->Record.GetTxId() != TxId) {
            return;
        }

        Success();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (SchemeShardId != ev->Get()->TabletId) {
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

        Send(Parent, new TEvPrivate::TEvAlterDstResult(ReplicationId, TargetId));
        PassAway();
    }

    void Error(NKikimrScheme::EStatus status, const TString& error) {
        YDB_LOG_ERROR("Error",
            {"status", status},
            {"reason", error});

        Send(Parent, new TEvPrivate::TEvAlterDstResult(ReplicationId, TargetId, status, error));
        PassAway();
    }

    void Retry() {
        YDB_LOG_DEBUG("Retry");
        WaitingForCompletion = false;
        Schedule(RetryInterval, new TEvents::TEvWakeup);
        Become(&TThis::StateRetry);
    }

    STATEFN(StateRetry) {
        // Retired proposals and duplicate delivery failures cannot complete the
        // operation or schedule additional retries while the backoff is pending.
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, AllocateTxId);
            sFunc(TEvents::TEvPoison, PassAway);
        }
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
        , LogPrefix(CreateActorLogPrefix("DstAlterer", ReplicationId, TargetId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
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
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TPathId DstPathId;
    const TReplication::EState DesiredState;
    NActors::NStructuredLog::TStructuredMessage LogPrefix;

    ui64 TxId = 0;
    bool WaitingForCompletion = false;
    TActorId PipeCache;
    TString AlterError;
    static constexpr auto RetryInterval = TDuration::Seconds(10);

}; // TDstAlterer

IActor* CreateDstAlterer(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    // Attachment validation can fail after the table has become read-only.
    // DONE must detach that persisted path even without a confirmed destination.
    const auto& dstPathId = target->GetDstPathId()
        ? target->GetDstPathId()
        : target->GetPendingDstPathId();
    return CreateDstAlterer(ctx.SelfID, replication->GetSchemeShardId(),
        replication->GetId(), target->GetId(), target->GetKind(), dstPathId, replication->GetDesiredState());
}

IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId, TReplication::EState desiredState)
{
    return new TDstAlterer(parent, schemeShardId, rid, tid, kind, dstPathId, desiredState);
}

}
