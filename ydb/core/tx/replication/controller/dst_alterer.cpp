#include "dst_alterer.h"
#include "logging.h"
#include "private_events.h"
#include "topic_mirror.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/replication.pb.h>
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
        if (Kind == TReplication::ETargetKind::Topic) {
            Send(PipeCache, new TEvPipeCache::TEvForward(
                new TEvSchemeShard::TEvDescribeScheme(DstPathId), SchemeShardId));
            Become(&TThis::StateDescribeTopic);
        } else {
            AlterDst();
        }
    }

    STATEFN(StateDescribeTopic) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, HandleTopic);
        default:
            return StateBase(ev);
        }
    }

    void HandleTopic(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        if (record.GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
            return DestinationMissing();
        } else if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            return Error(record.GetStatus(), record.GetReason());
        }

        const auto& desc = record.GetPathDescription();
        if (!desc.HasPersQueueGroup()) {
            return Error(NKikimrScheme::StatusSchemeError, "Destination is not a topic");
        }

        TopicTx.Clear();
        TopicTx.SetWorkingDir(TString(ExtractParent(record.GetPath())));
        TopicTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);

        auto& alter = *TopicTx.MutableAlterPersQueueGroup();
        alter.SetName(desc.GetSelf().GetName());
        alter.SetPathId(DstPathId.LocalPathId);
        alter.MutablePQTabletConfig()->CopyFrom(desc.GetPersQueueGroup().GetPQTabletConfig());

        auto& partition = *alter.MutablePQTabletConfig()->MutablePartitionConfig();
        if (DesiredState == TReplication::EState::Ready) {
            if (TopicMirror.Error) {
                return Error(NKikimrScheme::StatusInvalidParameter, TopicMirror.Error);
            }

            partition.MutableMirrorFrom()->CopyFrom(*TopicMirror.Config);
        } else {
            partition.ClearMirrorFrom();
        }

        auto& condition = *TopicTx.AddApplyIf();
        condition.SetPathId(DstPathId.LocalPathId);
        condition.SetPathVersion(desc.GetSelf().GetPathVersion());

        AlterDst();
    }

    void AlterDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        auto& tx = *ev->Record.AddTransaction();
        tx.SetInternal(true);
        if (ReplicationPathId) {
            ReplicationPathId.ToProto(tx.MutableExpectedReplicationOwnerPathId());
        }

        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            DstPathId.ToProto(tx.MutableAlterTable()->MutablePathId());
            tx.MutableAlterTable()->MutableReplicationConfig()->SetMode(
                NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
            break;
        case TReplication::ETargetKind::CdcTopic:
        case TReplication::ETargetKind::Topic:
            tx.CopyFrom(TopicTx);
            tx.SetInternal(true);
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
        case NKikimrScheme::StatusPreconditionFailed:
        case NKikimrScheme::StatusMultipleModifications:
        case NKikimrScheme::StatusResourceExhausted:
            // Concurrent DDL and SchemeShard in-flight limits are temporary.
            return Retry();
        case NKikimrScheme::StatusSuccess:
            // SchemeShard completed the conditional cleanup without changes.
            return Success();
        case NKikimrScheme::StatusPathDoesNotExist:
            return DestinationMissing();
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
            return DestinationMissing();
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

    void DestinationMissing() {
        if (Kind == TReplication::ETargetKind::Topic && DesiredState == TReplication::EState::Ready) {
            return Error(NKikimrScheme::StatusPathDoesNotExist,
                "Cannot resume mirroring: destination topic does not exist");
        }
        // PAUSE, DONE and DROP have nothing left to detach.
        Success();
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
            const TReplication::EState desiredState,
            const TPathId& replicationPathId,
            TTopicMirrorSettings&& topicMirror)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , DstPathId(dstPathId)
        , ReplicationPathId(replicationPathId)
        , DesiredState(desiredState)
        , TopicMirror(std::move(topicMirror))
        , LogPrefix(CreateActorLogPrefix("DstAlterer", ReplicationId, TargetId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (Kind == TReplication::ETargetKind::Topic) {
            return DstPathId ? AllocateTxId() : DestinationMissing();
        }
        switch (DesiredState) {
        case TReplication::EState::Done:
            if (!DstPathId) {
                return Success();
            } else {
                switch (Kind) {
                case TReplication::ETargetKind::Table:
                case TReplication::ETargetKind::IndexTable:
                    return AllocateTxId();
                case TReplication::ETargetKind::CdcTopic:
                case TReplication::ETargetKind::Topic:
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
    const TPathId ReplicationPathId;
    const TReplication::EState DesiredState;
    const TTopicMirrorSettings TopicMirror;
    NKikimrSchemeOp::TModifyScheme TopicTx;
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

    const auto desiredState = replication->GetState() == TReplication::EState::Removing
        ? TReplication::EState::Done : replication->GetDesiredState();
    if (target->GetKind() == TReplication::ETargetKind::Topic) {
        TTopicMirrorSettings topicMirror(replication->GetConfig().GetSrcConnectionParams(),
            target->GetSrcPath(), target->GetStreamConsumerName());
        return CreateDstTopicAlterer(ctx.SelfID, replication->GetSchemeShardId(),
            replication->GetId(), target->GetId(), target->GetDstPathId(), desiredState, std::move(topicMirror));
    }

    return CreateDstAlterer(ctx.SelfID, replication->GetSchemeShardId(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetDstPathId(),
        desiredState, replication->GetPathId());
}

IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId, TReplication::EState desiredState,
        const TPathId& replicationPathId)
{
    return new TDstAlterer(parent, schemeShardId, rid, tid, kind, dstPathId, desiredState, replicationPathId, {});
}

IActor* CreateDstTopicAlterer(const TActorId& parent, ui64 schemeShardId,
        ui64 rid, ui64 tid, const TPathId& dstPathId, TReplication::EState desiredState,
        TTopicMirrorSettings&& topicMirror)
{
    return new TDstAlterer(parent, schemeShardId, rid, tid, TReplication::ETargetKind::Topic,
        dstPathId, desiredState, {}, std::move(topicMirror));
}

}
