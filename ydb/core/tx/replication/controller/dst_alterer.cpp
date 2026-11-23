#include "dst_alterer.h"
#include "cdc_trim_verifier.h"
#include "logging.h"
#include "private_events.h"
#include "topic_mirror.h"
#include "target_cdc.h"

#include <util/generic/algorithm.h>

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

struct TCdcDetachStream {
    TPathId PathId;
    bool Activate;
};

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
        if (!CdcStreams.empty()) {
            DescribeDst(TrimRequestId ? TrimTablePathId : DstPathId);
            Become(&TThis::StateDescribeCdcTable);
        } else if (IsTopicTarget(Kind)) {
            DescribeDst(DstPathId);
            Become(&TThis::StateDescribeTopic);
        } else {
            AlterDst();
        }
    }

    void DescribeDst(const TPathId& pathId) {
        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(pathId);
        request->Record.MutableOptions()->SetShowPrivateTable(Kind == TReplication::ETargetKind::CdcTopic);
        if (TrimRequestId) {
            request->Record.MutableOptions()->SetReturnPartitioningInfo(true);
        }
        Send(PipeCache, new TEvPipeCache::TEvForward(request.Release(), SchemeShardId), 0,
            TrimRequestId ? ++DescribeGeneration : 0);
    }

    STATEFN(StateDescribeCdcTable) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, HandleCdcTable);
        default:
            return StateBase(ev);
        }
    }

    void HandleCdcTable(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        if (TrimRequestId && ev->Cookie != DescribeGeneration) {
            return;
        }
        const auto& record = ev->Get()->GetRecord();
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            return Error(record.GetStatus(), record.GetReason());
        }

        const auto& desc = record.GetPathDescription();
        const auto& self = desc.GetSelf();
        const auto& tablePathId = TrimRequestId ? TrimTablePathId : DstPathId;
        if (!desc.HasTable() || TPathId(self.GetSchemeshardId(), self.GetPathId()) != tablePathId) {
            return Error(NKikimrScheme::StatusPreconditionFailed, "CDC parent identity changed");
        }

        if (!self.GetCreateFinished() || self.GetPathState() != NKikimrSchemeOp::EPathStateNoChanges) {
            return Retry();
        }

        const auto& table = desc.GetTable();
        const auto& replication = table.GetReplicationConfig();
        const bool detached = replication.GetMode() == NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE;
        if ((!detached && (replication.GetMode() != NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY
                || TPathId::FromProto(replication.GetOwnerPathId()) != ReplicationPathId))
            || (detached && replication.HasOwnerPathId()))
        {
            return Error(NKikimrScheme::StatusPreconditionFailed, "CDC parent ownership changed");
        }

        for (const auto& expected : CdcStreams) {
            const auto stream = FindIf(table.GetCdcStreams(), [&](const auto& entry) {
                return TPathId::FromProto(entry.GetPathId()) == expected.PathId;
            });

            const auto state = detached && expected.Activate
                ? NKikimrSchemeOp::ECdcStreamStateReady : NKikimrSchemeOp::ECdcStreamStateDisabled;
            if (stream == table.GetCdcStreams().end() || stream->GetState() != state) {
                return Error(NKikimrScheme::StatusPreconditionFailed, "CDC handoff identities or states changed");
            }
        }

        if (CompletingCdcSetup) {
            if (detached) {
                return Error(NKikimrScheme::StatusPreconditionFailed, "CDC setup parent is no longer a replica");
            }
            if (EqualToOneOf(replication.GetImportState(),
                    NKikimrSchemeOp::TTableReplicationConfig::IMPORT_STATE_NONE,
                    NKikimrSchemeOp::TTableReplicationConfig::IMPORTED))
            {
                return Success();
            }
            if (replication.GetImportState() != NKikimrSchemeOp::TTableReplicationConfig::IMPORTING) {
                return Error(NKikimrScheme::StatusPreconditionFailed, "CDC setup parent state changed");
            }
        } else if (detached) {
            return Success();
        }

        ClearingCdcSetup = !TrimRequestId
            && replication.GetImportState() == NKikimrSchemeOp::TTableReplicationConfig::IMPORTING;
        if (ClearingCdcSetup) {
            SetupReplicationConfig = replication;
            SetupReplicationConfig.SetImportState(NKikimrSchemeOp::TTableReplicationConfig::IMPORT_STATE_NONE);
        }

        CdcTableVersion = self.GetPathVersion();
        if (TrimRequestId) {
            DescribeDst(DstPathId);
            return Become(&TThis::StateDescribeTopic);
        }
        AlterDst();
    }

    STATEFN(StateDescribeTopic) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, HandleTopic);
        default:
            return StateBase(ev);
        }
    }

    void HandleTopic(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        if (TrimRequestId && ev->Cookie != DescribeGeneration) {
            return;
        }
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

        if (TrimRequestId) {
            const auto& self = desc.GetSelf();
            const auto& config = desc.GetPersQueueGroup().GetPQTabletConfig();
            if (TPathId(self.GetSchemeshardId(), self.GetPathId()) != DstPathId
                || !config.HasCdcReplication() || config.GetReplicationSourceConsumer().empty())
            {
                return Error(NKikimrScheme::StatusPreconditionFailed, "Managed CDC topic identity changed");
            }
            if (!self.GetCreateFinished() || self.GetPathState() != NKikimrSchemeOp::EPathStateNoChanges) {
                return Retry();
            }
            if (config.GetPartitionConfig().HasMirrorFrom()) {
                return Error(NKikimrScheme::StatusPreconditionFailed, "CDC mirroring must stop before trimming");
            }
            if (config.GetCdcReplication().GetTrimTxId() == TrimRequestId) {
                TrimVerifier = Register(CreateCdcTrimVerifier(SelfId(), DstPathId, TrimRequestId,
                    desc.GetPersQueueGroup()));
                return Become(&TThis::StateVerifyTrim);
            }
            if (VerifyTrim || config.GetCdcReplication().HasTrimTxId()) {
                return Error(NKikimrScheme::StatusPreconditionFailed,
                    "CDC trim was not applied or a different trim request owns the topic");
            }
        }

        TopicTx.Clear();
        TopicTx.SetWorkingDir(TString(ExtractParent(record.GetPath())));
        TopicTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);

        auto& alter = *TopicTx.MutableAlterPersQueueGroup();
        alter.SetName(desc.GetSelf().GetName());
        alter.SetPathId(DstPathId.LocalPathId);
        auto& config = *alter.MutablePQTabletConfig();
        config = desc.GetPersQueueGroup().GetPQTabletConfig();
        if (Kind == TReplication::ETargetKind::CdcTopic) {
            config.ClearPartitionKeySchema();
        }
        if (TrimRequestId) {
            alter.SetTrimCdcReplicationTxId(TrimRequestId);
        }

        auto& partition = *config.MutablePartitionConfig();
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
        if (TrimRequestId) {
            auto& parentCondition = *TopicTx.AddApplyIf();
            parentCondition.SetPathId(TrimTablePathId.LocalPathId);
            parentCondition.SetPathVersion(CdcTableVersion);
        }

        AlterDst();
    }

    void AlterDst() {
        auto ev = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(TxId, SchemeShardId);
        auto& tx = *ev->Record.AddTransaction();

        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            DstPathId.ToProto(tx.MutableAlterTable()->MutablePathId());
            if (ClearingCdcSetup) {
                *tx.MutableAlterTable()->MutableReplicationConfig() = SetupReplicationConfig;
            } else {
                tx.MutableAlterTable()->MutableReplicationConfig()->SetMode(
                    NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
            }
            if (CdcTableVersion) {
                auto& condition = *tx.AddApplyIf();
                condition.SetPathId(DstPathId.LocalPathId);
                condition.SetPathVersion(CdcTableVersion);
            }
            for (const auto& stream : CdcStreams) {
                if (stream.Activate && !ClearingCdcSetup) {
                    stream.PathId.ToProto(tx.MutableAlterTable()->AddActivateCdcStreams());
                }
            }
            break;
        case TReplication::ETargetKind::CdcTopic:
        case TReplication::ETargetKind::Topic:
            tx.CopyFrom(TopicTx);
            break;
        case TReplication::ETargetKind::Transfer:
            break;
        }

        tx.SetInternal(true);
        if (ReplicationPathId) {
            ReplicationPathId.ToProto(tx.MutableExpectedReplicationOwnerPathId());
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
            return CompleteAlter();
        case NKikimrScheme::StatusPathDoesNotExist:
            return DestinationMissing();
        case NKikimrScheme::StatusNameConflict:
            // An already collected path id resolves to an empty path, which
            // ALTER reports as a name conflict. Confirm that it is absent.
            AlterError = record.GetReason();
            DescribeDst(DstPathId);
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

        CompleteAlter();
    }

    void CompleteAlter() {
        if (ClearingCdcSetup) {
            ClearingCdcSetup = false;
            AllocateTxId();
        } else if (TrimRequestId) {
            VerifyTrim = true;
            DescribeDst(DstPathId);
            Become(&TThis::StateDescribeTopic);
        } else {
            Success();
        }
    }

    STATEFN(StateVerifyTrim) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCdcTrimVerified, HandleTrimVerified);
        default:
            return StateBase(ev);
        }
    }

    void HandleTrimVerified(TEvPrivate::TEvCdcTrimVerified::TPtr& ev) {
        if (ev->Sender != TrimVerifier) {
            return;
        }
        TrimVerifier = {};
        if (!ev->Get()->Success) {
            return Error(NKikimrScheme::StatusPreconditionFailed, ev->Get()->Error);
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
        if (TrimRequestId) {
            return Error(NKikimrScheme::StatusPathDoesNotExist, "Managed CDC topic is missing");
        }
        if (IsTopicTarget(Kind) && DesiredState == TReplication::EState::Ready) {
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
        if (TrimVerifier) {
            Send(TrimVerifier, new TEvents::TEvPoison());
            TrimVerifier = {};
        }
        WaitingForCompletion = false;
        Schedule(RetryInterval, new TEvents::TEvWakeup);
        Become(&TThis::StateRetry);
    }

    void PassAway() override {
        if (TrimVerifier) {
            Send(TrimVerifier, new TEvents::TEvPoison());
        }
        TActorBootstrapped<TDstAlterer>::PassAway();
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

    TDstAlterer(const TActorId& parent, ui64 schemeShardId, ui64 rid, ui64 tid,
            const TPathId& dstPathId, const TPathId& replicationPathId, TVector<TCdcDetachStream>&& streams,
            bool completingCdcSetup = false)
        : TDstAlterer(parent, schemeShardId, rid, tid, TReplication::ETargetKind::Table,
            dstPathId, TReplication::EState::Done, replicationPathId, {})
    {
        CdcStreams = std::move(streams);
        CompletingCdcSetup = completingCdcSetup;
    }

    TDstAlterer(const TActorId& parent, ui64 schemeShardId, ui64 rid, ui64 tid,
            const TPathId& dstPathId, const TPathId& replicationPathId, const NKikimrReplicationController::TCdcTarget& cdc)
        : TDstAlterer(parent, schemeShardId, rid, tid, TReplication::ETargetKind::CdcTopic,
            dstPathId, TReplication::EState::Done, replicationPathId, {})
    {
        Trimming = true;
        TrimRequestId = cdc.GetTrimRequestId();
        TrimTablePathId = TPathId::FromProto(cdc.GetTablePathId());
        CdcStreams.push_back({TPathId::FromProto(cdc.GetStreamPathId()), cdc.GetActivateOnDetach()});
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (Trimming && (!TrimRequestId || !TrimTablePathId || !CdcStreams.front().PathId)) {
            return Error(NKikimrScheme::StatusInvalidParameter, "CDC trim request identities are missing");
        }
        if (IsTopicTarget(Kind)) {
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
    TVector<TCdcDetachStream> CdcStreams;
    NKikimrSchemeOp::TModifyScheme TopicTx;
    NActors::NStructuredLog::TStructuredMessage LogPrefix;

    ui64 TxId = 0;
    ui64 CdcTableVersion = 0;
    ui64 TrimRequestId = 0;
    ui64 DescribeGeneration = 0;
    bool VerifyTrim = false;
    bool Trimming = false;
    bool CompletingCdcSetup = false;
    bool ClearingCdcSetup = false;
    NKikimrSchemeOp::TTableReplicationConfig SetupReplicationConfig;
    TPathId TrimTablePathId;
    TActorId TrimVerifier;
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
    if (const auto* cdc = GetCdcTarget(target);
        cdc && cdc->GetCdc().GetPhase() == NKikimrReplicationController::TCdcTarget::TRIMMING)
    {
        return new TDstAlterer(ctx.SelfID, replication->GetSchemeShardId(), replication->GetId(), targetId,
            target->GetDstPathId(), replication->GetPathId(), cdc->GetCdc());
    }
    if (IsTopicTarget(target->GetKind())) {
        TTopicMirrorSettings topicMirror(replication->GetConfig().GetSrcConnectionParams(),
            target->GetSrcPath(), target->GetStreamConsumerName());
        if (target->GetKind() == TReplication::ETargetKind::CdcTopic) {
            return new TDstAlterer(ctx.SelfID, replication->GetSchemeShardId(), replication->GetId(), targetId,
                target->GetKind(), target->GetDstPathId(), desiredState, replication->GetPathId(), std::move(topicMirror));
        }
        return CreateDstTopicAlterer(ctx.SelfID, replication->GetSchemeShardId(), replication->GetId(), targetId,
            target->GetDstPathId(), desiredState, std::move(topicMirror));
    }

    TVector<TCdcDetachStream> streams;
    const auto children = GetCdcChildren(*replication, targetId);
    const bool completingCdcSetup = target->GetDstState() == TReplication::EDstState::PreparingCdc
        || (desiredState == TReplication::EState::Ready && !children.empty());
    if (desiredState == TReplication::EState::Done || completingCdcSetup) {
        for (const auto* child : children) {
            const auto& state = child->GetCdc();
            if (state.GetPhase() == NKikimrReplicationController::TCdcTarget::FINISHED && !state.HasStreamPathId()) {
                continue;
            }
            streams.push_back({TPathId::FromProto(state.GetStreamPathId()), !completingCdcSetup && child->ActivateOnDetach()});
        }
    }
    if (completingCdcSetup || !streams.empty()) {
        return new TDstAlterer(ctx.SelfID, replication->GetSchemeShardId(), replication->GetId(), targetId,
            target->GetDstPathId(), replication->GetPathId(), std::move(streams), completingCdcSetup);
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
