#include "dst_alterer.h"
#include "index_replication.h"
#include "logging.h"
#include "private_events.h"
#include "util.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

using namespace NSchemeShard;

class TDstAlterer: public TActorBootstrapped<TDstAlterer> {
    void PrepareSequences() {
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(SrcPath,
            NYdb::NTable::TDescribeTableSettings().WithSetVal(true)));
        Become(&TThis::StateDescribeSource);
    }

    STATEFN(StateDescribeSource) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            sFunc(TEvents::TEvWakeup, PrepareSequences);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                Schedule(RetryInterval, new TEvents::TEvWakeup);
                return;
            }
            return Error(NKikimrScheme::StatusPreconditionFailed,
                TStringBuilder() << "Cannot read fulltext/JSON sequence watermarks from " << SrcPath
                    << " before completion: " << result.GetIssues().ToString());
        }
        for (const auto& column : NYdb::TProtoAccessor::GetProto(result.GetTableDescription()).columns()) {
            if ((column.name() != NTableIndex::NFulltext::GenColumn
                && column.name() != NTableIndex::NFulltext::RowIdColumn) || !column.has_from_sequence())
            {
                continue;
            }
            const auto& source = column.from_sequence();
            if (!source.has_set_val()) {
                return Error(NKikimrScheme::StatusPreconditionFailed,
                    TStringBuilder() << "Source " << SrcPath << " did not return sequence watermark for " << column.name());
            }
            auto& tx = SequenceAlters.emplace_back();
            tx.SetInternal(true);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterSequence);
            tx.SetWorkingDir(DstPath);
            auto* sequence = tx.MutableSequence();
            sequence->SetName(column.name() == NTableIndex::NFulltext::RowIdColumn
                ? TString(NTableIndex::NFulltext::RowIdSequenceName) : SplitPath(TString(source.name())).back());
            sequence->SetRestart(true);
            sequence->MutableSetVal()->SetNextValue(source.set_val().next_value());
            sequence->MutableSetVal()->SetNextUsed(source.set_val().next_used());
        }
        AllocateTxId();
    }

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

        if (NextSequence < SequenceAlters.size()) {
            tx.CopyFrom(SequenceAlters[NextSequence]);
            Send(PipeCache, new TEvPipeCache::TEvForward(ev.Release(), SchemeShardId, true));
            return Become(&TThis::StateAlterDst);
        }

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
            if (NextSequence < SequenceAlters.size()) {
                return Error(record.GetStatus(), record.GetReason());
            }
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

        if (NextSequence < SequenceAlters.size()) {
            ++NextSequence;
            return AllocateTxId();
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
            const TReplication::EState desiredState,
            const TActorId& proxy, const TString& srcPath, const TString& dstPath)
        : Parent(parent)
        , SchemeShardId(schemeShardId)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(kind)
        , DstPathId(dstPathId)
        , DesiredState(desiredState)
        , LogPrefix(CreateActorLogPrefix("DstAlterer", ReplicationId, TargetId))
        , YdbProxy(proxy)
        , SrcPath(srcPath)
        , DstPath(dstPath)
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
                    if (YdbProxy) {
                        return PrepareSequences();
                    }
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
    const TActorId YdbProxy;
    const TString SrcPath;
    const TString DstPath;
    TVector<NKikimrSchemeOp::TModifyScheme> SequenceAlters;
    size_t NextSequence = 0;
    static constexpr auto RetryInterval = TDuration::Seconds(10);

}; // TDstAlterer

IActor* CreateDstAlterer(TReplication* replication, ui64 targetId, const TActorContext& ctx) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);
    bool needsSequences = target->GetConfig()->IsSrcIndexTable();
    if (target->GetKind() == TReplication::ETargetKind::Table) {
        for (const auto* peer : replication->GetTargets()) {
            needsSequences |= peer->GetKind() == TReplication::ETargetKind::IndexTable
                && replication->FindBaseTableTarget(*peer) == target
                && peer->GetConfig()->IsRowIdIndex();
        }
    }
    return CreateDstAlterer(ctx.SelfID, replication->GetSchemeShardId(),
        replication->GetId(), target->GetId(), target->GetKind(), target->GetDstPathId(), replication->GetDesiredState(),
        needsSequences ? replication->GetYdbProxy() : TActorId{}, target->GetSrcPath(), target->GetDstPath());
}

IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
        ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId, TReplication::EState desiredState,
        const TActorId& proxy, const TString& srcPath, const TString& dstPath)
{
    return new TDstAlterer(parent, schemeShardId, rid, tid, kind, dstPathId, desiredState, proxy, srcPath, dstPath);
}

}
