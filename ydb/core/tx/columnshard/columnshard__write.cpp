#include "columnshard_impl.h"

#include "blobs_action/transaction/tx_blobs_written.h"
#include "blobs_action/transaction/tx_draft.h"
#include "common/limits.h"
#include "counters/columnshard.h"
#include "engines/column_engine_logs.h"
#include "operations/batch_builder/builder.h"
#include "operations/manager.h"
#include "operations/write_data.h"
#include "transactions/operators/ev_write/primary.h"
#include "transactions/operators/ev_write/secondary.h"
#include "transactions/operators/ev_write/sync.h"

#include <ydb/core/tx/columnshard/tablet/write_queue.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/data_events/events.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

using namespace NTabletFlatExecutor;

void TColumnShard::OverloadWriteFail(const EOverloadStatus overloadReason, const NEvWrite::TWriteMeta& writeMeta, const ui64 writeSize,
    const ui64 cookie, std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx) {
    Counters.GetTabletCounters()->IncCounter(COUNTER_WRITE_FAIL);
    Counters.GetCSCounters().OnWriteOverload(overloadReason, writeSize);
    switch (overloadReason) {
        case EOverloadStatus::Disk:
            Counters.OnWriteOverloadDisk();
            break;
        case EOverloadStatus::OverloadMetadata:
            Counters.OnWriteOverloadMetadata(writeSize);
            break;
        case EOverloadStatus::OverloadCompaction:
            Counters.OnWriteOverloadCompaction(writeSize);
            break;
        case EOverloadStatus::ShardTxInFly:
            Counters.OnWriteOverloadShardTx(writeSize);
            break;
        case EOverloadStatus::ShardWritesInFly:
            Counters.OnWriteOverloadShardWrites(writeSize);
            break;
        case EOverloadStatus::ShardWritesSizeInFly:
            Counters.OnWriteOverloadShardWritesSize(writeSize);
            break;
        case EOverloadStatus::None:
            Y_ABORT("invalid function usage");
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "write_overload")("size", writeSize)("path_id", writeMeta.GetPathId())(
        "reason", overloadReason);

    ctx.Send(writeMeta.GetSource(), event.release(), 0, cookie);
}

TColumnShard::EOverloadStatus TColumnShard::CheckOverloadedWait(const TInternalPathId pathId) const {
    Counters.GetCSCounters().OnIndexMetadataLimit(NOlap::IColumnEngine::GetMetadataLimit());
    if (TablesManager.GetPrimaryIndex()) {
        if (TablesManager.GetPrimaryIndex()->IsOverloadedByMetadata(NOlap::IColumnEngine::GetMetadataLimit())) {
            return EOverloadStatus::OverloadMetadata;
        }
        if (TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>()
                .GetGranuleVerified(pathId)
                .GetOptimizerPlanner()
                .IsOverloaded()) {
            return EOverloadStatus::OverloadCompaction;
        }
    }
    return EOverloadStatus::None;
}

void TColumnShard::Handle(NPrivateEvents::NWrite::TEvWritePortionResult::TPtr& ev, const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TEvWritePortionResult");
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletID())("event", "TEvWritePortionResult");
    TInsertedPortions writtenData = ev->Get()->DetachInsertedData();
    if (ev->Get()->GetWriteStatus() == NKikimrProto::OK) {
        const TMonotonic now = TMonotonic::Now();
        for (auto&& i : writtenData.GetWriteResults()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("writing_size", i.GetDataSize())("event", "data_write_finished")(
                "writing_id", i.GetWriteMeta().GetId());
            i.MutableWriteMeta().OnStage(NEvWrite::EWriteStage::SuccessWritingToLocalDB);
            if (i.GetWriteMeta().IsBulk()) {
                Counters.OnWritePutBulkBlobsSuccess(now - i.GetWriteMeta().GetWriteStartInstant(), i.GetRecordsCount());
            } else {
                Counters.OnWritePutBlobsSuccess(now - i.GetWriteMeta().GetWriteStartInstant(), i.GetRecordsCount());
            }
            Counters.GetWritesMonitor()->OnFinishWrite(i.GetDataSize(), 1);
        }
        Execute(new TTxBlobsWritingFinished(this, ev->Get()->GetWriteStatus(), ev->Get()->GetWriteAction(), std::move(writtenData)), ctx);
    } else {
        const TMonotonic now = TMonotonic::Now();
        for (auto&& i : writtenData.GetWriteResults()) {
            i.MutableWriteMeta().OnStage(NEvWrite::EWriteStage::FailWritingToLocalDB);
            if (i.GetWriteMeta().IsBulk()) {
                Counters.OnWritePutBulkBlobsFailed(now - i.GetWriteMeta().GetWriteStartInstant(), i.GetRecordsCount());
            } else {
                Counters.OnWritePutBlobsFailed(now - i.GetWriteMeta().GetWriteStartInstant(), i.GetRecordsCount());
            }
            Counters.GetCSCounters().OnWritePutBlobsFail(now - i.GetWriteMeta().GetWriteStartInstant());
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("writing_size", i.GetDataSize())("event", "data_write_error")(
                "writing_id", i.GetWriteMeta().GetId())("reason", i.GetErrorMessage());
            Counters.GetWritesMonitor()->OnFinishWrite(i.GetDataSize(), 1);
        }

        Execute(new TTxBlobsWritingFailed(this, std::move(writtenData)), ctx);
    }

    UpdateOverloadsStatus();
}

void TColumnShard::Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& ev, const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletID())("event", "TEvWriteBlobsResult");

    auto& putResult = ev->Get()->GetPutResult();
    AFL_VERIFY(putResult.GetPutStatus() != NKikimrProto::OK);
    OnYellowChannels(putResult);
    NOlap::TWritingBuffer& wBuffer = ev->Get()->MutableWritesBuffer();
    auto baseAggregations = wBuffer.GetAggregations();
    wBuffer.InitReplyReceived(TMonotonic::Now());

    for (auto&& aggr : baseAggregations) {
        const auto& writeMeta = aggr->GetWriteMeta();
        aggr->MutableWriteMeta().OnStage(NEvWrite::EWriteStage::Aborted);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "blobs_write_finished")("writing_size", aggr->GetSize())(
            "writing_id", writeMeta.GetId())("status", putResult.GetPutStatus());
        Counters.GetWritesMonitor()->OnFinishWrite(aggr->GetSize(), 1);

        Counters.GetCSCounters().OnWritePutBlobsFail(TMonotonic::Now() - writeMeta.GetWriteStartInstant());
        Counters.GetTabletCounters()->IncCounter(COUNTER_WRITE_FAIL);

        AFL_VERIFY(!writeMeta.HasLongTxId());
        auto operation = OperationsManager->GetOperationVerified((TOperationWriteId)writeMeta.GetWriteId());
        LWPROBE(EvWriteResult, TabletID(), writeMeta.GetSource().ToString(), 0, operation->GetCookie(), "write_blob_result", false, ev->Get()->GetErrorMessage() ? ev->Get()->GetErrorMessage() : "put data fails");
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), operation->GetLockId(), ev->Get()->GetWriteResultStatus(),
            ev->Get()->GetErrorMessage() ? ev->Get()->GetErrorMessage() : "put data fails");
        ctx.Send(writeMeta.GetSource(), result.release(), 0, operation->GetCookie());
        Counters.GetCSCounters().OnFailedWriteResponse(EWriteFailReason::PutBlob);
        wBuffer.RemoveData(aggr, StoragesManager->GetInsertOperator());
    }
    AFL_VERIFY(wBuffer.IsEmpty());

    UpdateOverloadsStatus();
}

void TColumnShard::Handle(TEvPrivate::TEvWriteDraft::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWriteDraft(this, ev->Get()->WriteController), ctx);
}

class TCommitOperation {
private:
    const ui64 TabletId;

public:
    using TPtr = std::shared_ptr<TCommitOperation>;

    bool NeedSyncLocks() const {
        return SendingShards.size() || ReceivingShards.size();
    }

    bool IsPrimary() const {
        AFL_VERIFY(NeedSyncLocks());
        return TabletId == ArbiterColumnShard;
    }

    TCommitOperation(const ui64 tabletId)
        : TabletId(tabletId) {
    }

    TConclusionStatus Parse(const NEvents::TDataEvents::TEvWrite& evWrite) {
        TxId = evWrite.Record.GetTxId();
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("tx_id", TxId);
        const auto& locks = evWrite.Record.GetLocks();
        AFL_VERIFY(!locks.GetLocks().empty());
        auto& lock = locks.GetLocks()[0];
        LockId = lock.GetLockId();
        SendingShards = std::set<ui64>(locks.GetSendingShards().begin(), locks.GetSendingShards().end());
        ReceivingShards = std::set<ui64>(locks.GetReceivingShards().begin(), locks.GetReceivingShards().end());
        if (SendingShards.empty() != ReceivingShards.empty()) {
            return TConclusionStatus::Fail("incorrect synchronization data (send/receiving lists)");
        }
        if (ReceivingShards.size() && SendingShards.size()) {
            if (!ReceivingShards.contains(TabletId) && !SendingShards.contains(TabletId)) {
                return TConclusionStatus::Fail("current tablet_id is absent in sending and receiving lists");
            }
            if (!locks.HasArbiterColumnShard()) {
                return TConclusionStatus::Fail("no arbiter info in request");
            }
            ArbiterColumnShard = locks.GetArbiterColumnShard();

            if (IsPrimary() && !ReceivingShards.contains(ArbiterColumnShard)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "incorrect arbiter")("arbiter_id", ArbiterColumnShard)(
                    "receiving", JoinSeq(", ", ReceivingShards))("sending", JoinSeq(", ", SendingShards));
                return TConclusionStatus::Fail("arbiter is absent in receiving lists");
            }
            if (!IsPrimary() && (!ReceivingShards.contains(ArbiterColumnShard) || !SendingShards.contains(ArbiterColumnShard))) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "incorrect arbiter")("arbiter_id", ArbiterColumnShard)(
                    "receiving", JoinSeq(", ", ReceivingShards))("sending", JoinSeq(", ", SendingShards));
                return TConclusionStatus::Fail("arbiter is absent in sending or receiving lists");
            }
        }

        Generation = lock.GetGeneration();
        InternalGenerationCounter = lock.GetCounter();
        if (!GetLockId()) {
            return TConclusionStatus::Fail("not initialized lock info in commit message");
        }
        if (!TxId) {
            return TConclusionStatus::Fail("not initialized TxId for commit event");
        }
        if (locks.GetOp() != NKikimrDataEvents::TKqpLocks::Commit) {
            return TConclusionStatus::Fail("incorrect message type");
        }
        return TConclusionStatus::Success();
    }

    std::unique_ptr<NColumnShard::TEvWriteCommitSyncTransactionOperator> CreateTxOperator(
        const NKikimrTxColumnShard::ETransactionKind kind) const {
        if (IsPrimary()) {
            return std::make_unique<NColumnShard::TEvWriteCommitPrimaryTransactionOperator>(
                TFullTxInfo::BuildFake(kind), LockId, ReceivingShards, SendingShards);
        } else {
            return std::make_unique<NColumnShard::TEvWriteCommitSecondaryTransactionOperator>(
                TFullTxInfo::BuildFake(kind), LockId, ArbiterColumnShard, ReceivingShards.contains(TabletId));
        }
    }

private:
    YDB_READONLY(ui64, LockId, 0);
    YDB_READONLY(ui64, Generation, 0);
    YDB_READONLY(ui64, InternalGenerationCounter, 0);
    YDB_READONLY(ui64, TxId, 0);
    YDB_READONLY_DEF(std::set<ui64>, SendingShards);
    YDB_READONLY_DEF(std::set<ui64>, ReceivingShards);
    ui64 ArbiterColumnShard = 0;
};

class TProposeWriteTransaction: public TExtendedTransactionBase {
private:
    using TBase = TExtendedTransactionBase;

public:
    TProposeWriteTransaction(TColumnShard* self, TCommitOperation::TPtr op, const TActorId source, const ui64 cookie)
        : TBase(self, "TProposeWriteTransaction")
        , WriteCommit(op)
        , Source(source)
        , Cookie(cookie) {
    }

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        NKikimrTxColumnShard::TCommitWriteTxBody proto;
        NKikimrTxColumnShard::ETransactionKind kind;
        if (WriteCommit->NeedSyncLocks()) {
            if (WriteCommit->IsPrimary()) {
                kind = NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE_PRIMARY;
            } else {
                kind = NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE_SECONDARY;
            }
            proto = WriteCommit->CreateTxOperator(kind)->SerializeToProto();
        } else {
            kind = NKikimrTxColumnShard::TX_KIND_COMMIT_WRITE;
        }
        proto.SetLockId(WriteCommit->GetLockId());
        TxOperator = Self->GetProgressTxController().StartProposeOnExecute(
            TTxController::TTxInfo(kind, WriteCommit->GetTxId(), Source, Self->GetProgressTxController().GetAllowedStep(), Cookie, {}),
            proto.SerializeAsString(), txc);
        return true;
    }

    virtual void DoComplete(const TActorContext& ctx) override {
        Self->GetProgressTxController().FinishProposeOnComplete(WriteCommit->GetTxId(), ctx);
    }
    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE;
    }

private:
    TCommitOperation::TPtr WriteCommit;
    TActorId Source;
    ui64 Cookie;
    std::shared_ptr<TTxController::ITransactionOperator> TxOperator;
};

class TAbortWriteTransaction: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;

public:
    TAbortWriteTransaction(TColumnShard* self, const ui64 txId, const TActorId source, const ui64 cookie)
        : TBase(self)
        , TxId(txId)
        , Source(source)
        , Cookie(cookie) {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Self->GetOperationsManager().AbortTransactionOnExecute(*Self, TxId, txc);
        return true;
    }

    virtual void Complete(const TActorContext& ctx) override {
        LWPROBE(EvWriteResult, Self->TabletID(), Source.ToString(), TxId, Cookie, "abort", true, "");
        Self->GetOperationsManager().AbortTransactionOnComplete(*Self, TxId);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildCompleted(Self->TabletID(), TxId);
        ctx.Send(Source, result.release(), 0, Cookie);
    }
    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE;
    }

private:
    ui64 TxId;
    TActorId Source;
    ui64 Cookie;
};

void TColumnShard::Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    TMemoryProfileGuard mpg("NEvents::TDataEvents::TEvWrite");
    NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_WRITE)("tablet_id", TabletID())("event", "TEvWrite");

    const auto& record = ev->Get()->Record;
    const auto source = ev->Sender;
    const auto cookie = ev->Cookie;


    std::optional<TDuration> writeTimeout;
    if (record.HasTimeoutSeconds()) {
        writeTimeout = TDuration::Seconds(record.GetTimeoutSeconds());
    }

    if (!TablesManager.GetPrimaryIndex()) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "schema not ready for writing");
        Counters.GetTabletCounters()->IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
            TabletID(), 0, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "schema not ready for writing");
        ctx.Send(source, result.release(), 0, cookie);
        return;
    }

    const auto behaviourConclusion = TOperationsManager::GetBehaviour(*ev->Get());
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_WRITE)("ev_write", record.DebugString());
    if (behaviourConclusion.IsFail()) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "invalid write event: " + behaviourConclusion.GetErrorMessage());
        Counters.GetTabletCounters()->IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), 0, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
            "invalid write event: " + behaviourConclusion.GetErrorMessage());
        ctx.Send(source, result.release(), 0, cookie);
        return;
    }
    auto behaviour = *behaviourConclusion;

    if (behaviour == EOperationBehaviour::AbortWriteLock) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "AbortWriteLock", true, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED), "");
        Execute(new TAbortWriteTransaction(this, record.GetLocks().GetLocks()[0].GetLockId(), source, cookie), ctx);
        return;
    }

    const auto sendError = [&](const TString& message, const NKikimrDataEvents::TEvWriteResult::EStatus status) {
        Counters.GetTabletCounters()->IncCounter(COUNTER_WRITE_FAIL);
        LWPROBE(EvWriteResult, TabletID(), source.ToString(), record.GetTxId(), cookie, "immediate error", false, message);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(TabletID(), record.GetTxId(), status, message);
        ctx.Send(source, result.release(), 0, cookie);
    };
    if (behaviour == EOperationBehaviour::CommitWriteLock) {
        auto commitOperation = std::make_shared<TCommitOperation>(TabletID());
        auto conclusionParse = commitOperation->Parse(*ev->Get());
        if (conclusionParse.IsFail()) {
            LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "CommitWriteLock", true, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), conclusionParse.GetErrorMessage());
            sendError(conclusionParse.GetErrorMessage(), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        } else {
            auto* lockInfo = OperationsManager->GetLockOptional(commitOperation->GetLockId());
            if (!lockInfo) {
                LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "CommitWriteLock", true, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "haven't lock for commit: " + ::ToString(commitOperation->GetLockId()));
                sendError("haven't lock for commit: " + ::ToString(commitOperation->GetLockId()),
                    NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            } else {
                if (commitOperation->NeedSyncLocks()) {
                    if (lockInfo->GetGeneration() != commitOperation->GetGeneration()) {
                        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "CommitWriteLock", true, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN), "tablet lock have another generation: " + ::ToString(lockInfo->GetGeneration()) + " != " + ::ToString(commitOperation->GetGeneration()));
                        sendError("tablet lock have another generation: " + ::ToString(lockInfo->GetGeneration()) +
                                      " != " + ::ToString(commitOperation->GetGeneration()),
                            NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
                    } else if (lockInfo->GetInternalGenerationCounter() != commitOperation->GetInternalGenerationCounter()) {
                        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "CommitWriteLock", true, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN), "tablet lock have another internal generation counter: " + ::ToString(lockInfo->GetInternalGenerationCounter()) + " != " + ::ToString(commitOperation->GetInternalGenerationCounter()));
                        sendError(
                            "tablet lock have another internal generation counter: " + ::ToString(lockInfo->GetInternalGenerationCounter()) +
                                " != " + ::ToString(commitOperation->GetInternalGenerationCounter()),
                            NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
                    } else {
                        Execute(new TProposeWriteTransaction(this, commitOperation, source, cookie), ctx);
                    }
                } else {
                    Execute(new TProposeWriteTransaction(this, commitOperation, source, cookie), ctx);
                }
            }
        }
        return;
    }

    if (record.GetOperations().size() != 1) {
        LWPROBE(EvWriteResult, TabletID(), source.ToString(), record.GetTxId(), cookie, "immediate error", false, "only single operation is supported");
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, false, ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "only single operation is supported");
        Counters.GetTabletCounters()->IncCounter(COUNTER_WRITE_FAIL);
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
            TabletID(), 0, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "only single operation is supported");
        ctx.Send(source, result.release(), 0, cookie);
        return;
    }

    const auto& operation = record.GetOperations()[0];
    const std::optional<NEvWrite::EModificationType> mType =
        TEnumOperator<NEvWrite::EModificationType>::DeserializeFromProto(operation.GetType());
    if (!mType) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "operation " + NKikimrDataEvents::TEvWrite::TOperation::EOperationType_Name(operation.GetType()) + " is not supported");
        sendError("operation " + NKikimrDataEvents::TEvWrite::TOperation::EOperationType_Name(operation.GetType()) + " is not supported",
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        return;
    }

    if (!operation.GetTableId().HasSchemaVersion()) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "schema version not set");
        sendError("schema version not set", NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        return;
    }

    auto schema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchemaOptional(operation.GetTableId().GetSchemaVersion());
    if (!schema) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "unknown schema version");
        sendError("unknown schema version", NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        return;
    }

    const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(operation.GetTableId());
    const auto& internalPathId = TablesManager.ResolveInternalPathId(schemeShardLocalPathId, false);
    if (!internalPathId) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "unknown table");
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "unknown_table")("path_id", schemeShardLocalPathId);
        sendError("unknown table", NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        return;
    }
    const auto& pathId = TUnifiedPathId::BuildValid(*internalPathId, schemeShardLocalPathId);
    if (!TablesManager.IsReadyForStartWrite(*internalPathId, false)) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), 0, "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR), "unknown schema version");
        sendError("table not writable", NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR);
        return;
    }

    Counters.GetColumnTablesCounters()->GetPathIdCounter(*internalPathId)->OnWriteEvent();

    auto arrowData = std::make_shared<TArrowData>(schema);
    if (!arrowData->Parse(operation, NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite>(*ev->Get()))) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), arrowData->GetSize(), "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST), "parsing data error");
        sendError("parsing data error", NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        return;
    }

    if (!AppDataVerified().ColumnShardConfig.GetWritingEnabled()) {
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), arrowData->GetSize(), "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED), "writing disabled");
        sendError("writing disabled", NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED);
        return;
    }

    const bool outOfSpace = SpaceWatcher->SubDomainOutOfSpace && (*mType != NEvWrite::EModificationType::Delete);
    if (outOfSpace) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_writing")("reason", "quota_exceeded")("source", "dataevent");
    }
    auto overloadStatus = outOfSpace ? EOverloadStatus::Disk : CheckOverloadedImmediate(*internalPathId);
    if (overloadStatus != EOverloadStatus::None) {
        LWPROBE(EvWriteResult, TabletID(), source.ToString(), record.GetTxId(), cookie, "immediate error", false, "overload data error");
        LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), arrowData->GetSize(), "", false, operation.GetIsBulk(), ToString(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED), "overload data error " + ToString(overloadStatus));
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
            TabletID(), 0, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED, "overload data error");

        if (!outOfSpace && record.HasOverloadSubscribe()) {
            const auto rejectReasons = NOverload::MakeRejectReasons(overloadStatus);
            OverloadSubscribers.SetOverloadSubscribed(record.GetOverloadSubscribe(), ev->Recipient, ev->Sender, rejectReasons, result->Record);
        }
        OverloadWriteFail(overloadStatus,
            NEvWrite::TWriteMeta(0, pathId, source, {}, TGUID::CreateTimebased().AsGuidString(),
                Counters.GetCSCounters().WritingCounters->GetWriteFlowCounters()),
            arrowData->GetSize(), cookie, std::move(result), ctx);
        return;
    }

    std::optional<ui32> granuleShardingVersionId;
    if (record.HasGranuleShardingVersionId()) {
        granuleShardingVersionId = record.GetGranuleShardingVersionId();
    }

    ui64 lockId = 0;
    if (behaviour == EOperationBehaviour::NoTxWrite) {
        lockId = BuildEphemeralTxId();
    } else {
        lockId = record.GetLockTxId();
    }

    const bool isBulk = operation.HasIsBulk() && operation.GetIsBulk();

    LWPROBE(EvWrite, TabletID(), source.ToString(), cookie, record.GetTxId(), writeTimeout.value_or(TDuration::Max()), arrowData->GetSize(), "", true, operation.GetIsBulk(), "", "");
    Counters.GetWritesMonitor()->OnStartWrite(arrowData->GetSize());
    WriteTasksQueue->Enqueue(TWriteTask(
        arrowData, schema, source, granuleShardingVersionId, pathId, cookie, lockId, *mType, behaviour, writeTimeout, record.GetTxId(), isBulk));
    WriteTasksQueue->Drain(false, ctx);
}

}   // namespace NKikimr::NColumnShard
