#include "defs.h"

#include "datashard_active_transaction.h"
#include "datashard_kqp.h"
#include "datashard_locks.h"
#include "datashard_impl.h"
#include "datashard_failpoints.h"
#include "key_conflicts.h"

#include <ydb/library/actors/util/memory_track.h>

namespace NKikimr {
namespace NDataShard {

TValidatedDataTx::TValidatedDataTx(TDataShard *self,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx,
                                   const TStepOrder &stepTxId,
                                   TInstant receivedAt,
                                   const TString &txBody,
                                   bool usesMvccSnapshot)
    : StepTxId_(stepTxId)
    , TabletId_(self->TabletID())
    , TxBody(txBody)
    , EngineBay(self, txc, ctx, stepTxId.ToPair())
    , ErrCode(NKikimrTxDataShard::TError::OK)
    , TxSize(0)
    , TxCacheUsage(0)
    , IsReleased(false)
    , BuiltTaskRunner(false)
    , IsReadOnly(true)
    , AllowCancelROwithReadsets(self->AllowCancelROwithReadsets())
    , Cancelled(false)
    , ReceivedAt_(receivedAt)
{
    bool success = Tx.ParseFromArray(TxBody.data(), TxBody.size());
    if (!success) {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = "Failed to parse TxBody";
        return;
    }

    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);

    Y_ABORT_UNLESS(Tx.HasMiniKQL() || Tx.HasReadTableTransaction() || Tx.HasKqpTransaction(),
             "One of the fields should be set: MiniKQL, ReadTableTransaction, KqpTransaction");

    if (Tx.GetLockTxId())
        EngineBay.SetLockTxId(Tx.GetLockTxId(), Tx.GetLockNodeId());

    if (Tx.GetImmediate())
        EngineBay.SetIsImmediateTx();

    if (usesMvccSnapshot)
        EngineBay.SetIsRepeatableSnapshot();

    if (Tx.HasReadTableTransaction()) {
        auto &tx = Tx.GetReadTableTransaction();
        if (self->TableInfos.contains(tx.GetTableId().GetTableId())) {
            auto* info = self->TableInfos[tx.GetTableId().GetTableId()].Get();
            Y_ABORT_UNLESS(info, "Unexpected missing table info");
            TSerializedTableRange range(tx.GetRange());
            EngineBay.AddReadRange(TTableId(tx.GetTableId().GetOwnerId(),
                                            tx.GetTableId().GetTableId()),
                                   {}, range.ToTableRange(), info->KeyColumnTypes);
        } else {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = "Trying to read from table that doesn't exist";
        }
    } else if (IsKqpTx()) {
        if (Y_UNLIKELY(!IsKqpDataTx())) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Unexpected KQP transaction type, shard: " << TabletId()
                << ", txid: " << StepTxId_.TxId << ", tx: " << Tx.DebugString());
            ErrCode = NKikimrTxDataShard::TError::BAD_TX_KIND;
            ErrStr = TStringBuilder() << "Unexpected KQP transaction type: "
                << NKikimrTxDataShard::EKqpTransactionType_Name(Tx.GetKqpTransaction().GetType()) << ".";
            return;
        }

        auto& typeRegistry = *AppData()->TypeRegistry;
        auto& computeCtx = EngineBay.GetKqpComputeCtx();

        try {
            bool hasPersistentChannels = false;
            if (!KqpValidateTransaction(GetTasks(), Immediate(), StepTxId_.TxId, ctx, hasPersistentChannels)) {
                LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP transaction validation failed, datashard: "
                    << TabletId() << ", txid: " << StepTxId_.TxId);
                ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
                ErrStr = "Transaction validation failed.";
                return;
            }
            computeCtx.SetHasPersistentChannels(hasPersistentChannels);

            for (auto& task : GetTasks()) {
                NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta meta;
                if (!task.GetMeta().UnpackTo(&meta)) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP transaction validation failed"
                        << ", datashard: " << TabletId()
                        << ", txid: " << StepTxId_.TxId
                        << ", failed to load task meta: " << task.GetMeta().value());
                    ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
                    ErrStr = "Transaction validation failed: invalid task metadata.";
                    return;
                }

                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TxId: " << StepTxId_.TxId << ", shard " << TabletId()
                    << ", task: " << task.GetId() << ", meta: " << meta.ShortDebugString());

                auto& tableMeta = meta.GetTable();

                auto tableInfoPtr = self->TableInfos.FindPtr(tableMeta.GetTableId().GetTableId());
                if (!tableInfoPtr) {
                    ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
                    ErrStr = TStringBuilder() << "Table '" << tableMeta.GetTablePath() << "' doesn't exist";
                    return;
                }
                auto tableInfo = tableInfoPtr->Get();
                YQL_ENSURE(tableInfo);

                if (tableInfo->GetTableSchemaVersion() != 0 &&
                    tableMeta.GetSchemaVersion() != tableInfo->GetTableSchemaVersion())
                {
                    ErrCode = NKikimrTxDataShard::TError::SCHEME_CHANGED;
                    ErrStr = TStringBuilder() << "Table '" << tableMeta.GetTablePath() << "' scheme changed.";
                    return;
                }

                for (auto& read : meta.GetReads()) {
                    for (auto& column : read.GetColumns()) {
                        if (tableInfo->Columns.contains(column.GetId()) || IsSystemColumn(column.GetName())) {
                            // ok
                        } else {
                            ErrCode = NKikimrTxDataShard::TError::SCHEME_CHANGED;
                            ErrStr = TStringBuilder() << "Table '" << tableMeta.GetTablePath() << "' scheme changed:"
                                << " column '" << column.GetName() << "' not found.";
                            return;
                        }
                    }
                }

                KqpSetTxKeys(TabletId(), task.GetId(), tableInfo, meta, typeRegistry, ctx, EngineBay);

                for (auto& output : task.GetOutputs()) {
                    for (auto& channel : output.GetChannels()) {
                        computeCtx.SetTaskOutputChannel(task.GetId(), channel.GetId(),
                            ActorIdFromProto(channel.GetDstEndpoint().GetActorId()));
                    }
                }
            }

            if (Tx.HasPerShardKeysSizeLimitBytes()) {
                PerShardKeysSizeLimitBytes_ = Tx.GetPerShardKeysSizeLimitBytes();
            }

            IsReadOnly = IsReadOnly && Tx.GetReadOnly();

            KqpSetTxLocksKeys(GetKqpLocks(), self->SysLocksTable(), EngineBay);
            EngineBay.MarkTxLoaded();

            auto& tasksRunner = GetKqpTasksRunner(); // create tasks runner, can throw TMemoryLimitExceededException

            auto allocGuard = tasksRunner.BindAllocator(100_MB); // set big enough limit, decrease/correct later

            auto execCtx = DefaultKqpExecutionContext();
            tasksRunner.Prepare(DefaultKqpDataReqMemoryLimits(), *execCtx);
        } catch (const TMemoryLimitExceededException&) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Not enough memory to create tasks runner, datashard: "
                << TabletId() << ", txid: " << StepTxId_.TxId);
            ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
            ErrStr = TStringBuilder() << "Transaction validation failed: not enough memory.";
            return;
        } catch (const yexception& e) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Exception while validating KQP transaction, datashard: "
                << TabletId() << ", txid: " << StepTxId_.TxId << ", error: " << e.what());
            ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
            ErrStr = TStringBuilder() << "Transaction validation failed: " << e.what() << ".";
            return;
        }
    } else {
        Y_ABORT_UNLESS(Tx.HasMiniKQL());
        if (Tx.GetLlvmRuntime()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Using LLVM runtime to execute transaction: " << StepTxId_.TxId);
            EngineBay.SetUseLlvmRuntime(true);
        }
        if (Tx.HasPerShardKeysSizeLimitBytes()) {
            PerShardKeysSizeLimitBytes_ = Tx.GetPerShardKeysSizeLimitBytes();
        }

        IsReadOnly = IsReadOnly && Tx.GetReadOnly();

        auto engine = EngineBay.GetEngine();
        auto result = engine->AddProgram(TabletId_, Tx.GetMiniKQL(), Tx.GetReadOnly());

        ErrStr = engine->GetErrors();
        ErrCode = ConvertErrCode(result);
    }

    ComputeDeadline();
}

TValidatedDataTx::~TValidatedDataTx() {
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
}

ui32 TValidatedDataTx::ExtractKeys(bool allowErrors)
{
    using EResult = NMiniKQL::IEngineFlat::EResult;

    EResult result = EngineBay.Validate();
    if (allowErrors) {
        if (result != EResult::Ok) {
            ErrStr = EngineBay.GetEngine()->GetErrors();
            ErrCode = ConvertErrCode(result);
            return 0;
        }
    } else {
        Y_ABORT_UNLESS(result == EResult::Ok, "Engine errors: %s", EngineBay.GetEngine()->GetErrors().data());
    }
    return KeysCount();
}

bool TValidatedDataTx::ReValidateKeys()
{
    using EResult = NMiniKQL::IEngineFlat::EResult;

    if (IsKqpTx()) {
        auto [result, error] = EngineBay.GetKqpComputeCtx().ValidateKeys(EngineBay.TxInfo());
        if (result != EResult::Ok) {
            ErrStr = std::move(error);
            ErrCode = ConvertErrCode(result);
            return false;
        }
    } else {
        EResult result = EngineBay.ReValidateKeys();
        if (result != EResult::Ok) {
            ErrStr = EngineBay.GetEngine()->GetErrors();
            ErrCode = ConvertErrCode(result);
            return false;
        }
    }

    return true;
}

bool TValidatedDataTx::CanCancel() {
    if (!IsTxReadOnly()) {
        return false;
    }

    if (!AllowCancelROwithReadsets) {
        if (HasOutReadsets() || HasInReadsets()) {
            return false;
        }
    }

    return true;
}

bool TValidatedDataTx::CheckCancelled() {
    if (Cancelled) {
        return true;
    }

    if (!CanCancel()) {
        return false;
    }

    TInstant now = AppData()->TimeProvider->Now();
    Cancelled = (now >= Deadline());

    Cancelled = Cancelled || gCancelTxFailPoint.Check(TabletId(), TxId());

    if (Cancelled) {
        LOG_NOTICE_S(*TlsActivationContext->ExecutorThread.ActorSystem, NKikimrServices::TX_DATASHARD,
            "CANCELLED TxId " << TxId() << " at " << TabletId());
    }
    return Cancelled;
}

void TValidatedDataTx::ReleaseTxData() {
    TxBody = "";
    auto lock = Tx.GetLockTxId();
    auto lockNode = Tx.GetLockNodeId();
    Tx.Clear();
    if (lock) {
        Tx.SetLockTxId(lock);
    }
    if (lockNode) {
        Tx.SetLockNodeId(lockNode);
    }
    EngineBay.DestroyEngine();
    IsReleased = true;

    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);
}

void TValidatedDataTx::ComputeTxSize() {
    TxSize = sizeof(TValidatedDataTx);
    TxSize += TxBody.size();
    TxSize += Tx.ByteSize();
}

void TValidatedDataTx::ComputeDeadline() {
    Deadline_ = Tx.GetCancelDeadlineMs() ? TInstant::MilliSeconds(Tx.GetCancelDeadlineMs()) : TInstant::Max();
    if (ReceivedAt_ && Tx.GetCancelAfterMs()) {
        // If local timeout is specified in CancelAfterMs then take it into account as well
        Deadline_ = Min(Deadline_, ReceivedAt_ + TDuration::MilliSeconds(Tx.GetCancelAfterMs()));
    }
}

TActiveTransaction::~TActiveTransaction()
{
    UntrackMemory();
}

void TActiveTransaction::FillTxData(TValidatedDataTx::TPtr dataTx)
{
    Y_ABORT_UNLESS(!DataTx);
    Y_ABORT_UNLESS(TxBody.empty() || HasVolatilePrepareFlag());

    Target = dataTx->Source();
    DataTx = dataTx;

    if (DataTx->HasStreamResponse())
        SetStreamSink(DataTx->GetSink());
}

void TActiveTransaction::FillTxData(TDataShard *self,
                                    TTransactionContext &txc,
                                    const TActorContext &ctx,
                                    const TActorId &target,
                                    const TString &txBody,
                                    const TVector<TSysTables::TLocksTable::TLock> &locks,
                                    ui64 artifactFlags)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!DataTx);
    Y_ABORT_UNLESS(TxBody.empty());

    Target = target;
    TxBody = txBody;
    if (locks.size()) {
        for (auto lock : locks)
            LocksCache().Locks[lock.LockId] = lock;
    }
    ArtifactFlags = artifactFlags;
    if (IsDataTx() || IsReadTable()) {
        Y_ABORT_UNLESS(!DataTx);
        BuildDataTx(self, txc, ctx);
        Y_ABORT_UNLESS(DataTx->Ready());

        if (DataTx->HasStreamResponse())
            SetStreamSink(DataTx->GetSink());
    } else if (IsSchemeTx()) {
        BuildSchemeTx();
    } else if (IsSnapshotTx()) {
        BuildSnapshotTx();
    } else if (IsDistributedEraseTx()) {
        BuildDistributedEraseTx();
    } else if (IsCommitWritesTx()) {
        BuildCommitWritesTx();
    }

    TrackMemory();
}

void TActiveTransaction::FillVolatileTxData(TDataShard *self,
                                            TTransactionContext &txc,
                                            const TActorContext &ctx)
{
    UntrackMemory();

    Y_ABORT_UNLESS(!DataTx);
    Y_ABORT_UNLESS(!TxBody.empty());

    if (IsDataTx() || IsReadTable()) {
        BuildDataTx(self, txc, ctx);
        Y_ABORT_UNLESS(DataTx->Ready());

        if (DataTx->HasStreamResponse())
            SetStreamSink(DataTx->GetSink());
    } else if (IsSnapshotTx()) {
        BuildSnapshotTx();
    } else {
        Y_ABORT("Unexpected FillVolatileTxData call");
    }

    TrackMemory();
}

TValidatedDataTx::TPtr TActiveTransaction::BuildDataTx(TDataShard *self,
                                                       TTransactionContext &txc,
                                                       const TActorContext &ctx)
{
    Y_ABORT_UNLESS(IsDataTx() || IsReadTable());
    if (!DataTx) {
        Y_ABORT_UNLESS(TxBody);
        DataTx = std::make_shared<TValidatedDataTx>(self, txc, ctx, GetStepOrder(),
                                                    GetReceivedAt(), TxBody, MvccSnapshotRepeatable);
        if (DataTx->HasStreamResponse())
            SetStreamSink(DataTx->GetSink());
    }
    return DataTx;
}

bool TActiveTransaction::BuildSchemeTx()
{
    Y_ABORT_UNLESS(TxBody);
    SchemeTx.Reset(new NKikimrTxDataShard::TFlatSchemeTransaction);
    bool res = SchemeTx->ParseFromArray(TxBody.data(), TxBody.size());
    if (!res)
        return false;

    ui32 count = (ui32)SchemeTx->HasCreateTable()
        + (ui32)SchemeTx->HasDropTable()
        + (ui32)SchemeTx->HasAlterTable()
        + (ui32)SchemeTx->HasBackup()
        + (ui32)SchemeTx->HasRestore()
        + (ui32)SchemeTx->HasSendSnapshot()
        + (ui32)SchemeTx->HasCreatePersistentSnapshot()
        + (ui32)SchemeTx->HasDropPersistentSnapshot()
        + (ui32)SchemeTx->HasInitiateBuildIndex()
        + (ui32)SchemeTx->HasFinalizeBuildIndex()
        + (ui32)SchemeTx->HasDropIndexNotice()
        + (ui32)SchemeTx->HasMoveTable()
        + (ui32)SchemeTx->HasCreateCdcStreamNotice()
        + (ui32)SchemeTx->HasAlterCdcStreamNotice()
        + (ui32)SchemeTx->HasDropCdcStreamNotice()
        + (ui32)SchemeTx->HasMoveIndex();
    if (count != 1)
        return false;

    if (SchemeTx->HasCreateTable())
        SchemeTxType = TSchemaOperation::ETypeCreate;
    else if (SchemeTx->HasDropTable())
        SchemeTxType = TSchemaOperation::ETypeDrop;
    else if (SchemeTx->HasAlterTable())
        SchemeTxType = TSchemaOperation::ETypeAlter;
    else if (SchemeTx->HasBackup())
        SchemeTxType = TSchemaOperation::ETypeBackup;
    else if (SchemeTx->HasRestore())
        SchemeTxType = TSchemaOperation::ETypeRestore;
    else if (SchemeTx->HasSendSnapshot())
        SchemeTxType = TSchemaOperation::ETypeCopy;
    else if (SchemeTx->HasCreatePersistentSnapshot())
        SchemeTxType = TSchemaOperation::ETypeCreatePersistentSnapshot;
    else if (SchemeTx->HasDropPersistentSnapshot())
        SchemeTxType = TSchemaOperation::ETypeDropPersistentSnapshot;
    else if (SchemeTx->HasInitiateBuildIndex())
        SchemeTxType = TSchemaOperation::ETypeInitiateBuildIndex;
    else if (SchemeTx->HasFinalizeBuildIndex())
        SchemeTxType = TSchemaOperation::ETypeFinalizeBuildIndex;
    else if (SchemeTx->HasDropIndexNotice())
        SchemeTxType = TSchemaOperation::ETypeDropIndexNotice;
    else if (SchemeTx->HasMoveTable())
        SchemeTxType = TSchemaOperation::ETypeMoveTable;
    else if (SchemeTx->HasCreateCdcStreamNotice())
        SchemeTxType = TSchemaOperation::ETypeCreateCdcStream;
    else if (SchemeTx->HasAlterCdcStreamNotice())
        SchemeTxType = TSchemaOperation::ETypeAlterCdcStream;
    else if (SchemeTx->HasDropCdcStreamNotice())
        SchemeTxType = TSchemaOperation::ETypeDropCdcStream;
    else if (SchemeTx->HasMoveIndex())
        SchemeTxType = TSchemaOperation::ETypeMoveIndex;
    else
        SchemeTxType = TSchemaOperation::ETypeUnknown;

    return SchemeTxType != TSchemaOperation::ETypeUnknown;
}

bool TActiveTransaction::BuildSnapshotTx()
{
    Y_ABORT_UNLESS(TxBody);
    SnapshotTx.Reset(new NKikimrTxDataShard::TSnapshotTransaction);
    if (!SnapshotTx->ParseFromArray(TxBody.data(), TxBody.size())) {
        return false;
    }

    size_t count = (
        SnapshotTx->HasCreateVolatileSnapshot() +
        SnapshotTx->HasDropVolatileSnapshot());
    if (count != 1) {
        return false;
    }

    return true;
}

bool TDistributedEraseTx::TryParse(const TString& serialized) {
    if (!Body.ParseFromArray(serialized.data(), serialized.size())) {
        return false;
    }

    return true;
}

bool TActiveTransaction::BuildDistributedEraseTx() {
    Y_ABORT_UNLESS(TxBody);
    DistributedEraseTx.Reset(new TDistributedEraseTx);
    return DistributedEraseTx->TryParse(TxBody);
}

//

bool TCommitWritesTx::TryParse(const TString& serialized) {
    if (!Body.ParseFromArray(serialized.data(), serialized.size())) {
        return false;
    }

    return true;
}

bool TActiveTransaction::BuildCommitWritesTx() {
    Y_ABORT_UNLESS(TxBody);
    CommitWritesTx.Reset(new TCommitWritesTx);
    return CommitWritesTx->TryParse(TxBody);
}

//

void TActiveTransaction::ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase &provider,
                                       const TActorContext &ctx) {
    ReleasedTxDataSize = provider.GetMemoryLimit() + provider.GetRequestedMemory();

    if (!DataTx || DataTx->IsTxDataReleased())
        return;

    DataTx->ReleaseTxData();
    // Immediate transactions have no body stored.
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        UntrackMemory();
        TxBody.clear();
        TrackMemory();
    }

    //InReadSets.clear();
    OutReadSets().clear();
    LocksAccessLog().Locks.clear();
    LocksCache().Locks.clear();
    ArtifactFlags = 0;

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " released its data");
}

void TActiveTransaction::DbStoreLocksAccessLog(TDataShard * self,
                                               TTransactionContext &txc,
                                               const TActorContext &ctx)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);

    using TLocksVector = TVector<TSysTables::TLocksTable::TPersistentLock>;
    TLocksVector vec;
    vec.reserve(LocksAccessLog().Locks.size());
    for (auto &pr : LocksAccessLog().Locks)
        vec.emplace_back(pr.second);

    // Historically C++ column type was TVector<TLock>
    const char* vecDataStart = reinterpret_cast<const char*>(vec.data());
    size_t vecDataSize = vec.size() * sizeof(TLocksVector::value_type);
    TStringBuf vecData(vecDataStart, vecDataSize);
    db.Table<Schema::TxArtifacts>().Key(GetTxId())
        .Update(NIceDb::TUpdate<Schema::TxArtifacts::Locks>(vecData));

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Storing " << vec.size() << " locks for txid=" << GetTxId()
                << " in " << self->TabletID());
}

void TActiveTransaction::DbStoreArtifactFlags(TDataShard * self,
                                              TTransactionContext &txc,
                                              const TActorContext &ctx)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TxArtifacts>().Key(GetTxId())
        .Update<Schema::TxArtifacts::Flags>(ArtifactFlags);

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Storing artifactflags=" << ArtifactFlags << " for txid=" << GetTxId()
                << " in " << self->TabletID());
}

ui64 TActiveTransaction::GetMemoryConsumption() const {
    ui64 res = 0;
    if (DataTx) {
        res += DataTx->GetTxSize() + DataTx->GetMemoryAllocated();
    }
    return res;
}

ERestoreDataStatus TActiveTransaction::RestoreTxData(
        TDataShard *self,
        TTransactionContext &txc,
        const TActorContext &ctx)
{
    if (!DataTx) {
        ReleasedTxDataSize = 0;
        return ERestoreDataStatus::Ok;
    }

    UntrackMemory();

    // For immediate transactions we should restore just
    // from the TxBody. For planned transaction we should
    // restore from local database.
    TVector<TSysTables::TLocksTable::TLock> locks;
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        NIceDb::TNiceDb db(txc.DB);
        bool ok = self->TransQueue.LoadTxDetails(db, GetTxId(), Target, TxBody,
                                                 locks, ArtifactFlags);
        if (!ok) {
            TxBody.clear();
            ArtifactFlags = 0;
            return ERestoreDataStatus::Restart;
        }
    } else {
        Y_ABORT_UNLESS(TxBody);
    }

    TrackMemory();

    for (auto &lock : locks)
        LocksCache().Locks[lock.LockId] = lock;

    bool extractKeys = DataTx->IsTxInfoLoaded();
    DataTx = std::make_shared<TValidatedDataTx>(self, txc, ctx, GetStepOrder(),
                                                GetReceivedAt(), TxBody, MvccSnapshotRepeatable);
    if (DataTx->Ready() && extractKeys) {
        DataTx->ExtractKeys(true);
    }

    if (!DataTx->Ready()) {
        return ERestoreDataStatus::Error;
    }

    ReleasedTxDataSize = 0;

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " at "
                << self->TabletID() << " restored its data");

    return ERestoreDataStatus::Ok;
}

void TActiveTransaction::FinalizeDataTxPlan()
{
    Y_ABORT_UNLESS(IsDataTx());
    Y_ABORT_UNLESS(!IsImmediate());
    Y_ABORT_UNLESS(!IsKqpScanTransaction());

    TVector<EExecutionUnitKind> plan;

    plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
    if (IsKqpDataTransaction()) {
        plan.push_back(EExecutionUnitKind::BuildKqpDataTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareKqpDataTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::ExecuteKqpDataTx);
    } else {
        plan.push_back(EExecutionUnitKind::BuildDataTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareDataTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::ExecuteDataTx);
    }
    plan.push_back(EExecutionUnitKind::CompleteOperation);
    plan.push_back(EExecutionUnitKind::CompletedOperations);

    RewriteExecutionPlan(plan);
}

class TFinalizeDataTxPlanUnit : public TExecutionUnit {
public:
    TFinalizeDataTxPlanUnit(TDataShard &dataShard, TPipeline &pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeDataTxPlan, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override
    {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);

        TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());
        Y_VERIFY_S(tx->IsDataTx(), "unexpected non-data tx");

        if (auto dataTx = tx->GetDataTx()) {
            // Restore transaction type flags
            if (dataTx->IsKqpDataTx() && !tx->IsKqpDataTransaction())
                tx->SetKqpDataTransactionFlag();
            Y_VERIFY_S(!dataTx->IsKqpScanTx(), "unexpected kqp scan tx");
        }

        tx->FinalizeDataTxPlan();

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override
    {
        Y_UNUSED(op);
        Y_UNUSED(ctx);
    }
};

THolder<TExecutionUnit> CreateFinalizeDataTxPlanUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TFinalizeDataTxPlanUnit(dataShard, pipeline));
}

void TActiveTransaction::BuildExecutionPlan(bool loaded)
{
    Y_ABORT_UNLESS(GetExecutionPlan().empty());
    Y_ABORT_UNLESS(!IsKqpScanTransaction());

    TVector<EExecutionUnitKind> plan;
    if (IsDataTx()) {
        if (IsImmediate()) {
            Y_ABORT_UNLESS(!loaded);
            plan.push_back(EExecutionUnitKind::CheckDataTx);
            plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
            if (IsKqpDataTransaction()) {
                plan.push_back(EExecutionUnitKind::ExecuteKqpDataTx);
            } else {
                plan.push_back(EExecutionUnitKind::ExecuteDataTx);
            }
            plan.push_back(EExecutionUnitKind::FinishPropose);
            plan.push_back(EExecutionUnitKind::CompletedOperations);
        } else if (HasVolatilePrepareFlag()) {
            Y_ABORT_UNLESS(!loaded);
            plan.push_back(EExecutionUnitKind::CheckDataTx);
            plan.push_back(EExecutionUnitKind::StoreDataTx); // note: stores in memory
            plan.push_back(EExecutionUnitKind::FinishPropose);
            Y_ABORT_UNLESS(!GetStep());
            plan.push_back(EExecutionUnitKind::WaitForPlan);
            plan.push_back(EExecutionUnitKind::PlanQueue);
            plan.push_back(EExecutionUnitKind::LoadTxDetails); // note: reloads from memory
            plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
            Y_ABORT_UNLESS(IsKqpDataTransaction());
            // Note: execute will also prepare and send readsets
            plan.push_back(EExecutionUnitKind::ExecuteKqpDataTx);
            // Note: it is important that plan here is the same as regular
            // distributed tx, since normal tx may decide to commit in a
            // volatile manner with dependencies, to avoid waiting for
            // locked keys to resolve.
            plan.push_back(EExecutionUnitKind::CompleteOperation);
            plan.push_back(EExecutionUnitKind::CompletedOperations);
        } else {
            if (!loaded) {
                plan.push_back(EExecutionUnitKind::CheckDataTx);
                plan.push_back(EExecutionUnitKind::StoreDataTx);
                plan.push_back(EExecutionUnitKind::FinishPropose);
            }
            if (!GetStep())
                plan.push_back(EExecutionUnitKind::WaitForPlan);
            plan.push_back(EExecutionUnitKind::PlanQueue);
            plan.push_back(EExecutionUnitKind::LoadTxDetails);
            plan.push_back(EExecutionUnitKind::FinalizeDataTxPlan);
        }
    } else if (IsReadTable()) {
        if (IsImmediate()) {
            plan.push_back(EExecutionUnitKind::CheckDataTx);
        } else {
            if (!loaded) {
                plan.push_back(EExecutionUnitKind::CheckDataTx);
                plan.push_back(EExecutionUnitKind::StoreDataTx);
                plan.push_back(EExecutionUnitKind::FinishPropose);
            }
            if (!GetStep())
                plan.push_back(EExecutionUnitKind::WaitForPlan);
            plan.push_back(EExecutionUnitKind::PlanQueue);
            plan.push_back(EExecutionUnitKind::LoadTxDetails);
        }
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::MakeScanSnapshot);
        plan.push_back(EExecutionUnitKind::WaitForStreamClearance);
        plan.push_back(EExecutionUnitKind::ReadTableScan);
        if (IsImmediate()) {
            plan.push_back(EExecutionUnitKind::FinishPropose);
        } else {
            plan.push_back(EExecutionUnitKind::CompleteOperation);
        }
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else if (IsSnapshotTx()) {
        if (IsImmediate()) {
            plan.push_back(EExecutionUnitKind::CheckSnapshotTx);
        } else {
            if (!loaded) {
                plan.push_back(EExecutionUnitKind::CheckSnapshotTx);
                plan.push_back(EExecutionUnitKind::StoreSnapshotTx);
                plan.push_back(EExecutionUnitKind::FinishPropose);
            }
            if (!GetStep())
                plan.push_back(EExecutionUnitKind::WaitForPlan);
            plan.push_back(EExecutionUnitKind::PlanQueue);
            plan.push_back(EExecutionUnitKind::LoadTxDetails);
        }
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::CreateVolatileSnapshot);
        plan.push_back(EExecutionUnitKind::DropVolatileSnapshot);
        if (IsImmediate()) {
            plan.push_back(EExecutionUnitKind::FinishPropose);
        } else {
            plan.push_back(EExecutionUnitKind::CompleteOperation);
        }
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else if (IsDistributedEraseTx()) {
        if (!loaded) {
            plan.push_back(EExecutionUnitKind::CheckDistributedEraseTx);
            plan.push_back(EExecutionUnitKind::StoreDistributedEraseTx);
            plan.push_back(EExecutionUnitKind::FinishPropose);
        }
        if (!GetStep()) {
            plan.push_back(EExecutionUnitKind::WaitForPlan);
        }
        plan.push_back(EExecutionUnitKind::PlanQueue);
        plan.push_back(EExecutionUnitKind::LoadTxDetails);
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::BuildDistributedEraseTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareDistributedEraseTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::ExecuteDistributedEraseTx);
        plan.push_back(EExecutionUnitKind::CompleteOperation);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else if (IsCommitWritesTx()) {
        if (IsImmediate()) {
            Y_ABORT_UNLESS(!loaded);
            plan.push_back(EExecutionUnitKind::CheckCommitWritesTx);
            plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
            plan.push_back(EExecutionUnitKind::ExecuteCommitWritesTx);
            plan.push_back(EExecutionUnitKind::FinishPropose);
        } else {
            if (!loaded) {
                plan.push_back(EExecutionUnitKind::CheckCommitWritesTx);
                plan.push_back(EExecutionUnitKind::StoreCommitWritesTx);
                plan.push_back(EExecutionUnitKind::FinishPropose);
            }
            if (!GetStep()) {
                plan.push_back(EExecutionUnitKind::WaitForPlan);
            }
            plan.push_back(EExecutionUnitKind::PlanQueue);
            plan.push_back(EExecutionUnitKind::LoadTxDetails);
            plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
            plan.push_back(EExecutionUnitKind::ExecuteCommitWritesTx);
            plan.push_back(EExecutionUnitKind::CompleteOperation);
        }
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else if (IsSchemeTx()) {
        if (!loaded) {
            plan.push_back(EExecutionUnitKind::CheckSchemeTx);
            plan.push_back(EExecutionUnitKind::StoreSchemeTx);
            plan.push_back(EExecutionUnitKind::FinishPropose);
        }
        if (!GetStep())
            plan.push_back(EExecutionUnitKind::WaitForPlan);
        plan.push_back(EExecutionUnitKind::PlanQueue);
        plan.push_back(EExecutionUnitKind::LoadTxDetails);
        plan.push_back(EExecutionUnitKind::ProtectSchemeEchoes);
        plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
        plan.push_back(EExecutionUnitKind::MakeSnapshot);
        plan.push_back(EExecutionUnitKind::BuildSchemeTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareSchemeTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::Backup);
        plan.push_back(EExecutionUnitKind::Restore);
        plan.push_back(EExecutionUnitKind::CreateTable);
        plan.push_back(EExecutionUnitKind::ReceiveSnapshot);
        plan.push_back(EExecutionUnitKind::ReceiveSnapshotCleanup);
        plan.push_back(EExecutionUnitKind::AlterMoveShadow);
        plan.push_back(EExecutionUnitKind::AlterTable);
        plan.push_back(EExecutionUnitKind::DropTable);
        plan.push_back(EExecutionUnitKind::CreatePersistentSnapshot);
        plan.push_back(EExecutionUnitKind::DropPersistentSnapshot);
        plan.push_back(EExecutionUnitKind::InitiateBuildIndex);
        plan.push_back(EExecutionUnitKind::FinalizeBuildIndex);
        plan.push_back(EExecutionUnitKind::DropIndexNotice);
        plan.push_back(EExecutionUnitKind::MoveTable);
        plan.push_back(EExecutionUnitKind::MoveIndex);
        plan.push_back(EExecutionUnitKind::CreateCdcStream);
        plan.push_back(EExecutionUnitKind::AlterCdcStream);
        plan.push_back(EExecutionUnitKind::DropCdcStream);
        plan.push_back(EExecutionUnitKind::CompleteOperation);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else {
        Y_FAIL_S("unknown operation kind " << GetKind());
    }

    RewriteExecutionPlan(plan);
}

void TActiveTransaction::FillState(NKikimrTxDataShard::TEvGetOperationResponse &resp) const
{
    if (IsReadTable()) {
        auto &state = *resp.MutableReadTableState();
        if (DataTx)
            state.SetTableId(DataTx->GetReadTableTransaction().GetTableId().GetTableId());
        state.SetSnapshotId(ScanSnapshotId);
        state.SetScanTaskId(ScanTask);
        state.SetSinkActor(ToString(StreamSink));
        state.SetScanActor(ToString(ScanActor));
    }
}

void TActiveTransaction::KillAsyncJobActor(const TActorContext& ctx) {
    if (!GetAsyncJobActor()) {
        return;
    }

    ctx.Send(GetAsyncJobActor(), new TEvents::TEvPoison());
    SetAsyncJobActor(TActorId());
}

void TActiveTransaction::TrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Add(TxBody.size());
}

void TActiveTransaction::UntrackMemory() const {
    NActors::NMemory::TLabel<MemoryLabelActiveTransactionBody>::Sub(TxBody.size());
}

}}
