#include "defs.h"

#include "datashard_active_transaction.h"
#include "datashard_kqp.h"
#include "datashard_impl.h"
#include "datashard_failpoints.h"
#include "key_conflicts.h"

#include <ydb/core/tx/locks/locks.h>
#include <ydb/library/actors/util/memory_track.h>

namespace NKikimr {
namespace NDataShard {

TValidatedDataTx::TValidatedDataTx(TDataShard *self,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx,
                                   const TStepOrder &stepTxId,
                                   TInstant receivedAt,
                                   const TString &txBody,
                                   bool usesMvccSnapshot,
                                   const TString& userSID,
                                   bool isPropose)
    : StepTxId_(stepTxId)
    , TxBody(txBody)
    , EngineBay(self, txc, ctx, stepTxId, userSID)
    , ErrCode(NKikimrTxDataShard::TError::OK)
    , TxSize(0)
    , IsReleased(false)
    , BuiltTaskRunner(false)
    , IsReadOnly(true)
    , AllowCancelROwithReadsets(self->AllowCancelROwithReadsets())
    , Cancelled(false)
    , ReceivedAt_(receivedAt)
{
    const ui64 tabletId = self->TabletID();

    bool success = Tx.ParseFromArray(TxBody.data(), TxBody.size());
    if (!success) {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = "Failed to parse TxBody";
        return;
    }

    auto& typeRegistry = *AppData()->TypeRegistry;

    ComputeTxSize();
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Add(TxSize);

    Y_ENSURE(Tx.HasMiniKQL() || Tx.HasReadTableTransaction() || Tx.HasKqpTransaction(),
             "One of the fields should be set: MiniKQL, ReadTableTransaction, KqpTransaction");

    if (Tx.GetLockTxId())
        EngineBay.SetLockTxId(Tx.GetLockTxId(), Tx.GetLockNodeId());

    if (Tx.GetLockMode() != NKikimrDataEvents::OPTIMISTIC) {
        ErrCode = NKikimrTxDataShard::TError::BAD_ARGUMENT;
        ErrStr = TStringBuilder() << "Only OPTIMISTIC lock mode is supported in data transactions.";
        return;
    }

    if (Tx.GetImmediate())
        EngineBay.SetIsImmediateTx();

    if (usesMvccSnapshot)
        EngineBay.SetUsesMvccSnapshot();

    if (Tx.HasReadTableTransaction()) {
        auto &tx = Tx.GetReadTableTransaction();
        if (self->TableInfos.contains(tx.GetTableId().GetTableId())) {
            auto* info = self->TableInfos[tx.GetTableId().GetTableId()].Get();
            Y_ENSURE(info, "Unexpected missing table info");
            TSerializedTableRange range(tx.GetRange());
            EngineBay.GetKeyValidator().AddReadRange(TTableId(tx.GetTableId().GetOwnerId(),
                                            tx.GetTableId().GetTableId()),
                                   {}, range.ToTableRange(), info->KeyColumnTypes);
        } else {
            ErrCode = NKikimrTxDataShard::TError::SCHEME_ERROR;
            ErrStr = "Trying to read from table that doesn't exist";
        }
    } else if (IsKqpTx()) {
        if (Y_UNLIKELY(!IsKqpDataTx())) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Unexpected KQP transaction type, shard: " << tabletId
                << ", txid: " << StepTxId_.TxId << ", tx: " << Tx.DebugString());
            ErrCode = NKikimrTxDataShard::TError::BAD_TX_KIND;
            ErrStr = TStringBuilder() << "Unexpected KQP transaction type: "
                << NKikimrTxDataShard::EKqpTransactionType_Name(Tx.GetKqpTransaction().GetType()) << ".";
            return;
        }

        auto& computeCtx = EngineBay.GetKqpComputeCtx();

        try {
            bool hasPersistentChannels = false;
            if (!KqpValidateTransaction(GetTasks(), Immediate(), StepTxId_.TxId, ctx, hasPersistentChannels)) {
                LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP transaction validation failed, datashard: "
                    << tabletId << ", txid: " << StepTxId_.TxId);
                ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
                ErrStr = "Transaction validation failed.";
                return;
            }
            computeCtx.SetHasPersistentChannels(hasPersistentChannels);

            for (auto& task : GetTasks()) {
                NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta meta;
                if (!task.GetMeta().UnpackTo(&meta)) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP transaction validation failed"
                        << ", datashard: " << tabletId
                        << ", txid: " << StepTxId_.TxId
                        << ", failed to load task meta: " << task.GetMeta().value());
                    ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
                    ErrStr = "Transaction validation failed: invalid task metadata.";
                    return;
                }

                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TxId: " << StepTxId_.TxId << ", shard " << tabletId
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

                KqpSetTxKeys(tabletId, task.GetId(), tableInfo, meta, typeRegistry, ctx, EngineBay.GetKeyValidator());

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

            KqpSetTxLocksKeys(GetKqpLocks(), self->SysLocksTable(), EngineBay.GetKeyValidator());
            EngineBay.MarkTxLoaded();

            auto& tasksRunner = GetKqpTasksRunner(); // create tasks runner, can throw TMemoryLimitExceededException

            auto allocGuard = tasksRunner.BindAllocator(isPropose ? 100_MB : 1_GB); // set big enough limit, decrease/correct later

            auto execCtx = DefaultKqpExecutionContext();
            tasksRunner.Prepare(DefaultKqpDataReqMemoryLimits(), *execCtx);
        } catch (const TMemoryLimitExceededException&) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Not enough memory to create tasks runner, datashard: "
                << tabletId << ", txid: " << StepTxId_.TxId);
            ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
            ErrStr = TStringBuilder() << "Transaction validation failed: not enough memory.";
            return;
        } catch (const yexception& e) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Exception while validating KQP transaction, datashard: "
                << tabletId << ", txid: " << StepTxId_.TxId << ", error: " << e.what());
            ErrCode = NKikimrTxDataShard::TError::PROGRAM_ERROR;
            ErrStr = TStringBuilder() << "Transaction validation failed: " << e.what() << ".";
            return;
        }
    } else {
        Y_ENSURE(Tx.HasMiniKQL());
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
        auto result = engine->AddProgram(tabletId, Tx.GetMiniKQL(), Tx.GetReadOnly());

        ErrStr = engine->GetErrors();
        ErrCode = ConvertErrCode(result);
    }

    ComputeDeadline();
}

TValidatedDataTx::~TValidatedDataTx() {
    NActors::NMemory::TLabel<MemoryLabelValidatedDataTx>::Sub(TxSize);
}

TDataShardUserDb& TValidatedDataTx::GetUserDb() {
    return EngineBay.GetUserDb();
}
const TDataShardUserDb& TValidatedDataTx::GetUserDb() const {
    return EngineBay.GetUserDb();
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
        Y_ENSURE(result == EResult::Ok, "Engine errors: " << EngineBay.GetEngine()->GetErrors());
    }
    return KeysCount();
}

bool TValidatedDataTx::ReValidateKeys(const NTable::TScheme& scheme)
{
    using EResult = NMiniKQL::IEngineFlat::EResult;

    if (IsKqpTx()) {
        const auto& userDb = EngineBay.GetUserDb();
        TKeyValidator::TValidateOptions options(userDb.GetLockTxId(), userDb.GetLockNodeId(), userDb.GetUsesMvccSnapshot(), userDb.GetIsImmediateTx(), userDb.GetIsWriteTx(), scheme);
        auto [result, error] = EngineBay.GetKeyValidator().ValidateKeys(options);
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

bool TValidatedDataTx::CheckCancelled(ui64 tabletId) {
    if (Cancelled) {
        return true;
    }

    if (!CanCancel()) {
        return false;
    }

    TInstant now = AppData()->TimeProvider->Now();
    Cancelled = (now >= Deadline());

    Cancelled = Cancelled || gCancelTxFailPoint.Check(tabletId, GetTxId());

    if (Cancelled) {
        LOG_NOTICE_S(*TActivationContext::ActorSystem(), NKikimrServices::TX_DATASHARD, "CANCELLED TxId " << GetTxId() << " at " << tabletId);
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
    Y_ENSURE(!DataTx);
    Y_ENSURE(TxBody.empty() || HasVolatilePrepareFlag());

    Target = dataTx->GetSource();
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
                                    ui64 artifactFlags,
                                    const TString& userSID)
{
    UntrackMemory();

    Y_ENSURE(!DataTx);
    Y_ENSURE(TxBody.empty());

    UserSID = userSID;
    Target = target;
    TxBody = txBody;
    if (locks.size()) {
        for (auto lock : locks)
            LocksCache().Locks[lock.LockId] = lock;
    }
    ArtifactFlags = artifactFlags;
    if (IsDataTx() || IsReadTable()) {
        Y_ENSURE(!DataTx);
        BuildDataTx(self, txc, ctx, userSID);
        Y_ENSURE(DataTx->Ready());

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
                                            const TActorContext &ctx,
                                            const TString& userSID)
{
    UntrackMemory();

    Y_ENSURE(!DataTx);
    Y_ENSURE(!TxBody.empty());

    UserSID = userSID;
    
    if (IsDataTx() || IsReadTable()) {
        BuildDataTx(self, txc, ctx, userSID);
        Y_ENSURE(DataTx->Ready());

        if (DataTx->HasStreamResponse())
            SetStreamSink(DataTx->GetSink());
    } else if (IsSnapshotTx()) {
        BuildSnapshotTx();
    } else {
        Y_ENSURE(false, "Unexpected FillVolatileTxData call");
    }

    TrackMemory();
    UserSID = userSID;
}

TValidatedDataTx::TPtr TActiveTransaction::BuildDataTx(TDataShard *self,
                                                       TTransactionContext &txc,
                                                       const TActorContext &ctx,
                                                       const TString& userSID,
                                                       bool isPropose)
{
    Y_ENSURE(IsDataTx() || IsReadTable());
    if (!DataTx) {
        Y_ENSURE(TxBody);
        DataTx = std::make_shared<TValidatedDataTx>(self, txc, ctx, GetStepOrder(),
                                                    GetReceivedAt(), TxBody, IsMvccSnapshotRead(), userSID, isPropose);
        if (DataTx->HasStreamResponse())
            SetStreamSink(DataTx->GetSink());
    }
    return DataTx;
}

bool TActiveTransaction::BuildSchemeTx()
{
    Y_ENSURE(TxBody);
    SchemeTx.Reset(new NKikimrTxDataShard::TFlatSchemeTransaction);

    bool res = SchemeTx->ParseFromArray(TxBody.data(), TxBody.size());
    if (!res)
        return false;

    size_t count = 0;
    SchemeTxType = TSchemaOperation::ETypeUnknown;

    if (SchemeTx->HasCreateTable()) {
        SchemeTxType = TSchemaOperation::ETypeCreate;
        count++;
    }
    
    if (SchemeTx->HasDropTable()) {
        SchemeTxType = TSchemaOperation::ETypeDrop;
        count++;
    }
    
    if (SchemeTx->HasAlterTable()) {
        SchemeTxType = TSchemaOperation::ETypeAlter;
        count++;
    }
    
    if (SchemeTx->HasBackup()) {
        SchemeTxType = TSchemaOperation::ETypeBackup;
        count++;
    }
    
    if (SchemeTx->HasRestore()) {
        SchemeTxType = TSchemaOperation::ETypeRestore;
        count++;
    }
    
    if (SchemeTx->HasSendSnapshot()) {
        SchemeTxType = TSchemaOperation::ETypeCopy;
        count++;
    }
    
    if (SchemeTx->HasCreatePersistentSnapshot()) {
        SchemeTxType = TSchemaOperation::ETypeCreatePersistentSnapshot;
        count++;
    }
    
    if (SchemeTx->HasDropPersistentSnapshot()) {
        SchemeTxType = TSchemaOperation::ETypeDropPersistentSnapshot;
        count++;
    }
    
    if (SchemeTx->HasInitiateBuildIndex()) {
        SchemeTxType = TSchemaOperation::ETypeInitiateBuildIndex;
        count++;
    }
    
    if (SchemeTx->HasFinalizeBuildIndex()) {
        SchemeTxType = TSchemaOperation::ETypeFinalizeBuildIndex;
        count++;
    }
    
    if (SchemeTx->HasDropIndexNotice()) {
        SchemeTxType = TSchemaOperation::ETypeDropIndexNotice;
        count++;
    }
    
    if (SchemeTx->HasMoveTable()) {
        SchemeTxType = TSchemaOperation::ETypeMoveTable;
        count++;
    }
    
    if (SchemeTx->HasCreateCdcStreamNotice()) {
        SchemeTxType = TSchemaOperation::ETypeCreateCdcStream;
        count++;
    }
    
    if (SchemeTx->HasAlterCdcStreamNotice()) {
        SchemeTxType = TSchemaOperation::ETypeAlterCdcStream;
        count++;
    }
    
    if (SchemeTx->HasDropCdcStreamNotice()) {
        SchemeTxType = TSchemaOperation::ETypeDropCdcStream;
        count++;
    }
    
    if (SchemeTx->HasRotateCdcStreamNotice()) {
        SchemeTxType = TSchemaOperation::ETypeRotateCdcStream;
        count++;
    }
    
    if (SchemeTx->HasMoveIndex()) {
        SchemeTxType = TSchemaOperation::ETypeMoveIndex;
        count++;
    }
    
    if (SchemeTx->HasCreateIncrementalRestoreSrc()) {
        SchemeTxType = TSchemaOperation::ETypeCreateIncrementalRestoreSrc;
        count++;
    }
    
    if (SchemeTx->HasCreateIncrementalBackupSrc()) {
        SchemeTxType = TSchemaOperation::ETypeCreateIncrementalBackupSrc;
        count++;
    }
    
    if (SchemeTx->HasTruncateTable()) {
        SchemeTxType = TSchemaOperation::ETypeTruncate;
        count++;
    }

    return count == 1;
}

bool TActiveTransaction::BuildSnapshotTx()
{
    Y_ENSURE(TxBody);
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
    Y_ENSURE(TxBody);
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
    Y_ENSURE(TxBody);
    CommitWritesTx.Reset(new TCommitWritesTx);
    return CommitWritesTx->TryParse(TxBody);
}

//

void TActiveTransaction::ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase &provider,
                                       const TActorContext &ctx) {
    ReleasedTxDataSize = provider.GetMemoryLimit() + provider.GetRequestedMemory();

    if (!DataTx || DataTx->GetIsReleased())
        return;

    DataTx->ReleaseTxData();
    // Immediate transactions have no body stored.
    if (!IsImmediate() && !HasVolatilePrepareFlag()) {
        ClearTxBody();
    }

    //InReadSets.clear();
    OutReadSets().clear();
    LocksAccessLog().Locks.clear();
    LocksCache().Locks.clear();
    ArtifactFlags = 0;

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << GetTxId() << " released its data");
}

void TActiveTransaction::DbStoreLocksAccessLog(ui64 tabletId,
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
                << " in " << tabletId);
}

void TActiveTransaction::DbStoreArtifactFlags(ui64 tabletId,
                                              TTransactionContext &txc,
                                              const TActorContext &ctx)
{
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TxArtifacts>().Key(GetTxId())
        .Update<Schema::TxArtifacts::Flags>(ArtifactFlags);

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Storing artifactflags=" << ArtifactFlags << " for txid=" << GetTxId()
                << " in " << tabletId);
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
        const TActorContext &ctx,
        const TString& userSID)
{
    UserSID = userSID;
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
        Y_ENSURE(TxBody);
    }

    TrackMemory();

    for (auto &lock : locks)
        LocksCache().Locks[lock.LockId] = lock;

    bool extractKeys = DataTx->IsTxInfoLoaded();
    DataTx = std::make_shared<TValidatedDataTx>(self, txc, ctx, GetStepOrder(),
                                                GetReceivedAt(), TxBody, IsMvccSnapshotRead(), userSID);
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
    Y_ENSURE(IsDataTx());
    Y_ENSURE(!IsImmediate());
    Y_ENSURE(!IsKqpScanTransaction());

    TVector<EExecutionUnitKind> plan;

    plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
    if (IsKqpDataTransaction()) {
        plan.push_back(EExecutionUnitKind::BuildKqpDataTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareKqpDataTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::BlockFailPoint);
        plan.push_back(EExecutionUnitKind::ExecuteKqpDataTx);
    } else {
        plan.push_back(EExecutionUnitKind::BuildDataTxOutRS);
        plan.push_back(EExecutionUnitKind::StoreAndSendOutRS);
        plan.push_back(EExecutionUnitKind::PrepareDataTxInRS);
        plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        plan.push_back(EExecutionUnitKind::BlockFailPoint);
        plan.push_back(EExecutionUnitKind::ExecuteDataTx);
    }
    plan.push_back(EExecutionUnitKind::CompleteOperation);
    plan.push_back(EExecutionUnitKind::CompletedOperations);

    RewriteExecutionPlan(plan);
}


void TActiveTransaction::BuildExecutionPlan(bool loaded)
{
    Y_ENSURE(GetExecutionPlan().empty());
    Y_ENSURE(!IsKqpScanTransaction());

    TVector<EExecutionUnitKind> plan;
    if (IsDataTx()) {
        if (IsImmediate()) {
            Y_ENSURE(!loaded);
            plan.push_back(EExecutionUnitKind::CheckDataTx);
            plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
            plan.push_back(EExecutionUnitKind::BlockFailPoint);
            if (IsKqpDataTransaction()) {
                plan.push_back(EExecutionUnitKind::ExecuteKqpDataTx);
            } else {
                plan.push_back(EExecutionUnitKind::ExecuteDataTx);
            }
            plan.push_back(EExecutionUnitKind::FinishPropose);
            plan.push_back(EExecutionUnitKind::CompletedOperations);
        } else if (HasVolatilePrepareFlag()) {
            Y_ENSURE(!loaded);
            plan.push_back(EExecutionUnitKind::CheckDataTx);
            plan.push_back(EExecutionUnitKind::StoreDataTx); // note: stores in memory
            plan.push_back(EExecutionUnitKind::FinishPropose);
            Y_ENSURE(!GetStep());
            plan.push_back(EExecutionUnitKind::WaitForPlan);
            plan.push_back(EExecutionUnitKind::PlanQueue);
            plan.push_back(EExecutionUnitKind::LoadTxDetails); // note: reloads from memory
            plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
            Y_ENSURE(IsKqpDataTransaction());
            // Note: execute will also prepare and send readsets
            plan.push_back(EExecutionUnitKind::BlockFailPoint);
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
            Y_ENSURE(!loaded);
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
        plan.push_back(EExecutionUnitKind::RotateCdcStream);
        plan.push_back(EExecutionUnitKind::CreateIncrementalRestoreSrc);
        plan.push_back(EExecutionUnitKind::Truncate);
        plan.push_back(EExecutionUnitKind::CompleteOperation);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
    } else {
        Y_ENSURE(false, "unknown operation kind " << GetKind());
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

bool TActiveTransaction::OnStopping(TDataShard& self, const TActorContext& ctx) {
    if (IsImmediate()) {
        // Send reject result immediately, because we cannot control when
        // a new datashard tablet may start and block us from commiting
        // anything new. The usual progress queue is too slow for that.
        if (!HasResultSentFlag() && !Result()) {
            auto kind = static_cast<NKikimrTxDataShard::ETransactionKind>(GetKind());
            auto rejectStatus = NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED;
            TString rejectReason = TStringBuilder()
                    << "Rejecting immediate tx "
                    << GetTxId()
                    << " because datashard "
                    << self.TabletID()
                    << " is restarting";
            auto result = std::make_unique<TEvDataShard::TEvProposeTransactionResult>(
                    kind, self.TabletID(), GetTxId(), rejectStatus);
            result->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, rejectReason);
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, rejectReason);

            ctx.Send(GetTarget(), result.release(), 0, GetCookie());

            self.IncCounter(COUNTER_PREPARE_OVERLOADED);
            self.IncCounter(COUNTER_PREPARE_COMPLETE);
            SetResultSentFlag();
        }

        // Immediate ops become ready when stopping flag is set
        return true;
    } else if (HasVolatilePrepareFlag()) {
        // Volatile transactions may be aborted at any time unless executed
        // Note: we need to send the result (and discard the transaction) as
        // soon as possible, because new transactions are unlikely to execute
        // and commits will even more likely fail.
        if (!HasResultSentFlag() && !Result() && !HasCompletedFlag()) {
            auto kind = static_cast<NKikimrTxDataShard::ETransactionKind>(GetKind());
            auto status = NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED;
            auto result = std::make_unique<TEvDataShard::TEvProposeTransactionResult>(
                kind, self.TabletID(), GetTxId(), status);
            result->AddError(NKikimrTxDataShard::TError::EXECUTION_CANCELLED, TStringBuilder()
                << "DataShard " << self.TabletID() << " is restarting");
            ctx.Send(GetTarget(), result.release(), 0, GetCookie());

            // Make sure we also send acks and nodata readsets to expecting participants
            std::vector<std::unique_ptr<IEventHandle>> cleanupReplies;
            self.GetCleanupReplies(this, cleanupReplies);

            for (auto& ev : cleanupReplies) {
                TActivationContext::Send(ev.release());
            }

            SetResultSentFlag();
            return true;
        }

        // Executed transactions will have to wait until committed
        // There is no way to hand-off committing volatile transactions for now
        return false;
    } else {
        // Distributed operations send notification when proposed
        if (GetTarget() && !HasCompletedFlag()) {
            self.SendRestartNotification(this);
        }

        // Distributed ops avoid doing new work when stopping
        return false;
    }
}

void TActiveTransaction::OnCleanup(TDataShard& self, std::vector<std::unique_ptr<IEventHandle>>& replies) {
    if (!IsImmediate() && GetTarget() && !HasCompletedFlag()) {
        auto kind = static_cast<NKikimrTxDataShard::ETransactionKind>(GetKind());
        auto status = NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED;
        auto result = std::make_unique<TEvDataShard::TEvProposeTransactionResult>(
            kind, self.TabletID(), GetTxId(), status);

        if (self.State == TShardState::SplitSrcWaitForNoTxInFlight) {
            result->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, TStringBuilder()
                << "DataShard " << self.TabletID() << " is splitting");
        } else if (self.Pipeline.HasWaitingSchemeOps()) {
            result->AddError(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED, TStringBuilder()
                << "DataShard " << self.TabletID() << " is blocked by a schema operation");
        } else {
            result->AddError(NKikimrTxDataShard::TError::EXECUTION_CANCELLED, "Transaction was cleaned up");
        }

        replies.push_back(std::make_unique<IEventHandle>(GetTarget(), self.SelfId(), result.release(), 0, GetCookie()));
    }
}

}}
