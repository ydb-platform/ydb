#include "datashard_write_operation.h"
#include "datashard_pipeline.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"
#include "datashard_kqp.h"
#include "datashard_integrity_trails.h"

#include <ydb/core/engine/mkql_engine_flat_host.h>

namespace NKikimr {
namespace NDataShard {

class TExecuteWriteUnit : public TExecutionUnit {
public:
    TExecuteWriteUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteWrite, true, self, pipeline)
    {
    }

    ~TExecuteWriteUnit()
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

        if (writeOp->GetWriteResult() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
            return true;
        }

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return false;
        }

        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    // Filter out self-breaks from locksBrokenByTx, log breaker TLI events, and update TxStats.
    // Returns the count of locks broken by OTHER transactions (excluding self-breaks).
    size_t HandleBreakerLocks(
        const TVector<ui64>& locksBrokenByTx, ui64 selfTxId, ui64 querySpanId,
        TSysLocks& sysLocks, NKikimrDataEvents::TEvWriteResult& record,
        const TActorContext& ctx, TStringBuf message,
        const NKikimrDataEvents::TKqpLocks* kqpLocks = nullptr)
    {
        // Collect all lock IDs that belong to the current transaction.
        // In the OLTP sink model, a transaction reads with lockTxId=X and commits
        // with txId=Y. The commit write may break its own read lock (lockId=X),
        // but X != Y, so checking selfTxId alone is insufficient. The kqpLocks
        // contain the transaction's own lock descriptions with their LockIds.
        THashSet<ui64> selfLockIds;
        selfLockIds.insert(selfTxId);
        if (kqpLocks) {
            for (const auto& lock : kqpLocks->GetLocks()) {
                selfLockIds.insert(lock.GetLockId());
            }
        }

        TVector<ui64> otherLocksBroken;
        otherLocksBroken.reserve(locksBrokenByTx.size());
        for (const auto& lockId : locksBrokenByTx) {
            if (!selfLockIds.contains(lockId)) {
                otherLocksBroken.push_back(lockId);
            }
        }
        record.MutableTxStats()->SetLocksBrokenAsBreaker(otherLocksBroken.size());
        if (!otherLocksBroken.empty()) {
            auto victimQuerySpanIds = sysLocks.ExtractVictimQuerySpanIds(otherLocksBroken);

            // Resolve BreakerQuerySpanId: prefer the conflict-derived ID (set during CommitLock
            // from the stored conflict data — the actual query that wrote to the conflicting key).
            // Fall back to EvWrite's QuerySpanId when unavailable.
            ui64 primaryBreakerSpanId = 0;
            if (auto breakerQuerySpanId = sysLocks.GetCurrentBreakerQuerySpanId()) {
                primaryBreakerSpanId = *breakerQuerySpanId;
            }

            if (primaryBreakerSpanId != 0) {
                // Conflict-derived: report only the actual breaker query, not all shard writers.
                record.MutableTxStats()->AddBreakerQuerySpanIds(primaryBreakerSpanId);
            } else {
                // Direct write (no commit locks): use EvWrite's QuerySpanId.
                if (querySpanId != 0) {
                    record.MutableTxStats()->AddBreakerQuerySpanIds(querySpanId);
                    primaryBreakerSpanId = querySpanId;
                }
            }

            NDataIntegrity::LogLocksBroken(ctx, DataShard.TabletID(), message,
                otherLocksBroken, primaryBreakerSpanId, victimQuerySpanIds);
        }
        return otherLocksBroken.size();
    }

    void AddLocksToResult(TWriteOperation* writeOp, ui64 querySpanId, const TActorContext& ctx,
        const NKikimrDataEvents::TKqpLocks* kqpLocks = nullptr)
    {
        NEvents::TDataEvents::TEvWriteResult& writeResult = *writeOp->GetWriteResult();

        auto [locks, locksBrokenByTx] = DataShard.SysLocksTable().ApplyLocks();
        HandleBreakerLocks(locksBrokenByTx, writeOp->GetTxId(), querySpanId,
            DataShard.SysLocksTable(), writeResult.Record, ctx,
            "Write transaction broke other locks", kqpLocks);
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "add locks to result: " << locks.size());
        for (const auto& lock : locks) {
            if (lock.IsError()) {
                LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "Lock is not set for " << *writeOp << " at " << DataShard.TabletID() << " lock " << lock);
            }

            writeResult.AddTxLock(lock.LockId, lock.DataShard, lock.Generation, lock.Counter, lock.SchemeShard, lock.PathId, lock.HasWrites);

            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "add lock to result: " << writeResult.Record.GetTxLocks().rbegin()->ShortDebugString());
        }
        DataShard.SubscribeNewLocks(ctx);
    }

    void ResetChanges(TDataShardUserDb& userDb, TTransactionContext& txc) {
        userDb.ResetCollectedChanges();

        if (txc.DB.HasChanges()) {
            txc.DB.RollbackChanges();
        }
    }

    bool CheckForVolatileReadDependencies(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        if (!userDb.GetVolatileReadDependencies().empty()) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting because volatile read dependencies");

            for (ui64 txId : userDb.GetVolatileReadDependencies()) {
                writeOp.AddVolatileDependency(txId);
                bool ok = DataShard.GetVolatileTxManager().AttachBlockedOperation(txId, writeOp.GetTxId());
                Y_ENSURE(ok, "Unexpected failure to attach TxId# " << writeOp.GetTxId() << " to volatile tx " << txId);
            }

            ResetChanges(userDb, txc);
            writeOp.ReleaseTxData(txc);

            return true;
        }

        return false;
    }

    void FillOps(const NTable::TScheme& scheme, const TUserTable& userTable, const NTable::TScheme::TTableInfo& tableInfo, const TValidatedWriteTxOperation& validatedOperation, ui32 rowIdx, TSmallVec<NTable::TUpdateOp>& ops) {
        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
        const auto& columnIds = validatedOperation.GetColumnIds();

        ops.clear();
        Y_ENSURE(matrix.GetColCount() >= userTable.KeyColumnIds.size());
        ops.reserve(matrix.GetColCount() - userTable.KeyColumnIds.size());

        for (ui16 valueColIdx = userTable.KeyColumnIds.size(); valueColIdx < matrix.GetColCount(); ++valueColIdx) {
            ui32 columnTag = columnIds[valueColIdx];
            const TCell& cell = matrix.GetCell(rowIdx, valueColIdx);

            const NScheme::TTypeId vtypeId = scheme.GetColumnInfo(&tableInfo, columnTag)->PType.GetTypeId();
            ops.emplace_back(columnTag, NTable::ECellOp::Set, cell.IsNull() ? TRawTypeValue() : TRawTypeValue(cell.Data(), cell.Size(), vtypeId));
        }
    };

    void FillKey(const NTable::TScheme& scheme, const TUserTable& userTable, const NTable::TScheme::TTableInfo& tableInfo, const TValidatedWriteTxOperation& validatedOperation, ui32 rowIdx, TSmallVec<TRawTypeValue>& key) {
        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();

        key.clear();
        key.reserve(userTable.KeyColumnIds.size());
        for (ui16 keyColIdx = 0; keyColIdx < userTable.KeyColumnIds.size(); ++keyColIdx) {
            const TCell& cell = matrix.GetCell(rowIdx, keyColIdx);
            ui32 keyCol = tableInfo.KeyColumns[keyColIdx];
            if (cell.IsNull()) {
                key.emplace_back();
            } else {
                NScheme::TTypeId vtypeId = scheme.GetColumnInfo(&tableInfo, keyCol)->PType.GetTypeId();
                key.emplace_back(cell.Data(), cell.Size(), vtypeId);
            }
        }
    };

    EExecutionStatus OnTabletNotReadyException(TDataShardUserDb& userDb, TWriteOperation& writeOp, size_t operationIndexToPrecharge, TTransactionContext& txc, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Tablet " << DataShard.TabletID() << " is not ready for " << writeOp << " execution");

        // Precharge
        if (operationIndexToPrecharge != SIZE_MAX) {
            const TValidatedWriteTx::TPtr& writeTx = writeOp.GetWriteTx();
            for (size_t operationIndex = operationIndexToPrecharge; operationIndex < writeTx->GetOperations().size(); ++operationIndex) {
                const TValidatedWriteTxOperation& validatedOperation = writeTx->GetOperations()[operationIndex];
                const ui64 tableId = validatedOperation.GetTableId().PathId.LocalPathId;
                const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);
                const TUserTable& userTable = *DataShard.GetUserTables().at(tableId);

                const NTable::TScheme& scheme = txc.DB.GetScheme();
                const NTable::TScheme::TTableInfo& tableInfo = *scheme.GetTableInfo(userTable.LocalTid);

                const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
                const auto operationType = validatedOperation.GetOperationType();

                TSmallVec<TRawTypeValue> key;

                if (operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT ||
                    operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE ||
                    userDb.NeedToReadBeforeWrite(fullTableId))
                {
                    for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx) {
                        FillKey(scheme, userTable, tableInfo, validatedOperation, rowIdx, key);
                        userDb.PrechargeRow(fullTableId, key);
                    }
                }
            }
        }

        DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

        ResetChanges(userDb, txc);
        writeOp.ReleaseTxData(txc);
        return EExecutionStatus::Restart;
    }

    bool OnUniqueConstrainException(TDataShardUserDb& userDb, TWriteOperation& writeOp, ui64 lockTxId, ui64 querySpanId, TTransactionContext& txc, const TActorContext& ctx) {
        if (CheckForVolatileReadDependencies(userDb, writeOp, txc, ctx)) {
            return false;
        }

        if (userDb.GetSnapshotReadConflict()) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting. Conflict with another transaction.");
            writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Read conflict with concurrent transaction.");
            writeOp.GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(1);
            NDataIntegrity::LogVictimDetected(ctx, DataShard.TabletID(), "Write transaction was a victim of broken locks",
                                              lockTxId ? DataShard.SysLocksTable().GetVictimQuerySpanIdForLock(lockTxId) : Nothing(),
                                              querySpanId ? TMaybe<ui64>(querySpanId) : Nothing());
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting. Conflict with existing key.");
            writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, "Conflict with existing key.");
        }

        ResetChanges(userDb, txc);
        return true;
    }

    void DoUpdateToUserDb(TDataShardUserDb& userDb, const TValidatedWriteTxOperation& validatedOperation, TTransactionContext& txc) {
        const ui64 tableId = validatedOperation.GetTableId().PathId.LocalPathId;
        const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);
        const TUserTable& userTable = *DataShard.GetUserTables().at(tableId);

        const NTable::TScheme& scheme = txc.DB.GetScheme();
        const NTable::TScheme::TTableInfo& tableInfo = *scheme.GetTableInfo(userTable.LocalTid);

        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
        const auto operationType = validatedOperation.GetOperationType();

        TSmallVec<TRawTypeValue> key;
        TSmallVec<NTable::TUpdateOp> ops;
        const ui32 defaultFilledColumnCount = validatedOperation.GetDefaultFilledColumnCount();

        // Main update cycle

        for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx)
        {
            FillKey(scheme, userTable, tableInfo, validatedOperation, rowIdx, key);

            switch (operationType) {
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.UpsertRow(fullTableId, key, ops, defaultFilledColumnCount);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.ReplaceRow(fullTableId, key, ops);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE: {
                    userDb.EraseRow(fullTableId, key);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.InsertRow(fullTableId, key, ops);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.UpdateRow(fullTableId, key, ops);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.IncrementRow(fullTableId, key, ops, false /*insertMissing*/);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT: {
                    FillOps(scheme, userTable, tableInfo, validatedOperation, rowIdx, ops);
                    userDb.IncrementRow(fullTableId, key, ops, true /*insertMissing*/);
                    break;
                }
                default:
                    // Checked before in TWriteOperation
                    Y_ENSURE(false, operationType << " operation is not supported now");
            }
        }

        // Counters

        switch (operationType) {
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT: {
                DataShard.IncCounter(COUNTER_WRITE_ROWS, matrix.GetRowCount());
                DataShard.IncCounter(COUNTER_WRITE_BYTES, matrix.GetBuffer().size());
                break;
            }
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE: {
                DataShard.IncCounter(COUNTER_ERASE_ROWS, matrix.GetRowCount());
                break;
            }
            default:
                // Checked before in TWriteOperation
                Y_ENSURE(false, operationType << " operation is not supported now");
        }
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);
        const ui64 tabletId = DataShard.TabletID();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executing write operation for " << *op << " at " << tabletId);

        if (writeOp->GetWriteResult() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
            return EExecutionStatus::Executed;
        }

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return EExecutionStatus::Continue;
        }

        if (op->IsImmediate()) {
            // Every time we execute immediate transaction we may choose a new mvcc version
            op->CachedMvccVersion.reset();
        }

        const TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();
        size_t validatedOperationIndex = SIZE_MAX;

        DataShard.ReleaseCache(*writeOp);

        if (writeOp->IsTxDataReleased()) {
            switch (Pipeline.RestoreWriteTx(writeOp, txc)) {
                case ERestoreDataStatus::Ok:
                    break;

                case ERestoreDataStatus::Restart:
                    return EExecutionStatus::Restart;

                case ERestoreDataStatus::Error:
                    // For immediate transactions we want to translate this into a propose failure
                    if (op->IsImmediate()) {
                        Y_ENSURE(!writeTx->Ready());
                        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, writeTx->GetErrStr());
                        return EExecutionStatus::Executed;
                    }

                    // For planned transactions errors are not expected
                    Y_ENSURE(false, "Failed to restore tx data: " << writeTx->GetErrStr());
            }
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        if (op->IsImmediate() && !writeOp->ReValidateKeys(txc.DB.GetScheme())) {
            // Immediate transactions may be reordered with schema changes and become invalid
            Y_ENSURE(!writeTx->Ready());
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, writeTx->GetErrStr());
            return EExecutionStatus::Executed;
        }

        if (writeTx->CheckCancelled()) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
            DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
            writeOp->ReleaseTxData(txc);
            return EExecutionStatus::Executed;
        }

        NMiniKQL::TEngineHostCounters engineHostCounters;
        const ui64 txId = op->GetTxId();
        const auto mvccVersion = DataShard.GetMvccVersion(writeOp);

        TDataShardUserDb userDb(DataShard, txc.DB, op->GetGlobalTxId(), mvccVersion, engineHostCounters, TAppData::TimeProvider->Now());
        userDb.SetIsWriteTx(true);
        userDb.SetIsImmediateTx(op->IsImmediate());
        userDb.SetLockTxId(writeTx->GetLockTxId());
        userDb.SetLockNodeId(writeTx->GetLockNodeId());
        userDb.SetLockMode(writeTx->GetLockMode());

        if (op->HasVolatilePrepareFlag() || op->GetRemainReadSets()) {
            userDb.SetVolatileTxId(txId);
        }

        if (auto mvccSnapshot = writeTx->GetMvccSnapshot()) {
            userDb.SetSnapshotVersion(*mvccSnapshot);
        }

        bool preparedOutReadSets = false;
        bool keepOutReadSets = writeOp->IsOutRSStored();

        const auto* kqpLocks = writeTx->GetKqpLocks() ? &writeTx->GetKqpLocks().value() : nullptr;

        auto ensureAbortOutReadSets = [&]() -> std::optional<EExecutionStatus> {
            if (!op->IsImmediate() && !keepOutReadSets) {
                op->OutReadSets().clear();
                op->AwaitingDecisions().clear();
                KqpFillOutReadSets(op->OutReadSets(), kqpLocks, NKikimrTx::TReadSetData::DECISION_ABORT, tabletId);
                if (!op->OutReadSets().empty()) {
                    DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(), op->PreparedOutReadSets(), txc, ctx);
                    preparedOutReadSets = true;
                }
                keepOutReadSets = true;

                if (preparedOutReadSets && !op->PreparedOutReadSets().empty()) {
                    op->SetWaitCompletionFlag(true);
                    return EExecutionStatus::DelayCompleteNoMoreRestarts;
                }
            }

            return std::nullopt;
        };

        try {
            const auto& inReadSets = op->InReadSets();
            auto& awaitingDecisions = op->AwaitingDecisions();
            auto& outReadSets = op->OutReadSets();
            bool useGenericReadSets = true;
            TSysLocks& sysLocks = DataShard.SysLocksTable();

            ui64 consumedMemory = writeTx->GetTxSize();
            if (MaybeRequestMoreTxMemory(consumedMemory, txc)) {
                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << tabletId << " requested " << txc.GetRequestedMemory() << " more memory");

                DataShard.IncCounter(COUNTER_TX_WAIT_RESOURCE);
                return EExecutionStatus::Restart;
            }

            if (guardLocks.LockTxId) {
                auto abortLock = [&]() {
                    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << tabletId << " aborting because it cannot acquire locks");
                    writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Operation is aborting because it cannot acquire locks");
                    writeOp->GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(1);
                    NDataIntegrity::LogVictimDetected(ctx, tabletId, "Write transaction was a victim of broken locks",
                                                      DataShard.SysLocksTable().GetVictimQuerySpanIdForLock(guardLocks.LockTxId),
                                                      guardLocks.QuerySpanId ? TMaybe<ui64>(guardLocks.QuerySpanId) : Nothing());
                    return EExecutionStatus::Executed;
                };

                switch (DataShard.SysLocksTable().EnsureCurrentLock()) {
                    case EEnsureCurrentLock::Success:
                        // Lock is valid, we may continue with reads and side-effects
                        break;

                    case EEnsureCurrentLock::Broken:
                        // Lock is valid, but broken, we could abort early in some
                        // cases, but it doesn't affect correctness.
                        if (!op->IsReadOnly()) {
                            return abortLock();
                        }
                        break;

                    case EEnsureCurrentLock::TooMany:
                        // Lock cannot be created, it's not necessarily a problem
                        // for read-only transactions, for non-readonly we need to
                        // abort;
                        if (!op->IsReadOnly()) {
                            return abortLock();
                        }
                        break;

                    case EEnsureCurrentLock::Abort:
                        // Lock cannot be created and we must abort
                        return abortLock();

                    case EEnsureCurrentLock::Missing:
                        Y_ENSURE(false, "unreachable");
                }
            }

            Y_DEFER {
                // We need to clear OutReadSets and AwaitingDecisions for
                // volatile transactions, except when we commit them.
                if (!keepOutReadSets) {
                    outReadSets.clear();
                    awaitingDecisions.clear();
                }
            };

            auto [validated, brokenLocks] = op->HasVolatilePrepareFlag()
                                                ? KqpValidateVolatileTx(tabletId, sysLocks, kqpLocks, useGenericReadSets, txId, op->DelayedInReadSets(), awaitingDecisions, outReadSets)
                                                : KqpValidateLocks(tabletId, sysLocks, kqpLocks, useGenericReadSets, inReadSets);

            if (!validated) {
                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << tabletId << " aborting because locks are not valid");
                writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Operation is aborting because locks are not valid");
                writeOp->GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(brokenLocks.size());

                // Get victim QuerySpanId from the first broken lock
                TMaybe<ui64> victimQuerySpanId = brokenLocks.empty() ? Nothing()
                    : sysLocks.GetVictimQuerySpanIdForLock(brokenLocks[0].GetLockId());
                NDataIntegrity::LogVictimDetected(ctx, tabletId, "Write transaction was a victim of broken locks",
                                                  victimQuerySpanId,
                                                  guardLocks.QuerySpanId ? TMaybe<ui64>(guardLocks.QuerySpanId) : Nothing());

                for (auto& brokenLock : brokenLocks) {
                    writeOp->GetWriteResult()->Record.MutableTxLocks()->Add()->Swap(&brokenLock);
                }

                KqpEraseLocks(tabletId, kqpLocks, sysLocks);
                auto [_, locksBrokenByTxCleanup] = sysLocks.ApplyLocks();
                HandleBreakerLocks(locksBrokenByTxCleanup, writeOp->GetTxId(), guardLocks.QuerySpanId,
                    sysLocks, writeOp->GetWriteResult()->Record, ctx,
                    "Write transaction aborted, broke other transaction locks during cleanup");
                DataShard.SubscribeNewLocks(ctx);

                if (auto status = ensureAbortOutReadSets()) {
                    return *status;
                }

                if (locksDb.HasChanges()) {
                    op->SetWaitCompletionFlag(true);
                    return EExecutionStatus::ExecutedNoMoreRestarts;
                }
                return EExecutionStatus::Executed;
            }

            const bool isArbiter = op->HasVolatilePrepareFlag() && KqpLocksIsArbiter(tabletId, kqpLocks);

            KqpCommitLocks(tabletId, kqpLocks, sysLocks, userDb);

            if (writeTx->HasOperations()) {
                for (validatedOperationIndex = 0; validatedOperationIndex < writeTx->GetOperations().size(); ++validatedOperationIndex) {
                    const TValidatedWriteTxOperation& validatedOperation = writeTx->GetOperations()[validatedOperationIndex];
                    if (writeOp->WriteRequest) {
                        const auto& protoOperations = writeOp->WriteRequest->Record.GetOperations();
                        if (validatedOperationIndex < static_cast<size_t>(protoOperations.size())) {
                            const auto& protoOp = protoOperations.Get(validatedOperationIndex);
                            if (protoOp.HasQuerySpanId() && protoOp.GetQuerySpanId() != 0) {
                                guardLocks.QuerySpanId = protoOp.GetQuerySpanId();
                            }
                        }
                    }
                    DoUpdateToUserDb(userDb, validatedOperation, txc);
                    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executed write operation for " << *writeOp << " at " << DataShard.TabletID() << ", row count=" << validatedOperation.GetMatrix().GetRowCount());
                }
                validatedOperationIndex = SIZE_MAX;
                DataShard.AddRecentWriteForTli(mvccVersion, guardLocks.QuerySpanId, writeOp->GetTarget().NodeId());
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Skip empty write operation for " << *writeOp << " at " << DataShard.TabletID());
            }

            if (CheckForVolatileReadDependencies(userDb, *writeOp, txc, ctx))
                return EExecutionStatus::Continue;

            writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildCompleted(tabletId, writeOp->GetTxId()));

            auto& writeResult = writeOp->GetWriteResult();
            writeResult->Record.SetOrderId(op->GetTxId());
            if (!op->IsImmediate())
                writeResult->Record.SetStep(op->GetStep());

            if (Pipeline.AddLockDependencies(op, guardLocks)) {
                userDb.ResetCollectedChanges();
                if (txc.DB.HasChanges()) {
                    txc.DB.RollbackChanges();
                }
                writeOp->ReleaseTxData(txc);
                return EExecutionStatus::Continue;
            }

            // Note: any transaction (e.g. immediate or non-volatile) may decide to commit as volatile due to dependencies
            // Such transactions would have no participants and become immediately committed
            auto commitTxIds = userDb.GetVolatileCommitTxIds();
            if (commitTxIds || isArbiter) {
                if (op->GetRemainReadSets()) {
                    for (const auto& [key, received] : inReadSets) {
                        if (received.empty()) {
                            awaitingDecisions.insert(key.first);
                        }
                    }
                }
                TVector<ui64> participants(awaitingDecisions.begin(), awaitingDecisions.end());
                DataShard.GetVolatileTxManager().PersistAddVolatileTx(
                    userDb.GetVolatileTxId(),
                    mvccVersion,
                    commitTxIds,
                    userDb.GetVolatileDependencies(),
                    participants,
                    userDb.GetChangeGroup(),
                    userDb.GetVolatileCommitOrdered(),
                    isArbiter,
                    /* disable expectations */ !participants.empty() && !op->HasVolatilePrepareFlag(),
                    txc
                );
            } else {
                awaitingDecisions.clear();
            }

            if (userDb.GetPerformedUserReads()) {
                op->SetPerformedUserReads(true);
            }

            if (op->HasVolatilePrepareFlag()) {
                // Notify other shards about our expectations as soon as possible, even before we commit
                for (ui64 target : op->AwaitingDecisions()) {
                    if (DataShard.AddExpectation(target, op->GetStep(), op->GetTxId())) {
                        DataShard.SendReadSetExpectation(ctx, op->GetStep(), op->GetTxId(), tabletId, target);
                    }
                }
            }

            if (!op->IsImmediate() && !keepOutReadSets) {
                if (op->OutReadSets().empty() && !op->HasVolatilePrepareFlag()) {
                    KqpFillOutReadSets(outReadSets, kqpLocks, NKikimrTx::TReadSetData::DECISION_COMMIT, tabletId);
                }
                if (!op->OutReadSets().empty()) {
                    DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(), op->PreparedOutReadSets(), txc, ctx);
                    preparedOutReadSets = true;
                }
                keepOutReadSets = true;
            }

            // Note: may erase persistent locks, must be after we persist volatile tx
            AddLocksToResult(writeOp, guardLocks.QuerySpanId, ctx, kqpLocks);

            if (!guardLocks.LockTxId) {
                mvccVersion.ToProto(writeResult->Record.MutableCommitVersion());
            }

            if (auto changes = std::move(userDb.GetCollectedChanges())) {
                op->ChangeRecords() = std::move(changes);
            }

            const auto& counters = userDb.GetCounters();
            KqpUpdateDataShardStatCounters(DataShard, counters);
            KqpFillTxStats(DataShard, counters, *writeResult->Record.MutableTxStats());

        } catch (const TNeedGlobalTxId&) {
            Y_ENSURE(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for write operation with TxId# " << op->GetGlobalTxId());
            Y_ENSURE(op->IsImmediate(),
                "Unexpected TNeedGlobalTxId exception for a non-immediate write operation with TxId# " << op->GetTxId());

            ctx.Send(MakeTxProxyID(),
                new TEvTxUserProxy::TEvAllocateTxId(),
                0, op->GetTxId());
            op->SetWaitingForGlobalTxIdFlag();

            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        } catch (const TNotReadyTabletException&) {
            return OnTabletNotReadyException(userDb, *writeOp, validatedOperationIndex, txc, ctx);
        } catch (const TLockedWriteLimitException&) {
            userDb.ResetCollectedChanges();

            // Note: we don't return TxLocks in the reply since all changes are rolled back and lock is unaffected
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Shard " << tabletId << " cannot write more uncommitted changes");

            // Transaction may have made some changes before it hit the limit,
            // so we need to roll them back.
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            writeOp->ReleaseTxData(txc);
            return EExecutionStatus::Executed;
        } catch (const TUniqueConstrainException&) {
            if (!OnUniqueConstrainException(userDb, *writeOp, guardLocks.LockTxId, guardLocks.QuerySpanId, txc, ctx)) {
                return EExecutionStatus::Continue;
            }

            // INSERT with unique constrain violation are also counted as read for stats.
            const auto& counters = userDb.GetCounters();
            KqpUpdateDataShardStatCounters(DataShard, counters);
            KqpFillTxStats(DataShard, counters, *writeOp->GetWriteResult()->Record.MutableTxStats());

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            writeOp->ReleaseTxData(txc);
            return EExecutionStatus::Executed;
        } catch (const TSerializableIsolationException&) {
            if (CheckForVolatileReadDependencies(userDb, *writeOp, txc, ctx)) {
                return EExecutionStatus::Continue;
            }

            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *writeOp << " at " << DataShard.TabletID() << " aborting. Conflict with another transaction.");
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Write conflict with concurrent transaction.");
            writeOp->GetWriteResult()->Record.MutableTxStats()->SetLocksBrokenAsVictim(1);
            NDataIntegrity::LogVictimDetected(ctx, DataShard.TabletID(), "Write transaction was a victim of broken locks",
                                              guardLocks.LockTxId ? DataShard.SysLocksTable().GetVictimQuerySpanIdForLock(guardLocks.LockTxId) : Nothing(),
                                              guardLocks.QuerySpanId ? TMaybe<ui64>(guardLocks.QuerySpanId) : Nothing());

            ResetChanges(userDb, txc);

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            return EExecutionStatus::Executed;
        } catch (const TKeySizeConstraintException&) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, TStringBuilder() << "Size of key in secondary index is more than " << NLimits::MaxWriteKeySize);
            txc.DB.RollbackChanges();
            LOG_ERROR_S(ctx, NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, "Operation " << *writeOp << " at " << DataShard.TabletID() << " aborting. Size of key of secondary index is too big.");

            if (auto status = ensureAbortOutReadSets()) {
                return *status;
            }

            return EExecutionStatus::Executed;
        }

        Pipeline.AddCommittingOp(op);

        op->ResetCurrentTimer();

        if (txc.DB.HasChanges()) {
            op->SetWaitCompletionFlag(true);
        }

        if (preparedOutReadSets && !op->PreparedOutReadSets().empty()) {
            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        if (!op->PreparedOutReadSets().empty()) {
            DataShard.SendReadSets(ctx, std::move(op->PreparedOutReadSets()));
        }
    }

};  // TExecuteWriteUnit

THolder<TExecutionUnit> CreateExecuteWriteUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TExecuteWriteUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
