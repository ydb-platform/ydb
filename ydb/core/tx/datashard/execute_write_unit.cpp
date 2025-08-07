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
        if (op->HasWaitingForGlobalTxIdFlag()) {
            return false;
        }

        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
            return true;
        }

        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    void AddLocksToResult(TWriteOperation* writeOp, const TActorContext& ctx) {
        NEvents::TDataEvents::TEvWriteResult& writeResult = *writeOp->GetWriteResult();

        auto [locks, locksBrokenByTx] = DataShard.SysLocksTable().ApplyLocks();
        NDataIntegrity::LogIntegrityTrailsLocks(ctx, DataShard.TabletID(), writeOp->GetTxId(), locksBrokenByTx);
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

    void ResetChanges(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc) {
        userDb.ResetCollectedChanges();

        writeOp.ReleaseTxData(txc);

        if (txc.DB.HasChanges())
            txc.DB.RollbackChanges();
    }

    bool CheckForVolatileReadDependencies(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        if (!userDb.GetVolatileReadDependencies().empty()) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting because volatile read dependencies");

            for (ui64 txId : userDb.GetVolatileReadDependencies()) {
                writeOp.AddVolatileDependency(txId);
                bool ok = DataShard.GetVolatileTxManager().AttachBlockedOperation(txId, writeOp.GetTxId());
                Y_ENSURE(ok, "Unexpected failure to attach TxId# " << writeOp.GetTxId() << " to volatile tx " << txId);
            }

            ResetChanges(userDb, writeOp, txc);

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

        ResetChanges(userDb, writeOp, txc);
        return EExecutionStatus::Restart;
    }

    EExecutionStatus OnUniqueConstrainException(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        if (CheckForVolatileReadDependencies(userDb, writeOp, txc, ctx)) {
            return EExecutionStatus::Continue;
        }

        if (userDb.GetSnapshotReadConflict()) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting. Conflict with another transaction.");
            writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Read conflict with concurrent transaction.");
            ResetChanges(userDb, writeOp, txc);
            return EExecutionStatus::Executed;
        }

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting. Conflict with existing key.");
        writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, "Conflict with existing key.");
        ResetChanges(userDb, writeOp, txc);
        return EExecutionStatus::Executed;
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
                    userDb.IncrementRow(fullTableId, key, ops);
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

        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
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
            writeOp->ReleaseTxData(txc);
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
            DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
            return EExecutionStatus::Executed;
        }

        NMiniKQL::TEngineHostCounters engineHostCounters;
        const ui64 txId = writeTx->GetTxId();
        const auto mvccVersion = DataShard.GetMvccVersion(writeOp);

        TDataShardUserDb userDb(DataShard, txc.DB, op->GetGlobalTxId(), mvccVersion, engineHostCounters, TAppData::TimeProvider->Now());
        userDb.SetIsWriteTx(true);
        userDb.SetIsImmediateTx(op->IsImmediate());
        userDb.SetLockTxId(writeTx->GetLockTxId());
        userDb.SetLockNodeId(writeTx->GetLockNodeId());

        if (op->HasVolatilePrepareFlag()) {
            userDb.SetVolatileTxId(txId);
        }

        auto mvccSnapshot = writeTx->GetMvccSnapshot();
        if (mvccSnapshot && !writeTx->GetLockTxId()) {
            userDb.SetSnapshotVersion(*mvccSnapshot);
        }

        try {
            const auto* kqpLocks = writeTx->GetKqpLocks() ? &writeTx->GetKqpLocks().value() : nullptr;
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
                }
            }

            bool keepOutReadSets = !op->HasVolatilePrepareFlag();

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

                for (auto& brokenLock : brokenLocks) {
                    writeOp->GetWriteResult()->Record.MutableTxLocks()->Add()->Swap(&brokenLock);
                }

                KqpEraseLocks(tabletId, kqpLocks, sysLocks);
                auto [_, locksBrokenByTx] = sysLocks.ApplyLocks();
                NDataIntegrity::LogIntegrityTrailsLocks(ctx, tabletId, txId, locksBrokenByTx);
                DataShard.SubscribeNewLocks(ctx);
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
                    DoUpdateToUserDb(userDb, validatedOperation, txc);
                    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executed write operation for " << *writeOp << " at " << DataShard.TabletID() << ", row count=" << validatedOperation.GetMatrix().GetRowCount());
                }
                validatedOperationIndex = SIZE_MAX;
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
                writeOp->ReleaseTxData(txc);
                if (txc.DB.HasChanges()) {
                    txc.DB.RollbackChanges();
                }
                return EExecutionStatus::Continue;
            }

            // Note: any transaction (e.g. immediate or non-volatile) may decide to commit as volatile due to dependencies
            // Such transactions would have no participants and become immediately committed
            auto commitTxIds = userDb.GetVolatileCommitTxIds();
            if (commitTxIds || isArbiter) {
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
                if (!op->OutReadSets().empty()) {
                    DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(), op->PreparedOutReadSets(), txc, ctx);
                }
                keepOutReadSets = true;
            }

            // Note: may erase persistent locks, must be after we persist volatile tx
            AddLocksToResult(writeOp, ctx);

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

            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Shard " << tabletId << " cannot write more uncommitted changes");

            for (auto& table : guardLocks.AffectedTables) {
                Y_ENSURE(guardLocks.LockTxId);
                op->Result()->AddTxLock(
                    guardLocks.LockTxId,
                    tabletId,
                    DataShard.Generation(),
                    Max<ui64>(),
                    table.GetTableId().OwnerId,
                    table.GetTableId().LocalPathId,
                    false
                );
            }

            writeOp->ReleaseTxData(txc);

            // Transaction may have made some changes before it hit the limit,
            // so we need to roll them back.
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Executed;
        } catch (const TUniqueConstrainException&) {
            return OnUniqueConstrainException(userDb, *writeOp, txc, ctx);
        } catch (const TKeySizeConstraintException&) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, TStringBuilder() << "Size of key in secondary index is more than " << NLimits::MaxWriteKeySize);
            txc.DB.RollbackChanges();
            LOG_ERROR_S(ctx, NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION, "Operation " << *writeOp << " at " << DataShard.TabletID() << " aborting. Size of key of secondary index is too big.");
                
            return EExecutionStatus::Executed;
        }

        Pipeline.AddCommittingOp(op);

        op->ResetCurrentTimer();

        if (txc.DB.HasChanges()) {
            op->SetWaitCompletionFlag(true);
        }

        if (op->HasVolatilePrepareFlag() && !op->PreparedOutReadSets().empty()) {
            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        if (op->HasVolatilePrepareFlag() && !op->PreparedOutReadSets().empty()) {
            DataShard.SendReadSets(ctx, std::move(op->PreparedOutReadSets()));
        }
    }

};  // TExecuteWriteUnit

THolder<TExecutionUnit> CreateExecuteWriteUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TExecuteWriteUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
