#include "datashard_write_operation.h"
#include "datashard_pipeline.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"
#include "datashard_kqp.h"

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

        auto locks = DataShard.SysLocksTable().ApplyLocks();
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
                Y_VERIFY_S(ok, "Unexpected failure to attach TxId# " << writeOp.GetTxId() << " to volatile tx " << txId);
            }

            ResetChanges(userDb, writeOp, txc);

            return true;
        }

        return false;
    }

    EExecutionStatus OnTabletNotReadyException(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Tablet " << DataShard.TabletID() << " is not ready for " << writeOp << " execution");

        DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

        ResetChanges(userDb, writeOp, txc);
        return EExecutionStatus::Restart;
    }

    EExecutionStatus OnUniqueConstrainException(TDataShardUserDb& userDb, TWriteOperation& writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        if (CheckForVolatileReadDependencies(userDb, writeOp, txc, ctx)) 
            return EExecutionStatus::Continue;
        
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << writeOp << " at " << DataShard.TabletID() << " aborting because an duplicate key");
        writeOp.SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "Operation is aborting because an duplicate key");
        ResetChanges(userDb, writeOp, txc);
        return EExecutionStatus::Executed;
    }

    void DoUpdateToUserDb(TDataShardUserDb& userDb, const TValidatedWriteTxOperation& validatedOperation, TTransactionContext& txc) {
        const ui64 tableId = validatedOperation.GetTableId().PathId.LocalPathId;
        const TTableId fullTableId(DataShard.GetPathOwnerId(), tableId);
        const TUserTable& userTable = *DataShard.GetUserTables().at(tableId);

        const NTable::TScheme& scheme = txc.DB.GetScheme();
        const NTable::TScheme::TTableInfo* tableInfo = scheme.GetTableInfo(userTable.LocalTid);

        TSmallVec<TRawTypeValue> key;
        TSmallVec<NTable::TUpdateOp> ops;

        const TSerializedCellMatrix& matrix = validatedOperation.GetMatrix();
        const auto operationType = validatedOperation.GetOperationType();

        auto fillOps = [&](ui32 rowIdx) {
            ops.clear();
            Y_ABORT_UNLESS(matrix.GetColCount() >= userTable.KeyColumnIds.size());
            ops.reserve(matrix.GetColCount() - userTable.KeyColumnIds.size());

            for (ui16 valueColIdx = userTable.KeyColumnIds.size(); valueColIdx < matrix.GetColCount(); ++valueColIdx) {
                ui32 columnTag = validatedOperation.GetColumnIds()[valueColIdx];
                const TCell& cell = matrix.GetCell(rowIdx, valueColIdx);

                NScheme::TTypeInfo vtypeInfo = scheme.GetColumnInfo(tableInfo, columnTag)->PType;
                ops.emplace_back(columnTag, NTable::ECellOp::Set, cell.IsNull() ? TRawTypeValue() : TRawTypeValue(cell.Data(), cell.Size(), vtypeInfo));
            }
        };

        for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx)
        {
            key.clear();
            key.reserve(userTable.KeyColumnIds.size());
            for (ui16 keyColIdx = 0; keyColIdx < userTable.KeyColumnIds.size(); ++keyColIdx) {
                const TCell& cell = matrix.GetCell(rowIdx, keyColIdx);
                ui32 keyCol = tableInfo->KeyColumns[keyColIdx];
                if (cell.IsNull()) {
                    key.emplace_back();
                } else {
                    NScheme::TTypeInfo vtypeInfo = scheme.GetColumnInfo(tableInfo, keyCol)->PType;
                    key.emplace_back(cell.Data(), cell.Size(), vtypeInfo);
                }
            }

            switch (operationType) {
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT: {
                    fillOps(rowIdx);
                    userDb.UpsertRow(fullTableId, key, ops);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE: {
                    fillOps(rowIdx);
                    userDb.ReplaceRow(fullTableId, key, ops);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE: {
                    userDb.EraseRow(fullTableId, key);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT: {
                    fillOps(rowIdx);
                    userDb.InsertRow(fullTableId, key, ops);
                    break;
                }
                case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE: {
                    fillOps(rowIdx);
                    userDb.UpdateRow(fullTableId, key, ops);
                    break;
                }
                default:
                    // Checked before in TWriteOperation
                    Y_FAIL_S(operationType << " operation is not supported now");
            }
        }

        switch (operationType) {
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT:
            case NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE: {
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
                Y_FAIL_S(operationType << " operation is not supported now");
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
            op->MvccReadWriteVersion.reset();
        }

        const TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();

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
                        Y_ABORT_UNLESS(!writeTx->Ready());
                        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, writeTx->GetErrStr());
                        return EExecutionStatus::Executed;
                    }

                    // For planned transactions errors are not expected
                    Y_ABORT("Failed to restore tx data: %s", writeTx->GetErrStr().c_str());
            }
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        if (op->IsImmediate() && !writeOp->ReValidateKeys(txc.DB.GetScheme())) {
            // Immediate transactions may be reordered with schema changes and become invalid
            Y_ABORT_UNLESS(!writeTx->Ready());
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
        const auto [readVersion, writeVersion] = DataShard.GetReadWriteVersions(writeOp);
        
        TDataShardUserDb userDb(DataShard, txc.DB, op->GetGlobalTxId(), readVersion, writeVersion, engineHostCounters, TAppData::TimeProvider->Now());
        userDb.SetIsWriteTx(true);
        userDb.SetIsImmediateTx(op->IsImmediate());
        userDb.SetLockTxId(writeTx->GetLockTxId());
        userDb.SetLockNodeId(writeTx->GetLockNodeId());

        if (op->HasVolatilePrepareFlag()) {
            userDb.SetVolatileTxId(txId);
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
                switch (DataShard.SysLocksTable().EnsureCurrentLock()) {
                    case EEnsureCurrentLock::Success:
                        // Lock is valid, we may continue with reads and side-effects
                        break;

                    case EEnsureCurrentLock::Broken:
                        // Lock is valid, but broken, we could abort early in some
                        // cases, but it doesn't affect correctness.
                        break;

                    case EEnsureCurrentLock::TooMany:
                        // Lock cannot be created, it's not necessarily a problem
                        // for read-only transactions, for non-readonly we need to
                        // abort;
                        if (op->IsReadOnly()) {
                            break;
                        }

                        [[fallthrough]];

                    case EEnsureCurrentLock::Abort:
                        // Lock cannot be created and we must abort
                        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << tabletId << " aborting because it cannot acquire locks");
                        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN, "Operation is aborting because it cannot acquire locks");
                        return EExecutionStatus::Executed;
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
                sysLocks.ApplyLocks();
                DataShard.SubscribeNewLocks(ctx);
                if (locksDb.HasChanges()) {
                    op->SetWaitCompletionFlag(true);
                    return EExecutionStatus::ExecutedNoMoreRestarts;
                }
                return EExecutionStatus::Executed;
            }

            const bool isArbiter = op->HasVolatilePrepareFlag() && KqpLocksIsArbiter(tabletId, kqpLocks);

            KqpCommitLocks(tabletId, kqpLocks, sysLocks, writeVersion, userDb);

            TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();
            if (writeTx->HasOperations()) {
                for (const auto& validatedOperation : writeTx->GetOperations()) {
                    DoUpdateToUserDb(userDb, validatedOperation, txc);
                    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executed write operation for " << *writeOp << " at " << DataShard.TabletID() << ", row count=" << validatedOperation.GetMatrix().GetRowCount());
                }
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
            if (commitTxIds) {
                TVector<ui64> participants(awaitingDecisions.begin(), awaitingDecisions.end());
                DataShard.GetVolatileTxManager().PersistAddVolatileTx(
                    txId,
                    writeVersion,
                    commitTxIds,
                    userDb.GetVolatileDependencies(),
                    participants,
                    userDb.GetChangeGroup(),
                    userDb.GetVolatileCommitOrdered(),
                    isArbiter,
                    txc
                );
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

            if (auto changes = std::move(userDb.GetCollectedChanges())) {
                op->ChangeRecords() = std::move(changes);
            }

            const auto& counters = userDb.GetCounters();
            KqpUpdateDataShardStatCounters(DataShard, counters);
            KqpFillTxStats(DataShard, counters, *writeResult->Record.MutableTxStats());

        } catch (const TNeedGlobalTxId&) {
            Y_VERIFY_S(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for write operation with TxId# " << op->GetGlobalTxId());
            Y_VERIFY_S(op->IsImmediate(),
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
            return OnTabletNotReadyException(userDb, *writeOp, txc, ctx);
        } catch (const TLockedWriteLimitException&) {
            userDb.ResetCollectedChanges();

            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Shard " << tabletId << " cannot write more uncommitted changes");

            for (auto& table : guardLocks.AffectedTables) {
                Y_ABORT_UNLESS(guardLocks.LockTxId);
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
