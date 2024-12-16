#include "datashard_impl.h"
#include "datashard_kqp.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"
#include "probes.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

#define LOG_T(stream) LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_C(stream) LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD, stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, stream)

class TExecuteKqpDataTxUnit : public TExecutionUnit {
public:
    TExecuteKqpDataTxUnit(TDataShard& dataShard, TPipeline& pipeline);
    ~TExecuteKqpDataTxUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(TOperation::TPtr op, const TActorContext& ctx) override;

private:
    void AddLocksToResult(TOperation::TPtr op, const TActorContext& ctx);
    EExecutionStatus OnTabletNotReady(TActiveTransaction& tx, TValidatedDataTx& dataTx, TTransactionContext& txc,
                                      const TActorContext& ctx);
};

TExecuteKqpDataTxUnit::TExecuteKqpDataTxUnit(TDataShard& dataShard, TPipeline& pipeline)
    : TExecutionUnit(EExecutionUnitKind::ExecuteKqpDataTx, true, dataShard, pipeline) {}

TExecuteKqpDataTxUnit::~TExecuteKqpDataTxUnit() {}

bool TExecuteKqpDataTxUnit::IsReadyToExecute(TOperation::TPtr op) const {
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
        return true;
    }

    if (DataShard.IsStopping()) {
        // Avoid doing any new work when datashard is stopping
        return false;
    }

    return !op->HasRuntimeConflicts();
}

EExecutionStatus TExecuteKqpDataTxUnit::Execute(TOperation::TPtr op, TTransactionContext& txc,
    const TActorContext& ctx)
{
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
        return EExecutionStatus::Executed;
    }

    // We remember current time now, but will only count it when transaction succeeds
    TDuration waitExecuteLatency = op->GetCurrentElapsed();
    TDuration waitTotalLatency = op->GetTotalElapsed();

    if (op->IsImmediate()) {
        // Every time we execute immediate transaction we may choose a new mvcc version
        op->MvccReadWriteVersion.reset();
    }

    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    DataShard.ReleaseCache(*tx);

    if (tx->IsTxDataReleased()) {
        switch (Pipeline.RestoreDataTx(tx, txc, ctx)) {
            case ERestoreDataStatus::Ok:
                break;

            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;

            case ERestoreDataStatus::Error:
                // For immediate transactions we want to translate this into a propose failure
                if (op->IsImmediate()) {
                    const auto& dataTx = tx->GetDataTx();
                    Y_ABORT_UNLESS(!dataTx->Ready());
                    op->SetAbortedFlag();
                    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
                    op->Result()->SetProcessError(dataTx->Code(), dataTx->GetErrors());
                    return EExecutionStatus::Executed;
                }

                // For planned transactions errors are not expected
                Y_ABORT("Failed to restore tx data: %s", tx->GetDataTx()->GetErrors().c_str());
        }
    }

    TDataShardLocksDb locksDb(DataShard, txc);
    TSetupSysLocks guardLocks(op, DataShard, &locksDb);

    ui64 tabletId = DataShard.TabletID();
    const TValidatedDataTx::TPtr& dataTx = tx->GetDataTx();

    if (op->IsImmediate() && !dataTx->ReValidateKeys(txc.DB.GetScheme())) {
        // Immediate transactions may be reordered with schema changes and become invalid
        Y_ABORT_UNLESS(!dataTx->Ready());
        op->SetAbortedFlag();
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
        op->Result()->SetProcessError(dataTx->Code(), dataTx->GetErrors());
        return EExecutionStatus::Executed;
    }

    if (dataTx->CheckCancelled(DataShard.TabletID())) {
        tx->ReleaseTxData(txc, ctx);
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED)
            ->AddError(NKikimrTxDataShard::TError::EXECUTION_CANCELLED, "Tx was cancelled");

        DataShard.IncCounter(op->IsImmediate() ? COUNTER_IMMEDIATE_TX_CANCELLED : COUNTER_PLANNED_TX_CANCELLED);

        return EExecutionStatus::Executed;
    }

    try {
        const ui64 txId = tx->GetTxId();
        const auto* kqpLocks = tx->GetDataTx()->HasKqpLocks() ? &tx->GetDataTx()->GetKqpLocks() : nullptr;
        const auto& inReadSets = op->InReadSets();
        auto& awaitingDecisions = tx->AwaitingDecisions();
        auto& outReadSets = tx->OutReadSets();
        bool useGenericReadSets = dataTx->GetUseGenericReadSets();
        auto& tasksRunner = dataTx->GetKqpTasksRunner();
        TSysLocks& sysLocks = DataShard.SysLocksTable();

        ui64 consumedMemory = dataTx->GetTxSize() + tasksRunner.GetAllocatedMemory();
        if (MaybeRequestMoreTxMemory(consumedMemory, txc)) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << DataShard.TabletID()
                << " requested " << txc.GetRequestedMemory() << " more memory");

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
                    LOG_T("Operation " << *op << " (execute_kqp_data_tx) at " << tabletId
                        << " aborting because it cannot acquire locks");

                    op->SetAbortedFlag();
                    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::LOCKS_BROKEN);
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
            ? KqpValidateVolatileTx(tabletId, sysLocks, kqpLocks, useGenericReadSets,
                txId, tx->DelayedInReadSets(), awaitingDecisions, outReadSets)
            : KqpValidateLocks(tabletId, sysLocks, kqpLocks, useGenericReadSets, inReadSets);

        if (!validated) {
            tx->Result() = MakeHolder<TEvDataShard::TEvProposeTransactionResult>(
                NKikimrTxDataShard::TX_KIND_DATA,
                tabletId,
                txId,
                NKikimrTxDataShard::TEvProposeTransactionResult::LOCKS_BROKEN
            );

            for (auto& brokenLock : brokenLocks) {
                tx->Result()->Record.MutableTxLocks()->Add()->Swap(&brokenLock);
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

        auto allocGuard = tasksRunner.BindAllocator(txc.GetMemoryLimit() - dataTx->GetTxSize());

        NKqp::GetKqpResourceManager()->GetCounters()->RmExternalMemory->Add(txc.GetMemoryLimit());
        Y_DEFER {
            NKqp::GetKqpResourceManager()->GetCounters()->RmExternalMemory->Sub(txc.GetMemoryLimit());
        };

        LOG_T("Operation " << *op << " (execute_kqp_data_tx) at " << tabletId
            << " set memory limit " << (txc.GetMemoryLimit() - dataTx->GetTxSize()));

        auto execCtx = DefaultKqpExecutionContext();
        tasksRunner.Prepare(DefaultKqpDataReqMemoryLimits(), *execCtx);

        auto [readVersion, writeVersion] = DataShard.GetReadWriteVersions(tx);
        dataTx->SetReadVersion(readVersion);
        dataTx->SetWriteVersion(writeVersion);

        if (op->HasVolatilePrepareFlag()) {
            dataTx->SetVolatileTxId(txId);
        }

        LWTRACK(ProposeTransactionKqpDataExecute, op->Orbit);

        const bool isArbiter = op->HasVolatilePrepareFlag() && KqpLocksIsArbiter(tabletId, kqpLocks);

        KqpCommitLocks(tabletId, kqpLocks, sysLocks, writeVersion, tx->GetDataTx()->GetUserDb());

        auto& computeCtx = tx->GetDataTx()->GetKqpComputeCtx();

        auto result = KqpCompleteTransaction(ctx, tabletId, op->GetTxId(),
            op->HasKqpAttachedRSFlag() ? nullptr : &op->InReadSets(), useGenericReadSets, tasksRunner, computeCtx);

        if (!result && computeCtx.HadInconsistentReads()) {
            LOG_T("Operation " << *op << " (execute_kqp_data_tx) at " << tabletId
                << " detected inconsistent reads and is going to abort");

            allocGuard.Release();

            dataTx->ResetCollectedChanges();

            op->SetAbortedFlag();

            // NOTE: we don't actually break locks, and rollback everything
            // instead, so transaction may continue with different reads that
            // won't conflict. This should not be considered a feature though,
            // it's just that actually breaking this (potentially persistent)
            // lock and rolling back changes will be unnecessarily complicated.
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::LOCKS_BROKEN);

            // Add a list of "broken" table locks to the result. It may be the
            // case that the lock is not even set yet (write + read with conflicts),
            // but we want kqp to have a list of affected tables, which is used
            // when generating error messages.
            // TODO: we would want an actual table id that caused inconsistency,
            //       relevant for future multi-table shards only.
            // NOTE: generation may not match an existing lock, but it's not a problem.
            for (auto& table : guardLocks.AffectedTables) {
                Y_ABORT_UNLESS(guardLocks.LockTxId);
                op->Result()->AddTxLock(
                    guardLocks.LockTxId,
                    DataShard.TabletID(),
                    DataShard.Generation(),
                    Max<ui64>(),
                    table.GetTableId().OwnerId,
                    table.GetTableId().LocalPathId,
                    false);
            }

            tx->ReleaseTxData(txc, ctx);

            // Transaction may have made some changes before it detected
            // inconsistency, so we need to roll them back.
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Executed;
        }

        if (!result && computeCtx.IsTabletNotReady()) {
            allocGuard.Release();
            return OnTabletNotReady(*tx, *dataTx, txc, ctx);
        }

        if (!result && computeCtx.HasVolatileReadDependencies()) {
            for (ui64 txId : computeCtx.GetVolatileReadDependencies()) {
                op->AddVolatileDependency(txId);
                bool ok = DataShard.GetVolatileTxManager().AttachBlockedOperation(txId, op->GetTxId());
                Y_VERIFY_S(ok, "Unexpected failure to attach TxId# " << op->GetTxId() << " to volatile tx " << txId);
            }

            allocGuard.Release();

            dataTx->ResetCollectedChanges();

            tx->ReleaseTxData(txc, ctx);

            // Rollback database changes, if any
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }

            return EExecutionStatus::Continue;
        }

        if (Pipeline.AddLockDependencies(op, guardLocks)) {
            allocGuard.Release();
            dataTx->ResetCollectedChanges();
            tx->ReleaseTxData(txc, ctx);
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        }

        Y_ABORT_UNLESS(result);
        op->Result().Swap(result);
        op->SetKqpAttachedRSFlag();

        if (dataTx->GetCounters().InvisibleRowSkips && op->LockTxId()) {
            sysLocks.BreakSetLocks();
        }

        // Note: any transaction (e.g. immediate or non-volatile) may decide to commit as volatile due to dependencies
        // Such transactions would have no participants and become immediately committed
        auto commitTxIds = dataTx->GetVolatileCommitTxIds();
        if (commitTxIds) {
            TVector<ui64> participants(awaitingDecisions.begin(), awaitingDecisions.end());
            DataShard.GetVolatileTxManager().PersistAddVolatileTx(
                txId,
                writeVersion,
                commitTxIds,
                dataTx->GetVolatileDependencies(),
                participants,
                dataTx->GetVolatileChangeGroup(),
                dataTx->GetVolatileCommitOrdered(),
                isArbiter,
                txc);
        }

        if (dataTx->GetPerformedUserReads()) {
            op->SetPerformedUserReads(true);
        }

        if (op->HasVolatilePrepareFlag()) {
            // Notify other shards about our expectations as soon as possible, even before we commit
            for (ui64 target : op->AwaitingDecisions()) {
                if (DataShard.AddExpectation(target, op->GetStep(), op->GetTxId())) {
                    DataShard.SendReadSetExpectation(ctx, op->GetStep(), op->GetTxId(), DataShard.TabletID(), target);
                }
            }
            if (!op->OutReadSets().empty()) {
                DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(), op->PreparedOutReadSets(), txc, ctx);
            }
            keepOutReadSets = true;
        }

        // Note: may erase persistent locks, must be after we persist volatile tx
        AddLocksToResult(op, ctx);

        if (auto changes = std::move(dataTx->GetCollectedChanges())) {
            op->ChangeRecords() = std::move(changes);
        }

        KqpUpdateDataShardStatCounters(DataShard, dataTx->GetCounters());
        auto statsMode = dataTx->GetKqpStatsMode();
        KqpFillStats(DataShard, tasksRunner, computeCtx, statsMode, *op->Result());
    } catch (const TMemoryLimitExceededException&) {
        dataTx->ResetCollectedChanges();

        txc.NotEnoughMemory();

        LOG_T("Operation " << *op << " at " << tabletId
            << " exceeded memory limit " << txc.GetMemoryLimit()
            << " and requests " << txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR
            << " more for the next try (" << txc.GetNotEnoughMemoryCount() << ")");

        DataShard.IncCounter(DataShard.NotEnoughMemoryCounter(txc.GetNotEnoughMemoryCount()));

        txc.RequestMemory(txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR);
        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    } catch (const TNotReadyTabletException&) {
        return OnTabletNotReady(*tx, *dataTx, txc, ctx);
    } catch (const TLockedWriteLimitException&) {
        dataTx->ResetCollectedChanges();

        op->SetAbortedFlag();

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR);

        op->Result()->AddError(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED,
            TStringBuilder() << "Shard " << DataShard.TabletID() << " cannot write more uncommitted changes");

        for (auto& table : guardLocks.AffectedTables) {
            Y_ABORT_UNLESS(guardLocks.LockTxId);
            op->Result()->AddTxLock(
                guardLocks.LockTxId,
                DataShard.TabletID(),
                DataShard.Generation(),
                Max<ui64>(),
                table.GetTableId().OwnerId,
                table.GetTableId().LocalPathId,
                false);
        }

        tx->ReleaseTxData(txc, ctx);

        // Transaction may have made some changes before it hit the limit,
        // so we need to roll them back.
        if (txc.DB.HasChanges()) {
            txc.DB.RollbackChanges();
        }
        return EExecutionStatus::Executed;
    } catch (const yexception& e) {
        LOG_C("Exception while executing KQP transaction " << *op << " at " << tabletId << ": " << e.what());
        if (op->IsReadOnly() || op->IsImmediate()) {
            tx->ReleaseTxData(txc, ctx);
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR)
                ->AddError(NKikimrTxDataShard::TError::UNKNOWN, TStringBuilder() << "Tx was terminated: " << e.what());
            return EExecutionStatus::Executed;
        } else {
            Y_FAIL_S("Unexpected exception in KQP transaction execution: " << e.what());
        }
    }

    Pipeline.AddCommittingOp(op);

    DataShard.IncCounter(COUNTER_WAIT_EXECUTE_LATENCY_MS, waitExecuteLatency.MilliSeconds());
    DataShard.IncCounter(COUNTER_WAIT_TOTAL_LATENCY_MS, waitTotalLatency.MilliSeconds());
    op->ResetCurrentTimer();

    if (txc.DB.HasChanges()) {
        op->SetWaitCompletionFlag(true);
    } else if (op->IsReadOnly()) {
        return EExecutionStatus::Executed;
    }

    if (op->HasVolatilePrepareFlag() && !op->PreparedOutReadSets().empty()) {
        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    return EExecutionStatus::ExecutedNoMoreRestarts;
}

void TExecuteKqpDataTxUnit::AddLocksToResult(TOperation::TPtr op, const TActorContext& ctx) {
    auto locks = DataShard.SysLocksTable().ApplyLocks();
    LOG_T("add locks to result: " << locks.size());
    for (const auto& lock : locks) {
        if (lock.IsError()) {
            LOG_NOTICE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, "Lock is not set for "
                << *op << " at " << DataShard.TabletID() << " lock " << lock);
        }

        op->Result()->AddTxLock(lock.LockId, lock.DataShard, lock.Generation, lock.Counter,
            lock.SchemeShard, lock.PathId, lock.HasWrites);

        LOG_T("add lock to result: " << op->Result()->Record.GetTxLocks().rbegin()->ShortDebugString());
    }
    DataShard.SubscribeNewLocks(ctx);
}

EExecutionStatus TExecuteKqpDataTxUnit::OnTabletNotReady(TActiveTransaction& tx, TValidatedDataTx& dataTx,
    TTransactionContext& txc, const TActorContext& ctx)
{
    LOG_T("Tablet " << DataShard.TabletID() << " is not ready for " << tx << " execution");

    DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

    dataTx.ResetCollectedChanges();

    ui64 pageFaultCount = tx.IncrementPageFaultCount();
    dataTx.GetKqpComputeCtx().PinPages(dataTx.TxInfo().Keys, pageFaultCount);

    tx.ReleaseTxData(txc, ctx);
    return EExecutionStatus::Restart;
}

void TExecuteKqpDataTxUnit::Complete(TOperation::TPtr op, const TActorContext& ctx) {
    if (op->HasVolatilePrepareFlag() && !op->PreparedOutReadSets().empty()) {
        DataShard.SendReadSets(ctx, std::move(op->PreparedOutReadSets()));
    }
}

THolder<TExecutionUnit> CreateExecuteKqpDataTxUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TExecuteKqpDataTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
