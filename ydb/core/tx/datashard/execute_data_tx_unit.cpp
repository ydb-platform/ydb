#include "datashard_kqp.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"


namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TExecuteDataTxUnit : public TExecutionUnit {
public:
    TExecuteDataTxUnit(TDataShard& dataShard,
                       TPipeline& pipeline);
    ~TExecuteDataTxUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext& txc,
                             const TActorContext& ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext& ctx) override;

private:
    void ExecuteDataTx(TOperation::TPtr op,
                       TTransactionContext& txc,
                       const TActorContext& ctx,
                       TSetupSysLocks& guardLocks);
    void AddLocksToResult(TOperation::TPtr op, const TActorContext& ctx);

private:
    class TRollbackAndWaitException : public yexception {};
};

TExecuteDataTxUnit::TExecuteDataTxUnit(TDataShard& dataShard,
                                       TPipeline& pipeline)
    : TExecutionUnit(EExecutionUnitKind::ExecuteDataTx, true, dataShard, pipeline) {
}

TExecuteDataTxUnit::~TExecuteDataTxUnit() {
}

bool TExecuteDataTxUnit::IsReadyToExecute(TOperation::TPtr op) const {
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
        return true;
    }

    if (DataShard.IsStopping()) {
        // Avoid doing any new work when datashard is stopping
        return false;
    }

    return !op->HasRuntimeConflicts();
}

EExecutionStatus TExecuteDataTxUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext& txc,
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

    IEngineFlat* engine = tx->GetDataTx()->GetEngine();
    Y_VERIFY_S(engine, "missing engine for " << *op << " at " << DataShard.TabletID());

    if (op->IsImmediate() && !tx->ReValidateKeys(txc.DB.GetScheme())) {
        // Immediate transactions may be reordered with schema changes and become invalid
        const auto& dataTx = tx->GetDataTx();
        Y_ABORT_UNLESS(!dataTx->Ready());
        op->SetAbortedFlag();
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
        op->Result()->SetProcessError(dataTx->Code(), dataTx->GetErrors());
        return EExecutionStatus::Executed;
    }

    // TODO: cancel tx in special execution unit.
    if (tx->GetDataTx()->CheckCancelled(DataShard.TabletID()))
        engine->Cancel();
    else {
        ui64 consumed = tx->GetDataTx()->GetTxSize() + engine->GetMemoryAllocated();
        if (MaybeRequestMoreTxMemory(consumed, txc)) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << DataShard.TabletID()
                << " requested " << txc.GetRequestedMemory() << " more memory");

            DataShard.IncCounter(COUNTER_TX_WAIT_RESOURCE);
            return EExecutionStatus::Restart;
        }
        engine->SetMemoryLimit(txc.GetMemoryLimit() - tx->GetDataTx()->GetTxSize());

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
                    op->SetAbortedFlag();
                    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::LOCKS_BROKEN);
                    return EExecutionStatus::Executed;
            }
        }
    }

    try {
        try {
            ExecuteDataTx(op, txc, ctx, guardLocks);
        } catch (const TNotReadyTabletException&) {
            // We want to try pinning (actually precharging) all required pages
            // before restarting the transaction, to minimize future restarts.
            ui64 pageFaultCount = tx->IncrementPageFaultCount();
            engine->PinPages(pageFaultCount);
            throw;
        }
    } catch (const TMemoryLimitExceededException&) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Operation " << *op << " at " << DataShard.TabletID()
            << " exceeded memory limit " << txc.GetMemoryLimit()
            << " and requests " << txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR
            << " more for the next try");

        txc.NotEnoughMemory();
        DataShard.IncCounter(DataShard.NotEnoughMemoryCounter(txc.GetNotEnoughMemoryCount()));

        engine->ReleaseUnusedMemory();
        txc.RequestMemory(txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR);

        tx->GetDataTx()->ResetCollectedChanges();
        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    } catch (const TNotReadyTabletException&) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Tablet " << DataShard.TabletID()
            << " is not ready for " << *op << " execution");

        DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

        tx->GetDataTx()->ResetCollectedChanges();
        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    } catch (const TRollbackAndWaitException&) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Tablet " << DataShard.TabletID()
            << " needs to wait " << *op << " for dependencies");

        tx->GetDataTx()->ResetCollectedChanges();
        tx->ReleaseTxData(txc, ctx);

        if (txc.DB.HasChanges()) {
            txc.DB.RollbackChanges();
        }
        return EExecutionStatus::Continue;
    }

    DataShard.IncCounter(COUNTER_WAIT_EXECUTE_LATENCY_MS, waitExecuteLatency.MilliSeconds());
    DataShard.IncCounter(COUNTER_WAIT_TOTAL_LATENCY_MS, waitTotalLatency.MilliSeconds());
    op->ResetCurrentTimer();

    if (op->IsReadOnly() && !locksDb.HasChanges())
        return EExecutionStatus::Executed;

    if (locksDb.HasChanges()) {
        // We made some changes to locks db, make sure we wait for commit
        op->SetWaitCompletionFlag(true);
    }

    return EExecutionStatus::ExecutedNoMoreRestarts;
}

void TExecuteDataTxUnit::ExecuteDataTx(TOperation::TPtr op,
                                       TTransactionContext& txc,
                                       const TActorContext& ctx,
                                       TSetupSysLocks& guardLocks)
{
    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    IEngineFlat* engine = tx->GetDataTx()->GetEngine();

    DataShard.ReleaseCache(*tx);
    tx->GetDataTx()->ResetCounters();

    auto [readVersion, writeVersion] = DataShard.GetReadWriteVersions(tx);
    tx->GetDataTx()->SetReadVersion(readVersion);
    tx->GetDataTx()->SetWriteVersion(writeVersion);

    // TODO: is it required to always prepare outgoing read sets?
    if (!engine->IsAfterOutgoingReadsetsExtracted()) {
        engine->PrepareOutgoingReadsets();
        engine->AfterOutgoingReadsetsExtracted();
    }

    for (auto& rs : op->InReadSets()) {
        for (auto& rsdata : rs.second) {
            engine->AddIncomingReadset(rsdata.Body);
        }
    }

    if (tx->GetDataTx()->CanCancel()) {
        engine->SetDeadline(tx->GetDataTx()->Deadline());
    }

    IEngineFlat::EResult engineResult = engine->Execute();

    if (Pipeline.AddLockDependencies(op, guardLocks)) {
        throw TRollbackAndWaitException();
    }

    if (engineResult != IEngineFlat::EResult::Ok) {
        TString errorMessage = TStringBuilder() << "Datashard execution error for " << *op << " at "
                                                << DataShard.TabletID() << ": " << engine->GetErrors();

        switch (engineResult) {
            case IEngineFlat::EResult::ResultTooBig:
                LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, errorMessage);
                break;
            case IEngineFlat::EResult::Cancelled:
                LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, errorMessage);
                Y_ABORT_UNLESS(tx->GetDataTx()->CanCancel());
                break;
            default:
                if (op->IsReadOnly() || op->IsImmediate()) {
                    LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD, errorMessage);
                } else {
                    // TODO: Kill only current datashard tablet.
                    Y_FAIL_S("Unexpected execution error in read-write transaction: "
                             << errorMessage);
                }
                break;
        }
    }

    if (engineResult == IEngineFlat::EResult::Cancelled)
        DataShard.IncCounter(op->IsImmediate()
                                 ? COUNTER_IMMEDIATE_TX_CANCELLED
                                 : COUNTER_PLANNED_TX_CANCELLED);

    auto& result = BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    result->Record.SetOrderId(op->GetTxId());
    if (!op->IsImmediate())
        result->Record.SetStep(op->GetStep());

    if (engine->GetStatus() == IEngineFlat::EStatus::Error) {
        result->SetExecutionError(ConvertErrCode(engineResult), engine->GetErrors());
    } else {
        result->SetTxResult(engine->GetShardReply(DataShard.TabletID()));

        op->ChangeRecords() = std::move(tx->GetDataTx()->GetCollectedChanges());
    }

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Executed operation " << *op << " at tablet " << DataShard.TabletID()
                                      << " with status " << result->GetStatus());

    auto& counters = tx->GetDataTx()->GetCounters();

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Datashard execution counters for " << *op << " at "
                                                    << DataShard.TabletID() << ": " << counters.ToString());

    KqpUpdateDataShardStatCounters(DataShard, counters);
    if (tx->GetDataTx()->CollectStats()) {
        KqpFillTxStats(DataShard, counters, *result->Record.MutableTxStats());
    }

    if (counters.InvisibleRowSkips && op->LockTxId()) {
        DataShard.SysLocksTable().BreakSetLocks();
    }

    // Note: any transaction (e.g. immediate or non-volatile) may decide to commit as volatile due to dependencies
    // Such transactions would have no participants and become immediately committed
    if (auto commitTxIds = tx->GetDataTx()->GetVolatileCommitTxIds()) {
        TVector<ui64> participants; // empty participants
        DataShard.GetVolatileTxManager().PersistAddVolatileTx(
            tx->GetTxId(),
            writeVersion,
            commitTxIds,
            tx->GetDataTx()->GetVolatileDependencies(),
            participants,
            tx->GetDataTx()->GetVolatileChangeGroup(),
            tx->GetDataTx()->GetVolatileCommitOrdered(),
            /* arbiter */ false,
            txc);
    }

    if (tx->GetDataTx()->GetPerformedUserReads()) {
        tx->SetPerformedUserReads(true);
    }

    AddLocksToResult(op, ctx);

    Pipeline.AddCommittingOp(op);
}

void TExecuteDataTxUnit::AddLocksToResult(TOperation::TPtr op, const TActorContext& ctx) {
    auto locks = DataShard.SysLocksTable().ApplyLocks();
    for (const auto& lock : locks) {
        if (lock.IsError()) {
            LOG_NOTICE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                         "Lock is not set for " << *op << " at " << DataShard.TabletID()
                                                << " lock " << lock);
        }
        op->Result()->AddTxLock(lock.LockId, lock.DataShard, lock.Generation, lock.Counter,
                                lock.SchemeShard, lock.PathId, lock.HasWrites);
    }
    DataShard.SubscribeNewLocks(ctx);
}

void TExecuteDataTxUnit::Complete(TOperation::TPtr, const TActorContext&) {
}

THolder<TExecutionUnit> CreateExecuteDataTxUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TExecuteDataTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
