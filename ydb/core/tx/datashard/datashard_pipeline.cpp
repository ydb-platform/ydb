#include "defs.h"

#include "datashard_pipeline.h"
#include "datashard_impl.h"
#include "datashard_txs.h"
#include "datashard_write_operation.h"

#include <ydb/core/base/cputime.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/tx/balance_coverage/balance_coverage_builder.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr {
namespace NDataShard {

TPipeline::TPipeline(TDataShard * self)
    : Self(self)
    , DepTracker(self)
    , ActivePlannedOpsLogicallyCompleteEnd(ActivePlannedOps.end())
    , ActivePlannedOpsLogicallyIncompleteEnd(ActivePlannedOps.end())
    , LastPlannedTx(0, 0)
    , LastCompleteTx(0, 0)
    , UtmostCompleteTx(0, 0)
    , KeepSchemaStep(0)
    , LastCleanupTime(0)
    , SchemaTx(nullptr)
{
    for (ui32 i = 0; i < static_cast<ui32>(EExecutionUnitKind::Count); ++i)
        ExecutionUnits[i] = CreateExecutionUnit(static_cast<EExecutionUnitKind>(i), *Self, *this);
}

TPipeline::~TPipeline()
{
    for (auto &pr : ActiveOps) {
        pr.second->ClearDependents();
        pr.second->ClearDependencies();
        pr.second->ClearSpecialDependents();
        pr.second->ClearSpecialDependencies();
        pr.second->ClearPlannedConflicts();
        pr.second->ClearImmediateConflicts();
        pr.second->ClearRepeatableReadConflicts();
    }
}

bool TPipeline::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    Y_ABORT_UNLESS(!SchemaTx);

    bool ready = true;
    ready &= Self->SysGetUi64(db, Schema::Sys_LastPlannedStep, LastPlannedTx.Step);
    ready &= Self->SysGetUi64(db, Schema::Sys_LastPlannedTx, LastPlannedTx.TxId);
    ready &= Self->SysGetUi64(db, Schema::Sys_LastCompleteStep, LastCompleteTx.Step);
    ready &= Self->SysGetUi64(db, Schema::Sys_LastCompleteTx, LastCompleteTx.TxId);
    ready &= Self->SysGetUi64(db, Schema::Sys_AliveStep, KeepSchemaStep);
    ready &= Self->SysGetUi64(db, Schema::SysPipeline_Flags, Config.Flags);
    ready &= Self->SysGetUi64(db, Schema::SysPipeline_LimitActiveTx, Config.LimitActiveTx);
    ready &= Self->SysGetUi64(db, Schema::SysPipeline_LimitDataTxCache, Config.LimitDataTxCache);
    if (!ready) {
        return false;
    }

    UtmostCompleteTx = LastCompleteTx;

    return true;
}

void TPipeline::PersistConfig(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    Self->PersistSys(db, Schema::SysPipeline_Flags, Config.Flags);
    Self->PersistSys(db, Schema::SysPipeline_LimitActiveTx, Config.LimitActiveTx);
    Self->PersistSys(db, Schema::SysPipeline_LimitDataTxCache, Config.LimitDataTxCache);
}

void TPipeline::UpdateConfig(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TPipelineConfig& cfg) {
    Config.Update(cfg);
    PersistConfig(db);
}

bool TPipeline::AddImmediateOp(TOperation::TPtr op)
{
    auto p = ImmediateOps.emplace(op->GetTxId(), op);
    Self->SetCounter(COUNTER_IMMEDIATE_TX_IN_FLY, ImmediateOps.size());
    return p.second;
}

void TPipeline::RemoveImmediateOp(TOperation::TPtr op)
{
    ImmediateOps.erase(op->GetTxId());
    Self->SetCounter(COUNTER_IMMEDIATE_TX_IN_FLY, ImmediateOps.size());
}

bool TPipeline::OutOfOrderLimits() const
{
    auto completed = GetExecutionUnit(EExecutionUnitKind::CompletedOperations).GetInFly();
    auto activePlanned = ActivePlannedOps.size() - completed;

    if (!Config.OutOfOrder())
        return activePlanned;
    if (activePlanned >= Config.LimitActiveTx)
        return true;
    if (completed >= Config.LimitDoneDataTx)
        return true;
    return false;
}

bool TPipeline::CanRunAnotherOp()
{
    if (GetNextActiveOp(true))
        return true;
    return false;
}

bool TPipeline::CanRunOp(const TOperation &op) const {
    if (!op.Exists())
        return false;

    const TSchemaOperation *schemaOp = Self->TransQueue.FindSchemaTx(op.GetTxId());
    if (schemaOp) {
        if (schemaOp->Type == TSchemaOperation::ETypeDrop && !Self->CanDrop()) {
            return false;
        }
    }
    return true;
}

TDuration TPipeline::CleanupTimeout() const {
    ui64 nextDeadlineStep = Self->TransQueue.NextDeadlineStep();
    if (nextDeadlineStep == Max<ui64>())
        return TDuration::Max();
    ui64 outdatedStep = Self->GetOutdatedCleanupStep();
    if (nextDeadlineStep >= outdatedStep)
        return TDuration::MilliSeconds(nextDeadlineStep - outdatedStep);
    return TDuration::Zero();
}

ECleanupStatus TPipeline::Cleanup(NIceDb::TNiceDb& db, const TActorContext& ctx,
        std::vector<std::unique_ptr<IEventHandle>>& replies)
{
    bool foundExpired = false;
    TOperation::TPtr op;
    ui64 step = 0;
    ui64 txId = 0;
    while (!op) {
        Self->TransQueue.GetPlannedTxId(step, txId);

        if (txId == 0)
            break;

        op = Self->TransQueue.FindTxInFly(txId);

        if (!op) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                        "TX [" << step << ":" << txId << "] is already executed or expired at tablet "
                        << Self->TabletID());

            ui64 lastStep = LastPlannedTx.Step;
            Y_ABORT_UNLESS(lastStep >= step,
                     "TX [%" PRIu64 ":%" PRIu64 "] is unknown while LastPlannedStep is %" PRIu64 " at tablet %" PRIu64,
                     step, txId, lastStep, Self->TabletID());

            Self->TransQueue.ForgetPlannedTx(db, step, txId);
            db.NoMoreReadsForTx();
            foundExpired = true;

            // Local DB Tx doesn't see it's own updates, so if we erase a row we must move to the next key
            ++txId;
            if (txId == 0)
                ++step;
        }
    }

    if (foundExpired) {
        // We performed some db writes, cannot continue
        return ECleanupStatus::Success;
    }

    // cleaunup outdated
    ui64 outdatedStep = Self->GetOutdatedCleanupStep();
    auto status = CleanupOutdated(db, ctx, outdatedStep, replies);
    switch (status) {
        case ECleanupStatus::None:
            if (!op || !CanRunOp(*op)) {
                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "No cleanup at " << Self->TabletID() << " outdated step " << outdatedStep
                    << " last cleanup " << LastCleanupTime);
            }
            break;

        case ECleanupStatus::Restart:
            break;

        case ECleanupStatus::Success:
            // Outdated op removal might enable scheme op proposal.
            MaybeActivateWaitingSchemeOps(ctx);
            // Outdated op removal might enable another op execution.
            // N.B.: actually this happens only for DROP which waits
            // for TxInFly having only the DROP.
            // N.B.: compatibility with older datashard versions.
            AddCandidateUnit(EExecutionUnitKind::PlanQueue);
            Self->PlanQueue.Progress(ctx);
            break;
    }

    return status;
}

bool TPipeline::IsReadyOp(TOperation::TPtr op)
{
    Y_VERIFY_DEBUG_S(!op->IsInProgress(),
                     "Found in-progress candidate operation " << op->GetKind()
                     << " " << *op << " executing in " << op->GetCurrentUnit()
                     << " at " << Self->TabletID());
    if (op->IsInProgress()) {
        LOG_CRIT_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                   "Found in-progress candidate operation " << op->GetKind()
                   << " " << *op << " executing in " << op->GetCurrentUnit()
                   << " at " << Self->TabletID());
        return false;
    }

    Y_VERIFY_DEBUG_S(!op->IsExecutionPlanFinished(),
                     "Found finished candidate operation " << op->GetKind()
                     << " " << *op << " at " << Self->TabletID());
    if (op->IsExecutionPlanFinished()) {
        LOG_CRIT_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                   "Found finished candidate operation " << op->GetKind()
                   << " " << *op << " at " << Self->TabletID());
        return false;
    }

    auto &unit = GetExecutionUnit(op->GetCurrentUnit());
    return unit.IsReadyToExecute(op);
}

TOperation::TPtr TPipeline::GetNextActiveOp(bool dryRun)
{
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                "GetNextActiveOp at " << Self->TabletID()
                << (dryRun ? " (dry run)" : "")
                << " active " << ActiveOps.size()
                << " active planned " << ActivePlannedOps.size()
                << " immediate " << ImmediateOps.size()
                << " planned " << Self->TransQueue.TxInFly());

    THashSet<TOperation::TPtr> checkedOps;
    THashSet<EExecutionUnitKind> checkedUnits;

    // Check if we have cached result from previous dry run.
    if (NextActiveOp) {
        if (IsReadyOp(NextActiveOp)) {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Return cached ready operation " << *NextActiveOp << " at "
                        << Self->TabletID());

            TOperation::TPtr res = NextActiveOp;
            if (!dryRun)
                NextActiveOp = nullptr;
            return res;
        } else {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Cached ready operation " << NextActiveOp->GetKind()
                        << " " << *NextActiveOp << " is not ready anymore for "
                        << NextActiveOp->GetCurrentUnit() << " at " << Self->TabletID());

            NextActiveOp = nullptr;
        }
    }

    // First look through candidate operations in
    // StepOrder order.
    while (!CandidateOps.empty()) {
        auto op = CandidateOps.begin()->second;
        CandidateOps.erase(CandidateOps.begin());

        if (IsReadyOp(op)) {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Found ready candidate operation " << *op << " at "
                        << Self->TabletID() << " for " << op->GetCurrentUnit());
            if (dryRun)
                NextActiveOp = op;
            return op;
        }

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Candidate operation " << *op << " at " << Self->TabletID()
                    << " is not ready for " << op->GetCurrentUnit());

        checkedOps.insert(op);
    }

    // Now look at candidate units. Start from units
    // with higher kind values which are supposed to
    // hold operations on later execution stages.
    while (!CandidateUnits.empty()) {
        auto &unit = GetExecutionUnit(*CandidateUnits.rbegin());

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Check candidate unit " << unit.GetKind() << " at " << Self->TabletID());

        auto op = unit.FindReadyOperation();
        if (op) {
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Found ready operation " << *op << " in " << unit.GetKind()
                        << " unit at " << Self->TabletID());
            if (dryRun)
                NextActiveOp = op;
            return op;
        }

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Unit " << unit.GetKind() << " has no ready operations at "
                    << Self->TabletID());

        checkedUnits.insert(unit.GetKind());
        CandidateUnits.erase(*CandidateUnits.rbegin());
    }

    // If candidates have no ready operation then check all other
    // running and waiting operations. Also check plan queue.
    // Such checks are done to find issues with proper candidates
    // identification. In release mode error is logged, in debug
    // mode just fail.
    for (auto &pr : ActiveOps) {
        const TOperation::TPtr &op = pr.second;
        Y_ABORT_UNLESS(!op->IsExecutionPlanFinished());

        if (checkedOps.contains(op) || op->IsInProgress())
            continue;

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Check active operation " << *op << " at " << Self->TabletID()
                    << " on unit " << op->GetCurrentUnit());

        bool ready = IsReadyOp(op);

        Y_VERIFY_DEBUG_S(!ready, "Found unexpected ready active operation " << *op
                         << " in " << op->GetCurrentUnit());

        if (ready) {
            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "TryGetExistingTx at " << Self->TabletID() << " return waiting " << *op);
            if (dryRun)
                NextActiveOp = op;
            return op;
        }

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Active operation " << *op << " at " << Self->TabletID()
                    << " is not ready for " << op->GetCurrentUnit());
    }

    if (!checkedUnits.contains(EExecutionUnitKind::PlanQueue)) {
        auto &unit = GetExecutionUnit(EExecutionUnitKind::PlanQueue);

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Check unit " << unit.GetKind() << " at " << Self->TabletID());

        auto op = unit.FindReadyOperation();

        Y_VERIFY_DEBUG_S(!op, "Found unexpected ready operation in plan queue " << *op
                         << " at " << Self->TabletID());

        if (op) {
            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Found ready operation " << *op << " in " << unit.GetKind()
                        << " unit at " << Self->TabletID());
            if (dryRun)
                NextActiveOp = op;
            return op;
        }

        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Unit " << unit.GetKind() << " has no ready operations at "
                    << Self->TabletID());
    }

    return nullptr;
}

TOperation::TPtr TPipeline::GetNextPlannedOp(ui64 step, ui64 txId) const {
    TOperation::TPtr op;
    while (!op) {
        if (!Self->TransQueue.GetNextPlannedTxId(step, txId))
            return nullptr;

        op = Self->TransQueue.FindTxInFly(txId);
    }

    return op;
}

void TPipeline::AddActiveOp(TOperation::TPtr op)
{
    if (op->IsImmediate()) {
        AddImmediateOp(op);
    } else {
        // Restore possibly missing flags based on current mvcc edges
        if (Self->IsMvccEnabled()) {
            TStepOrder stepOrder = op->GetStepOrder();
            TRowVersion version(stepOrder.Step, stepOrder.TxId);
            if (version <= Self->SnapshotManager.GetCompleteEdge() ||
                version < Self->SnapshotManager.GetImmediateWriteEdge() ||
                version < Self->SnapshotManager.GetUnprotectedReadEdge())
            {
                // This transaction would have been marked as logically complete
                if (!op->HasFlag(TTxFlags::BlockingImmediateOps)) {
                    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Adding BlockingImmediateOps for op " << *op << " at " << Self->TabletID());
                    op->SetFlag(TTxFlags::BlockingImmediateOps);
                }
            } else if (version <= Self->SnapshotManager.GetIncompleteEdge() ||
                       version <= Self->SnapshotManager.GetUnprotectedReadEdge())
            {
                // This transaction would have been marked as logically incomplete
                if (!op->HasFlag(TTxFlags::BlockingImmediateWrites)) {
                    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Adding BlockingImmediateWrites for op " << *op << " at " << Self->TabletID());
                    op->SetFlag(TTxFlags::BlockingImmediateWrites);
                }
            }
        }
        auto pr = ActivePlannedOps.emplace(op->GetStepOrder(), op);
        Y_ABORT_UNLESS(pr.second, "AddActiveOp must never add duplicate transactions");
        Y_ABORT_UNLESS(pr.first == std::prev(ActivePlannedOps.end()), "AddActiveOp must always add transactions in order");
        bool isComplete = op->HasFlag(TTxFlags::BlockingImmediateOps);
        if (ActivePlannedOpsLogicallyCompleteEnd == ActivePlannedOps.end() && !isComplete) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Operation " << *op << " is the new logically complete end at " << Self->TabletID());
            ActivePlannedOpsLogicallyCompleteEnd = pr.first;
        }
        bool isIncomplete = isComplete || op->HasFlag(TTxFlags::BlockingImmediateWrites);
        if (ActivePlannedOpsLogicallyIncompleteEnd == ActivePlannedOps.end() && !isIncomplete) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Operation " << *op << " is the new logically incomplete end at " << Self->TabletID());
            ActivePlannedOpsLogicallyIncompleteEnd = pr.first;
        }
    }

    if (op->IsSchemeTx()) {
        if (!SchemaTx)
            SchemaTx = Self->TransQueue.FindSchemaTx(op->GetTxId());
        Y_VERIFY_S(SchemaTx && SchemaTx->TxId == op->GetTxId(),
                   "Multiple schema operations at " << Self->TabletID() <<
                   ": got " << op->GetKind() << " operation " << *op
                   << " with " << SchemaTx->Type << " operation ["
                   << SchemaTx->PlanStep << ":" << SchemaTx->TxId
                   << "] already on shard");
    }

    auto inserted = ActiveOps.emplace(op->GetStepOrder(), op);
    Y_VERIFY_S(inserted.second,
               " cannot activate " << op->GetKind() << " operation " << *op << " at "
               << Self->TabletID() << " because it is already active");

    LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                "Activated operation " << *op << " at " << Self->TabletID());

    AddCandidateUnit(EExecutionUnitKind::PlanQueue);
}

void TPipeline::RemoveActiveOp(TOperation::TPtr op)
{
    UnblockNormalDependencies(op);
    UnblockSpecialDependencies(op);

    if (op->IsImmediate()) {
        RemoveImmediateOp(op);
    } else {
        auto it = ActivePlannedOps.find(op->GetStepOrder());
        Y_ABORT_UNLESS(it != ActivePlannedOps.end());
        auto next = std::next(it);
        if (ActivePlannedOpsLogicallyIncompleteEnd == it) {
            ActivePlannedOpsLogicallyIncompleteEnd = next;
        }
        if (ActivePlannedOpsLogicallyCompleteEnd == it) {
            ActivePlannedOpsLogicallyCompleteEnd = next;
        }
        ActivePlannedOps.erase(it);
    }
    ActiveOps.erase(op->GetStepOrder());
    RemoveExecuteBlocker(op);

    AddCandidateUnit(EExecutionUnitKind::PlanQueue);
}

void TPipeline::UnblockNormalDependencies(const TOperation::TPtr &op)
{
    // N.B. we only need to walk Dependents since operations only
    // wait for their Dependencies, never for their Depenents
    for (auto &dep : op->GetDependents()) {
        if (dep->GetDependencies().size() == 1) {
            Y_ABORT_UNLESS(op == *dep->GetDependencies().begin());
            if (dep->GetSpecialDependencies().empty()) {
                AddCandidateOp(dep);
            }
        }
    }

    // We no longer need all of these links
    op->ClearDependents();
    op->ClearDependencies();
    op->ClearPlannedConflicts();
    op->ClearImmediateConflicts();
    op->ClearRepeatableReadConflicts();
    DepTracker.RemoveOperation(op);
}

void TPipeline::UnblockSpecialDependencies(const TOperation::TPtr &op)
{
    for (auto &dep : op->GetSpecialDependents()) {
        if (dep->GetSpecialDependencies().size() == 1) {
            Y_ABORT_UNLESS(op == *dep->GetSpecialDependencies().begin());
            if (dep->GetDependencies().empty()) {
                AddCandidateOp(dep);
            }
        }
    }

    op->ClearSpecialDependents();
    op->ClearSpecialDependencies();
}

TOperation::TPtr TPipeline::FindOp(ui64 txId)
{
    if (Self->TransQueue.Has(txId))
        return Self->TransQueue.FindTxInFly(txId);

    if (ImmediateOps.contains(txId))
        return ImmediateOps.at(txId);

    return nullptr;
}

TOperation::TPtr TPipeline::GetActiveOp(ui64 txId)
{
    TOperation::TPtr op = FindOp(txId);
    if (op && (op->IsCompleted() || op->IsExecuting() || op->IsWaitingDependencies()))
        return op;
    return nullptr;
}

TOperation::TPtr TPipeline::GetVolatileOp(ui64 txId)
{
    TOperation::TPtr op = FindOp(txId);
    if (op && op->HasVolatilePrepareFlag()) {
        return op;
    }
    return nullptr;
}

bool TPipeline::LoadTxDetails(TTransactionContext &txc,
                              const TActorContext &ctx,
                              TActiveTransaction::TPtr tx)
{
    auto it = DataTxCache.find(tx->GetTxId());
    if (it != DataTxCache.end()) {
        auto baseTx = it->second;
        Y_ABORT_UNLESS(baseTx->GetType() == TValidatedTx::EType::DataTx, "Wrong tx type in cache");
        TValidatedDataTx::TPtr dataTx = std::static_pointer_cast<TValidatedDataTx>(baseTx);

        dataTx->SetStep(tx->GetStep());
        tx->FillTxData(dataTx);
        // Remove tx from cache.
        ForgetTx(tx->GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "LoadTxDetails at " << Self->TabletID() << " got data tx from cache "
                    << tx->GetStep() << ":" << tx->GetTxId());
    } else if (tx->HasVolatilePrepareFlag()) {
        // Since transaction is volatile it was never stored on disk, and it
        // shouldn't have any artifacts yet.
        tx->FillVolatileTxData(Self, txc, ctx);

        ui32 keysCount = 0;
        keysCount = tx->ExtractKeys();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "LoadTxDetails at " << Self->TabletID() << " loaded tx from memory "
                    << tx->GetStep() << ":" << tx->GetTxId() << " keys extracted: "
                    << keysCount);
    } else {
        NIceDb::TNiceDb db(txc.DB);
        TActorId target;
        TString txBody;
        TVector<TSysTables::TLocksTable::TLock> locks;
        ui64 artifactFlags = 0;
        bool ok = Self->TransQueue.LoadTxDetails(db, tx->GetTxId(), target,
                                                 txBody, locks, artifactFlags);
        if (!ok)
            return false;

        // Check we have enough memory to parse tx.
        ui64 requiredMem = txBody.size() * 10;
        if (MaybeRequestMoreTxMemory(requiredMem, txc))
            return false;

        tx->FillTxData(Self, txc, ctx, target, txBody,
                       std::move(locks), artifactFlags);

        ui32 keysCount = 0;
        //if (Config.LimitActiveTx > 1)
        keysCount = tx->ExtractKeys();

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "LoadTxDetails at " << Self->TabletID() << " loaded tx from db "
                    << tx->GetStep() << ":" << tx->GetTxId() << " keys extracted: "
                    << keysCount);
    }

    return true;
}

bool TPipeline::LoadWriteDetails(TTransactionContext& txc, const TActorContext& ctx, TWriteOperation::TPtr writeOp)
{
    auto it = DataTxCache.find(writeOp->GetTxId());
    if (it != DataTxCache.end()) {
        auto baseTx = it->second;
        Y_ABORT_UNLESS(baseTx->GetType() == TValidatedTx::EType::WriteTx, "Wrong writeOp type in cache");
        TValidatedWriteTx::TPtr writeTx = std::static_pointer_cast<TValidatedWriteTx>(baseTx);

        writeOp->FillTxData(writeTx);
        // Remove writeOp from cache.
        ForgetTx(writeOp->GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "LoadWriteDetails at " << Self->TabletID() << " got data writeOp from cache " << writeOp->GetStep() << ":" << writeOp->GetTxId());
    } else if (writeOp->HasVolatilePrepareFlag()) {
        // Since transaction is volatile it was never stored on disk, and it
        // shouldn't have any artifacts yet.
        writeOp->FillVolatileTxData(Self);

        ui32 keysCount = 0;
        keysCount = writeOp->ExtractKeys(txc.DB.GetScheme());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "LoadWriteDetails at " << Self->TabletID() << " loaded writeOp from memory " << writeOp->GetStep() << ":" << writeOp->GetTxId() << " keys extracted: " << keysCount);
    } else {
        NIceDb::TNiceDb db(txc.DB);
        TActorId target;
        TString txBody;
        TVector<TSysTables::TLocksTable::TLock> locks;
        ui64 artifactFlags = 0;
        bool ok = Self->TransQueue.LoadTxDetails(db, writeOp->GetTxId(), target, txBody, locks, artifactFlags);
        if (!ok)
            return false;

        // Check we have enough memory to parse writeOp.
        ui64 requiredMem = txBody.size() * 10;
        if (MaybeRequestMoreTxMemory(requiredMem, txc))
            return false;

        writeOp->FillTxData(Self, target, txBody, std::move(locks), artifactFlags);

        ui32 keysCount = 0;
        //if (Config.LimitActiveTx > 1)
        keysCount = writeOp->ExtractKeys(txc.DB.GetScheme());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "LoadWriteDetails at " << Self->TabletID() << " loaded writeOp from db " << writeOp->GetStep() << ":" << writeOp->GetTxId() << " keys extracted: " << keysCount);
    }

    return true;
}

void TPipeline::DeactivateOp(TOperation::TPtr op,
                             TTransactionContext& txc,
                             const TActorContext &ctx)
{
    op->SetCompletedFlag();
    op->ResetExecutingFlag();
    op->SetCompletedAt(TAppData::TimeProvider->Now());
    UnblockNormalDependencies(op);
    UnblockSpecialDependencies(op);

    AddCandidateUnit(EExecutionUnitKind::PlanQueue);

    CompleteTx(op, txc, ctx);

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    if (tx) {
        TValidatedDataTx::TPtr dataTx = tx->GetDataTx();
        // TODO: move flag computation into operation build phase?
        if (Config.DirtyOnline() && dataTx) {
            bool preventDirty = dataTx->LockTxId() && !dataTx->ReadOnly();
            if (!preventDirty)
                op->SetForceDirtyFlag();
        }
    }

    op->Deactivate();
}

bool TPipeline::SaveInReadSet(const TEvTxProcessing::TEvReadSet &rs,
                              THolder<IEventHandle> &ack,
                              TTransactionContext &txc,
                              const TActorContext &ctx)
{
    ui64 step = rs.Record.GetStep();
    ui64 txId = rs.Record.GetTxId();

    if (Self->GetVolatileTxManager().FindByTxId(txId)) {
        // This readset is for a known volatile transaction, we need to
        // hand it off to volatile tx manager.
        return Self->GetVolatileTxManager().ProcessReadSet(rs, std::move(ack), txc);
    }

    if (step <= OutdatedReadSetStep()) {
        LOG_NOTICE(ctx, NKikimrServices::TX_DATASHARD,
                   "Outdated readset for %" PRIu64 ":%" PRIu64 " at %" PRIu64,
                   step, txId, Self->TabletID());
        return true;
    }

    // If there is no required tx then we cannot assume it is finished.
    // There are cases when tablet is not leader any more but is not dead
    // yet and can receive RS for tx executed in another generation of this
    // tablet. In this case this tablet should be dead soon and source will
    // connect to the correct tablet and resend RS.
    // But it's also possible tx was executed on this tablet but its step
    // is not finished yet (e.g. due to out-of-order). In this case we should
    // store ack and send it after its step become outdated.
    if (!Self->TransQueue.Has(txId)) {
        LOG_NOTICE(ctx, NKikimrServices::TX_DATASHARD,
                   "Unexpected readset in state %" PRIu32 " for %" PRIu64 ":%" PRIu64 " at %" PRIu64,
                   Self->State, step, txId, Self->TabletID());
        if (ack) {
            DelayedAcks[TStepOrder(step, txId)].push_back(std::move(ack));
        }
        return false;
    }

    TOperation::TPtr op = GetActiveOp(txId);
    bool active = true;
    if (!op) {
        op = GetVolatileOp(txId);
        active = false;
    }
    if (op) {
        if (!op->GetStep() && !op->GetPredictedStep() && !active) {
            op->SetPredictedStep(step);
            AddPredictedPlan(step, txId, ctx);
        }
        // If input read sets are not loaded yet then
        // it will be added at load.
        if (op->HasLoadedInRSFlag()) {
            op->AddInReadSet(rs.Record);
        } else {
            op->AddDelayedInReadSet(rs.Record);
        }
        if (ack) {
            op->AddDelayedAck(THolder(ack.Release()));
        }

        if (active) {
            AddCandidateOp(op);
            Self->PlanQueue.Progress(ctx);
        }

        return false;
    }

    SaveInReadSet(rs, txc);

    return true;
}

void TPipeline::SaveInReadSet(const TEvTxProcessing::TEvReadSet &rs,
                              TTransactionContext &txc)
{
    using Schema = TDataShard::Schema;

    auto& pb = rs.Record;
    ui64 txId = pb.GetTxId();
    ui64 origin = pb.GetTabletProducer();
    ui64 from = pb.GetTabletSource();
    ui64 to = pb.GetTabletDest();

    TString coverageList;
    if (pb.HasBalanceTrackList()) {
        coverageList = pb.GetBalanceTrackList().SerializeAsString();
    }

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                "Save read set at " << Self->TabletID() << " for " << txId
                << " origin=" << origin << " from=" << from << " to=" << to);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::InReadSets>().Key(txId, origin, from, to).Update(
        NIceDb::TUpdate<Schema::InReadSets::Body>(pb.GetReadSet()),
        NIceDb::TUpdate<Schema::InReadSets::BalanceTrackList>(coverageList));
}

bool TPipeline::LoadInReadSets(TOperation::TPtr op,
                               TTransactionContext &txc,
                               const TActorContext &ctx)
{
    using Schema = TDataShard::Schema;

    // No incoming read sets are expected
    if (op->InReadSets().empty()) {
        op->SetLoadedInRSFlag();
        return true;
    }

    // Note that InReadSets holds an empty body for every
    // origin from which an incoming read set is expected
    op->InitRemainReadSets();

    // Create coverage builders to handle split/merge
    // of read set origins (datashards)
    for (const auto &kv : op->InReadSets()) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Prepare for loading readset for " << *op << " at " << Self->TabletID()
                    << " source=" << kv.first.first <<  " target=" << kv.first.second);
        op->CoverageBuilders()[kv.first].reset(new TBalanceCoverageBuilder());
    }

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Expected " << op->GetRemainReadSets() << " readsets for " << *op
                << " at " << Self->TabletID());

    NIceDb::TNiceDb db(txc.DB);

    THashSet<TReadSetKey> loadedReadSets;

    // Iterate over (txId, ...) keys
    // TODO[serxa]: this should be Range(txId) but it is not working right now
    auto rowset = db.Table<Schema::InReadSets>().GreaterOrEqual(op->GetTxId(), 0, 0, 0).Select();
    if (!rowset.IsReady())
        return false;
    while (!rowset.EndOfSet()) {
        // Check for the end of range
        if (rowset.GetValue<Schema::InReadSets::TxId>() != op->GetTxId())
            break;

        // Get row data
        ui64 origin = rowset.GetValue<Schema::InReadSets::Origin>();
        ui64 from = rowset.GetValue<Schema::InReadSets::From>();
        ui64 to = rowset.GetValue<Schema::InReadSets::To>();
        TString body = rowset.GetValue<Schema::InReadSets::Body>();
        TString track = rowset.GetValue<Schema::InReadSets::BalanceTrackList>();

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Read readset for " << *op << " at " << Self->TabletID()
                    << " from DB origin=" << origin << " from=" << from
                    << " to=" << to);

        // Parse track
        NKikimrTx::TBalanceTrackList balanceTrackList;
        bool success = balanceTrackList.ParseFromArray(track.data(), track.size());
        Y_ABORT_UNLESS(success);

        TReadSetKey rsKey(op->GetTxId(), origin, from, to);
        loadedReadSets.insert(rsKey);
        op->AddInReadSet(rsKey, balanceTrackList, body);

        if (!rowset.Next())
            return false;
    }

    // Add read sets not stored to DB.
    for (auto &rs : op->DelayedInReadSets()) {
        if (!loadedReadSets.contains(TReadSetKey(rs))) {
            op->AddInReadSet(std::move(rs));
        }
    }
    op->DelayedInReadSets().clear();

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Remain " << op->GetRemainReadSets() << " read sets for " << *op
                << " at " << Self->TabletID());

    op->SetLoadedInRSFlag();

    return true;
}

void TPipeline::RemoveInReadSets(TOperation::TPtr op,
                                 NIceDb::TNiceDb &db)
{
    using Schema = TDataShard::Schema;

    for (auto& rs : op->InReadSets()) {
        ui64 from = rs.first.first;
        ui64 to = rs.first.second;
        for (auto& rsdata: rs.second) {
            ui64 origin = rsdata.Origin;
            db.Table<Schema::InReadSets>().Key(op->GetTxId(), origin, from, to).Delete();
        }
    }
}

void TPipeline::RemoveTx(TStepOrder stepTxId) {
    // Optimization: faster Alter. Do not wait if no TxInFly.
    // Can't persist KeepSchemaStep here. TxInFly must be restored on init.
    if (Self->TransQueue.TxInFly() == 0) {
        KeepSchemaStep = LastPlannedTx.Step;
    }

    if (SchemaTx && SchemaTx->TxId == stepTxId.TxId) {
        SchemaTx->Done = true;
        SchemaTx = nullptr;
    }

    ForgetTx(stepTxId.TxId);
}

const TSchemaOperation* TPipeline::FindSchemaTx(ui64 txId) const {
    return Self->TransQueue.FindSchemaTx(txId);
}

void TPipeline::CompleteSchemaTx(NIceDb::TNiceDb& db, ui64 txId) {
    Y_ABORT_UNLESS(txId);
    TSchemaOperation * op = Self->TransQueue.FindSchemaTx(txId);
    if (!op)
        return;

    Y_ABORT_UNLESS(txId == op->TxId);
    Y_ABORT_UNLESS(op->Done);

    Self->TransQueue.RemoveScanProgress(db, txId);
    Self->TransQueue.RemoveSchemaOperation(db, txId);
}

bool TPipeline::PlanTxs(ui64 step,
                        TVector<ui64> &txIds,
                        TTransactionContext &txc,
                        const TActorContext &ctx) {
    Sort(txIds.begin(), txIds.end());
    // check uniqueness
    for (ui32 i = 1; i < txIds.size(); ++i) {
        Y_ABORT_UNLESS(txIds[i - 1] < txIds[i]);
    }

    if (step <= LastPlannedTx.Step)
        return false;

    auto it = PredictedPlan.begin();
    while (it != PredictedPlan.end() && it->Step < step) {
        PlanTxImpl(it->Step, it->TxId, txc, ctx);
        PredictedPlan.erase(it++);
    }

    ui64 lastTxId = 0;
    for (ui64 txId : txIds) {
        while (it != PredictedPlan.end() && it->Step == step && it->TxId < txId) {
            PlanTxImpl(step, it->TxId, txc, ctx);
            PredictedPlan.erase(it++);
        }
        if (it != PredictedPlan.end() && it->Step == step && it->TxId == txId) {
            PredictedPlan.erase(it++);
        }
        PlanTxImpl(step, txId, txc, ctx);
        lastTxId = txId;
    }

    while (it != PredictedPlan.end() && it->Step == step) {
        PlanTxImpl(step, it->TxId, txc, ctx);
        lastTxId = it->TxId;
        PredictedPlan.erase(it++);
    }

    NIceDb::TNiceDb db(txc.DB);
    SaveLastPlannedTx(db, TStepOrder(step, lastTxId));

    AddCandidateUnit(EExecutionUnitKind::PlanQueue);
    MaybeActivateWaitingSchemeOps(ctx);
    ActivateWaitingTxOps(ctx);

    return true;
}

bool TPipeline::PlanPredictedTxs(ui64 step, TTransactionContext &txc, const TActorContext &ctx) {
    if (step <= LastPlannedTx.Step) {
        return false;
    }

    ui64 lastStep = 0;
    ui64 lastTxId = 0;
    size_t planned = 0;

    auto it = PredictedPlan.begin();
    while (it != PredictedPlan.end() && it->Step <= step) {
        PlanTxImpl(it->Step, it->TxId, txc, ctx);
        lastStep = it->Step;
        lastTxId = it->TxId;
        PredictedPlan.erase(it++);
        ++planned;
    }

    if (planned == 0) {
        return false;
    }

    NIceDb::TNiceDb db(txc.DB);
    SaveLastPlannedTx(db, TStepOrder(lastStep, lastTxId));

    AddCandidateUnit(EExecutionUnitKind::PlanQueue);
    MaybeActivateWaitingSchemeOps(ctx);
    ActivateWaitingTxOps(ctx);
    return true;
}

void TPipeline::PlanTxImpl(ui64 step, ui64 txId, TTransactionContext &txc, const TActorContext &ctx)
{
    if (SchemaTx && SchemaTx->TxId == txId && SchemaTx->MinStep > step) {
        TString explain = TStringBuilder() << "Scheme transaction has come too early"
                                            << ", only after particular step this shema tx is allowed"
                                            << ", txId: " << txId
                                            << ", expected min step: " << SchemaTx->MinStep
                                            << ", actual step: " << step;
        Y_VERIFY_DEBUG_S(SchemaTx->MinStep <= step, explain);
        LOG_ALERT_S(ctx, NKikimrServices::TX_DATASHARD, explain);
    }

    auto op = Self->TransQueue.FindTxInFly(txId);
    if (!op) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Ignoring PlanStep " << step << " for unknown txId "
                    << txId << " at tablet " <<  Self->TabletID());
        return;
    }

    if (op->GetStep() && op->GetStep() != step) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Ignoring PlanStep " << step << " for txId " << txId
                    << " which already has PlanStep " << op->GetStep()
                    << " at tablet " << Self->TabletID());
        return;
    }

    NIceDb::TNiceDb db(txc.DB);
    Self->TransQueue.PlanTx(op, step, db);

    // Execute WaitForPlan unit here to correctly compute operation
    // profile. Otherwise profile might count time spent in plan
    // queue as 'wait for plan' time.
    if (op->GetCurrentUnit() == EExecutionUnitKind::WaitForPlan) {
        auto status = RunExecutionUnit(op, txc, ctx);
        Y_ABORT_UNLESS(status == EExecutionStatus::Executed);
    }
}

void TPipeline::AddPredictedPlan(ui64 step, ui64 txId, const TActorContext &ctx)
{
    if (step <= LastPlannedTx.Step) {
        // Trying to add a predicted step to transaction that is in the past
        // We cannot plan for the past, so we need to clean it up as soon as possible
        Self->ExecuteCleanupVolatileTx(txId, ctx);
        return;
    }

    PredictedPlan.emplace(step, txId);
    Self->WaitPredictedPlanStep(step);
}

void TPipeline::MarkOpAsUsingSnapshot(TOperation::TPtr op)
{
    UnblockNormalDependencies(op);

    op->SetUsingSnapshotFlag();
}

void TPipeline::SaveLastPlannedTx(NIceDb::TNiceDb& db, TStepOrder stepTxId) {
    using Schema = TDataShard::Schema;

    LastPlannedTx = stepTxId;
    Self->PersistSys(db, Schema::Sys_LastPlannedStep, LastPlannedTx.Step);
    Self->PersistSys(db, Schema::Sys_LastPlannedTx, LastPlannedTx.TxId);
}

void TPipeline::CompleteTx(const TOperation::TPtr op, TTransactionContext& txc, const TActorContext &ctx) {
    NIceDb::TNiceDb db(txc.DB);
    using Schema = TDataShard::Schema;

    if (UtmostCompleteTx < op->GetStepOrder())
        UtmostCompleteTx = op->GetStepOrder();

    if (Self->IsMvccEnabled()) {
        MarkPlannedLogicallyCompleteUpTo(TRowVersion(op->GetStep(), op->GetTxId()), txc);
        Self->PromoteCompleteEdge(op.Get(), txc);
    }

    Y_ABORT_UNLESS(ActivePlannedOps);
    if (ActivePlannedOps.begin()->first == op->GetStepOrder()) {
        LastCompleteTx = op->GetStepOrder();
        if (LastCompleteTx < UtmostCompleteTx && ActivePlannedOps.size() == 1) {
            LastCompleteTx = UtmostCompleteTx;
        }
        Self->PersistSys(db, Schema::Sys_LastCompleteStep, LastCompleteTx.Step);
        Self->PersistSys(db, Schema::Sys_LastCompleteTx, LastCompleteTx.TxId);
    } else {
        Self->IncCounter(COUNTER_TX_REORDERED);
    }

    Self->TransQueue.RemoveTx(db, *op);
    RemoveInReadSets(op, db);

    if (Self->IsMvccEnabled()) {
        Self->PromoteFollowerReadEdge(txc);
    }

    while (!DelayedAcks.empty()
           && DelayedAcks.begin()->first.Step <= OutdatedReadSetStep())
    {
        auto &pr = *DelayedAcks.begin();

        LOG_NOTICE(ctx, NKikimrServices::TX_DATASHARD,
                   "Will send outdated delayed readset ack for %" PRIu64 ":%" PRIu64 " at %" PRIu64,
                   pr.first.Step, pr.first.TxId, Self->TabletID());

        for (auto& ack : pr.second) {
            op->AddDelayedAck(std::move(ack));
        }
        DelayedAcks.erase(DelayedAcks.begin());
    }

    // FIXME: probably not needed, (txinfly - planned) does not change on completion
    MaybeActivateWaitingSchemeOps(ctx);
    ActivateWaitingTxOps(ctx);
}

void TPipeline::PreserveSchema(NIceDb::TNiceDb& db, ui64 step) {
    using Schema = TDataShard::Schema;
    if (step > KeepSchemaStep && step != Max<ui64>()) {
        KeepSchemaStep = step;
        Self->PersistSys(db, Schema::Sys_AliveStep, step);
    }
}

bool TPipeline::AssignPlanInterval(TOperation::TPtr op)
{
    // TODO: it's possible to plan them until Alter minStep
    // TODO: it's possible to plan them if both old and new schemas allow tx
    // TODO: use propose blockers and DF_BLOCK_PROPOSE to block shard with scheme tx
    if (HasSchemaOperation() && !SchemaTx->IsReadOnly())
        return false;

    if (HasWaitingSchemeOps())
        return false;

    op->SetMinStep(AllowedDataStep());
    op->SetMaxStep(op->GetMinStep() + Self->DefaultTxStepDeadline());

    return true;
}

ui64 TPipeline::OutdatedReadSetStep() const
{
    // If plan is not empty then step previous to the
    // first one in plan is outdated.
    if (Self->TransQueue.TxPlanned())
        return Self->TransQueue.GetPlan().begin()->Step - 1;

    // If there is no tx in transaction queue then the last
    // completed tx has outdated step.
    return LastCompleteTx.Step;
}

ui64 TPipeline::OutdatedCleanupStep() const
{
    return LastPlannedTx.Step;
}

ui64 TPipeline::GetTxCompleteLag(EOperationKind kind, ui64 timecastStep) const
{
    auto &plan = Self->TransQueue.GetPlan(kind);
    if (plan.empty())
        return 0;

    ui64 currentStep = plan.begin()->Step;
    if (timecastStep > currentStep)
        return timecastStep - currentStep;

    return 0;
}

ui64 TPipeline::GetDataTxCompleteLag(ui64 timecastStep) const
{
    return GetTxCompleteLag(EOperationKind::DataTx, timecastStep);
}

ui64 TPipeline::GetScanTxCompleteLag(ui64 timecastStep) const
{
    return GetTxCompleteLag(EOperationKind::ReadTable, timecastStep);
}

void TPipeline::ProposeTx(TOperation::TPtr op, const TStringBuf &txBody, TTransactionContext &txc, const TActorContext &ctx)
{
    NIceDb::TNiceDb db(txc.DB);
    SetProposed(op->GetTxId(), op->GetTarget());
    if (!op->HasVolatilePrepareFlag()) {
        PreserveSchema(db, op->GetMaxStep());
    }
    Self->TransQueue.ProposeTx(db, op, op->GetTarget(), txBody);

    if (Self->IsStopping() && op->GetTarget()) {
        // Send notification if we prepared a tx while shard was stopping
        op->OnStopping(*Self, ctx);
    }
}

void TPipeline::ProposeComplete(const TOperation::TPtr &op, const TActorContext &ctx)
{
    // Operation is now confirmed to be stored in the database
    op->SetStoredFlag();
    // Restart notifications are currently sent in FinishPropose
    Y_UNUSED(ctx);
}

void TPipeline::PersistTxFlags(TOperation::TPtr op, TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    Self->TransQueue.UpdateTxFlags(db, op->GetTxId(), op->GetFlags());
}

void TPipeline::UpdateSchemeTxBody(ui64 txId, const TStringBuf &txBody, TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    Self->TransQueue.UpdateTxBody(db, txId, txBody);
}

void TPipeline::ProposeSchemeTx(const TSchemaOperation &op,
                                TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    Self->TransQueue.ProposeSchemaTx(db, op);
}

bool TPipeline::CancelPropose(NIceDb::TNiceDb& db, const TActorContext& ctx, ui64 txId,
        std::vector<std::unique_ptr<IEventHandle>>& replies)
{
    auto op = Self->TransQueue.FindTxInFly(txId);
    if (!op || op->GetStep()) {
        // Operation either doesn't exist, or already planned and cannot be cancelled
        return true;
    }

    if (!Self->TransQueue.CancelPropose(db, txId, replies)) {
        // Page fault, try again
        return false;
    }

    if (!op->IsExecutionPlanFinished()) {
        GetExecutionUnit(op->GetCurrentUnit()).RemoveOperation(op);
    }

    ForgetTx(txId);
    Self->CheckDelayedProposeQueue(ctx);
    MaybeActivateWaitingSchemeOps(ctx);
    return true;
}

ECleanupStatus TPipeline::CleanupOutdated(NIceDb::TNiceDb& db, const TActorContext& ctx, ui64 outdatedStep,
        std::vector<std::unique_ptr<IEventHandle>>& replies)
{
    const ui32 OUTDATED_BATCH_SIZE = 100;
    TVector<ui64> outdatedTxs;
    auto status = Self->TransQueue.CleanupOutdated(db, outdatedStep, OUTDATED_BATCH_SIZE, outdatedTxs);
    switch (status) {
        case ECleanupStatus::None:
        case ECleanupStatus::Restart:
            return status;
        case ECleanupStatus::Success:
            break;
    }

    if (outdatedTxs.size() < OUTDATED_BATCH_SIZE) {
        LastCleanupTime = outdatedStep;
    }

    for (ui64 txId : outdatedTxs) {
        auto op = Self->TransQueue.FindTxInFly(txId);
        if (op && !op->IsExecutionPlanFinished()) {
            op->OnCleanup(*Self, replies);
            GetExecutionUnit(op->GetCurrentUnit()).RemoveOperation(op);
        }
        Self->TransQueue.RemoveTxInFly(txId, &replies);

        ForgetTx(txId);
        LOG_INFO(ctx, NKikimrServices::TX_DATASHARD,
                "Outdated Tx %" PRIu64 " is cleaned at tablet %" PRIu64 " and outdatedStep# %" PRIu64,
                txId, Self->TabletID(), outdatedStep);
    }

    Self->CheckDelayedProposeQueue(ctx);

    return status;
}

bool TPipeline::CleanupVolatile(ui64 txId, const TActorContext& ctx,
        std::vector<std::unique_ptr<IEventHandle>>& replies)
{
    if (Self->TransQueue.CleanupVolatile(txId)) {
        auto op = Self->TransQueue.FindTxInFly(txId);
        if (op && !op->IsExecutionPlanFinished()) {
            op->OnCleanup(*Self, replies);
            GetExecutionUnit(op->GetCurrentUnit()).RemoveOperation(op);
        }
        Self->TransQueue.RemoveTxInFly(txId, &replies);

        ForgetTx(txId);

        Self->CheckDelayedProposeQueue(ctx);

        // Outdated op removal might enable scheme op proposal.
        MaybeActivateWaitingSchemeOps(ctx);

        // Outdated op removal might enable another op execution.
        // N.B.: actually this happens only for DROP which waits
        // for TxInFly having only the DROP.
        // N.B.: compatibility with older datashard versions.
        AddCandidateUnit(EExecutionUnitKind::PlanQueue);
        Self->PlanQueue.Progress(ctx);
        return true;
    }

    return false;
}

size_t TPipeline::CleanupWaitingVolatile(const TActorContext& ctx, std::vector<std::unique_ptr<IEventHandle>>& replies)
{
    std::vector<ui64> cleanupTxs;
    for (const auto& pr : Self->TransQueue.GetTxsInFly()) {
        if (pr.second->HasVolatilePrepareFlag() && !pr.second->GetStep()) {
            cleanupTxs.push_back(pr.first);
        }
    }

    size_t cleaned = 0;
    for (ui64 txId : cleanupTxs) {
        if (CleanupVolatile(txId, ctx, replies)) {
            ++cleaned;
        }
    }

    return cleaned;
}

ui64 TPipeline::PlannedTxInFly() const
{
    return Self->TransQueue.TxInFly();
}

const TSet<TStepOrder> &TPipeline::GetPlan() const
{
    return Self->TransQueue.GetPlan();
}

bool TPipeline::HasProposeDelayers() const
{
    return Self->TransQueue.HasProposeDelayers();
}

bool TPipeline::RemoveProposeDelayer(ui64 txId)
{
    return Self->TransQueue.RemoveProposeDelayer(txId);
}

void TPipeline::ProcessDisconnected(ui32 nodeId)
{
    for (auto &pr : ActiveOps) {
        if (pr.second->HasProcessDisconnectsFlag()) {
            auto *ev = new TDataShard::TEvPrivate::TEvNodeDisconnected(nodeId);
            pr.second->AddInputEvent(new IEventHandle(Self->SelfId(), Self->SelfId(), ev));
            AddCandidateOp(pr.second);
        }
    }
}

ui64 TPipeline::GetInactiveTxSize() const {
    ui64 res = 0;
    return res;
}

bool TPipeline::SaveForPropose(TValidatedTx::TPtr tx) {
    Y_ABORT_UNLESS(tx && tx->GetTxId());

    if (DataTxCache.size() >= Config.LimitDataTxCache)
        return false;

    ui64 quota = tx->GetMemoryConsumption();
    if (Self->TryCaptureTxCache(quota)) {
        tx->SetTxCacheUsage(quota);
        DataTxCache[tx->GetTxId()] = tx;
        return true;
    }

    return false;
}

void TPipeline::SetProposed(ui64 txId, const TActorId& actorId) {
    auto it = DataTxCache.find(txId);
    if (it != DataTxCache.end()) {
        it->second->SetSource(actorId);
    }
}


void TPipeline::ForgetUnproposedTx(ui64 txId) {
    auto it = DataTxCache.find(txId);
    if (it != DataTxCache.end() && !it->second->IsProposed()) {
        Self->ReleaseCache(*it->second);
        DataTxCache.erase(it);
    }
}

void TPipeline::ForgetTx(ui64 txId) {
    auto it = DataTxCache.find(txId);
    if (it != DataTxCache.end()) {
        Self->ReleaseCache(*it->second);
        DataTxCache.erase(it);
    }
}

TOperation::TPtr TPipeline::BuildOperation(TEvDataShard::TEvProposeTransaction::TPtr &ev,
                                           TInstant receivedAt, ui64 tieBreakerIndex,
                                           NTabletFlatExecutor::TTransactionContext &txc,
                                           const TActorContext &ctx, NWilson::TSpan &&operationSpan)
{
    auto &rec = ev->Get()->Record;
    Y_ABORT_UNLESS(!(rec.GetFlags() & TTxFlags::PrivateFlagsMask));
    TBasicOpInfo info(rec.GetTxId(),
                      static_cast<EOperationKind>(rec.GetTxKind()),
                      rec.GetFlags(), 0,
                      receivedAt,
                      tieBreakerIndex);
    if (rec.HasMvccSnapshot()) {
        info.SetMvccSnapshot(TRowVersion(rec.GetMvccSnapshot().GetStep(), rec.GetMvccSnapshot().GetTxId()),
            rec.GetMvccSnapshot().GetRepeatableRead());
    }
    TActiveTransaction::TPtr tx = MakeIntrusive<TActiveTransaction>(info);
    tx->SetTarget(ev->Sender);
    tx->SetTxBody(rec.GetTxBody());
    tx->SetCookie(ev->Cookie);
    tx->Orbit = std::move(ev->Get()->Orbit);
    tx->OperationSpan = std::move(operationSpan);

    auto malformed = [&](const TStringBuf txType, const TString& txBody) {
        const TString error = TStringBuilder() << "Malformed " << txType << " tx"
            << " at tablet " << Self->TabletID()
            << " txId " << tx->GetTxId()
            << " ssId " << tx->GetSchemeShardId()
            << " parsed tx body " << txBody;

        tx->SetAbortedFlag();
        tx->Result().Reset(new TEvDataShard::TEvProposeTransactionResult(
            rec.GetTxKind(), Self->TabletID(), tx->GetTxId(), NKikimrTxDataShard::TEvProposeTransactionResult::ERROR));
        tx->Result()->SetProcessError(NKikimrTxDataShard::TError::BAD_ARGUMENT, error);

        LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, error);
    };

    auto badRequest = [&](const TString& error) {
        tx->SetAbortedFlag();
        tx->Result().Reset(new TEvDataShard::TEvProposeTransactionResult(
            rec.GetTxKind(), Self->TabletID(), tx->GetTxId(), NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST));
        tx->Result()->SetProcessError(NKikimrTxDataShard::TError::BAD_ARGUMENT, error);

        LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, error);
    };

    if (tx->IsSchemeTx()) {
        Y_ABORT_UNLESS(!tx->HasVolatilePrepareFlag(), "Volatile scheme transactions not supported");

        Y_ABORT_UNLESS(rec.HasSchemeShardId());
        Y_ABORT_UNLESS(rec.HasProcessingParams());

        tx->SetSchemeShardId(rec.GetSchemeShardId());
        tx->SetSubDomainPathId(rec.GetSubDomainPathId());
        tx->SetProcessingParams(rec.GetProcessingParams());

        if (!tx->BuildSchemeTx()) {
            malformed(TStringBuf("scheme"), tx->GetSchemeTx().ShortDebugString());
            return tx;
        }

        auto &schemeTx = tx->GetSchemeTx();
        if (schemeTx.GetReadOnly()) {
            tx->SetReadOnlyFlag();
            tx->SetGlobalReaderFlag();
        } else {
            tx->SetProposeBlockerFlag();
            tx->SetGlobalReaderFlag();
            tx->SetGlobalWriterFlag();
        }
    } else if (tx->IsSnapshotTx()) {
        if (!tx->BuildSnapshotTx()) {
            malformed(TStringBuf("snapshot"), tx->GetSnapshotTx().ShortDebugString());
            return tx;
        }

        if (tx->HasVolatilePrepareFlag()) {
            badRequest("Unsupported volatile snapshot tx");
            return tx;
        }

        if (tx->GetSnapshotTx().HasCreateVolatileSnapshot()) {
            tx->SetReadOnlyFlag();
            tx->SetGlobalReaderFlag();
        } else if (tx->GetSnapshotTx().HasDropVolatileSnapshot()) {
            // FIXME: snapshot users should be immediate, no need to synchronize
            // Need to think which flags actually make sense
            tx->SetReadOnlyFlag();
            tx->SetGlobalReaderFlag();
        }
    } else if (tx->IsDistributedEraseTx()) {
        if (!tx->BuildDistributedEraseTx()) {
            malformed(TStringBuf("distributed erase"), tx->GetDistributedEraseTx()->GetBody().ShortDebugString());
            return tx;
        }

        if (tx->HasVolatilePrepareFlag()) {
            badRequest("Unsupported volatile distributed erase");
            return tx;
        }

        tx->SetGlobalWriterFlag();
        if (tx->GetDistributedEraseTx()->HasDependents()) {
            tx->SetGlobalReaderFlag();
        }
    } else if (tx->IsCommitWritesTx()) {
        if (!tx->BuildCommitWritesTx()) {
            malformed(TStringBuf("commit writes"), tx->GetCommitWritesTx()->GetBody().ShortDebugString());
            return tx;
        }

        if (tx->HasVolatilePrepareFlag()) {
            badRequest("Unsupported volatile commit writes");
            return tx;
        }

        tx->SetGlobalWriterFlag();
    } else {
        Y_ABORT_UNLESS(tx->IsReadTable() || tx->IsDataTx());
        auto dataTx = tx->BuildDataTx(Self, txc, ctx);
        if (dataTx->Ready() && (dataTx->ProgramSize() || dataTx->IsKqpDataTx()))
            dataTx->ExtractKeys(true);

        if (!dataTx->Ready() && !dataTx->RequirePrepare()) {
            tx->SetAbortedFlag();
            tx->Result().Reset(new TEvDataShard::TEvProposeTransactionResult(rec.GetTxKind(),
                                                                         Self->TabletID(),
                                                                         tx->GetTxId(),
                                                                         NKikimrTxDataShard::TEvProposeTransactionResult::ERROR));
            tx->Result()->SetProcessError(dataTx->Code(), dataTx->GetErrors());

            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                        "Shard " << Self->TabletID() << " cannot parse tx "
                        << tx->GetTxId() << ": " << dataTx->GetErrors());

            return tx;
        }

        if (dataTx->Immediate())
            tx->SetImmediateFlag();
        if (dataTx->ReadOnly())
            tx->SetReadOnlyFlag();
        if (dataTx->NeedDiagnostics())
            tx->SetNeedDiagnosticsFlag();
        if (dataTx->IsKqpDataTx())
            tx->SetKqpDataTransactionFlag();
        if (dataTx->IsKqpScanTx()) {
            tx->SetKqpScanTransactionFlag();
            // TODO: support for extracting keys in kqp scan transaction
            tx->SetGlobalReaderFlag();
        }

        // Additional checks for volatile transactions
        if (tx->HasVolatilePrepareFlag()) {
            if (tx->HasImmediateFlag()) {
                badRequest(TStringBuilder()
                    << "Volatile distributed tx " << tx->GetTxId()
                    << " at tablet " << Self->TabletID()
                    << " cannot be immediate");
                return tx;
            }

            if (!dataTx->IsKqpDataTx()) {
                badRequest(TStringBuilder()
                    << "Volatile distributed tx " << tx->GetTxId()
                    << " at tablet " << Self->TabletID()
                    << " must be a kqp data tx");
                return tx;
            }

            if (dataTx->GetKqpComputeCtx().HasPersistentChannels()) {
                badRequest(TStringBuilder()
                    << "Volatile distributed tx " << tx->GetTxId()
                    << " at tablet " << Self->TabletID()
                    << " cannot have persistent channels");
                return tx;
            }

            Y_ABORT_UNLESS(!tx->IsImmediate(), "Sanity check failed: volatile tx cannot be immediate");
        }

        // Make config checks for immediate tx.
        if (tx->IsImmediate()) {
            if (Config.NoImmediate() || (Config.ForceOnlineRW() && !dataTx->ReadOnly())) {
                LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                           "Shard " << Self->TabletID() << " force immediate tx "
                           << tx->GetTxId() << " to online according to config");
                tx->SetForceOnlineFlag();
            } else if (tx->IsReadTable()) {
                // Feature flag tells us txproxy supports immediate mode for ReadTable
            } else if (dataTx->RequirePrepare()) {
                LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                           "Shard " << Self->TabletID() << " force immediate tx "
                           << tx->GetTxId() << " to online because of SNAPSHOT_NOT_READY_YET status");
                tx->SetForceOnlineFlag();
            } else {
                if (Config.DirtyImmediate())
                    tx->SetForceDirtyFlag();
            }
        }

        if (!tx->IsMvccSnapshotRead()) {
            // No op
        } else if (tx->IsReadTable() && dataTx->GetReadTableTransaction().HasSnapshotStep() && dataTx->GetReadTableTransaction().HasSnapshotTxId()) {
            badRequest("Ambiguous snapshot info. Cannot use both MVCC and read table snapshots in one transaction");
            return tx;
        } else if (tx->IsKqpScanTransaction() && dataTx->HasKqpSnapshot()) {
            badRequest("Ambiguous snapshot info. Cannot use both MVCC and kqp scan snapshots in one transaction");
            return tx;
        }

        auto allowSnapshot = [&]() -> bool {
            // must be immediate
            if (!tx->IsImmediate()) {
                return false;
            }
            // always ok for readonly
            if (tx->IsReadOnly()) {
                return true;
            }
            // ok for locked writes
            if (dataTx->LockTxId()) {
                return true;
            }
            return false;
        };

        if(tx->IsMvccSnapshotRead() && !allowSnapshot()) {
            badRequest("Snapshot read must be an immediate read-only or locked-write transaction");
            return tx;
        }

        if (!tx->IsImmediate() || !Self->IsMvccEnabled()) {
            // No op
        } else if (tx->IsKqpScanTransaction() && dataTx->HasKqpSnapshot()) {
            // to be consistent while dependencies calculation
            auto snapshot = dataTx->GetKqpSnapshot();
            tx->SetMvccSnapshot(TRowVersion(snapshot.GetStep(), snapshot.GetTxId()));
        }
    }

    return tx;
}

TOperation::TPtr TPipeline::BuildOperation(NEvents::TDataEvents::TEvWrite::TPtr&& ev,
                                           TInstant receivedAt, ui64 tieBreakerIndex,
                                           NTabletFlatExecutor::TTransactionContext& txc,
                                           NWilson::TSpan &&operationSpan)
{
    const auto& rec = ev->Get()->Record;
    TBasicOpInfo info(rec.GetTxId(), EOperationKind::WriteTx, NEvWrite::TConvertor::GetProposeFlags(rec.GetTxMode()), 0, receivedAt, tieBreakerIndex);
    if (rec.HasMvccSnapshot()) {
        info.SetMvccSnapshot(TRowVersion(rec.GetMvccSnapshot().GetStep(), rec.GetMvccSnapshot().GetTxId()),
            rec.GetMvccSnapshot().GetRepeatableRead());
    }
    auto writeOp = MakeIntrusive<TWriteOperation>(info, std::move(ev), Self);
    writeOp->OperationSpan = std::move(operationSpan);
    auto writeTx = writeOp->GetWriteTx();
    Y_ABORT_UNLESS(writeTx);

    auto badRequest = [&](NKikimrDataEvents::TEvWriteResult::EStatus status, const TString& error) {
        writeOp->SetError(status, TStringBuilder() << error << " at tablet# " << Self->TabletID());
        LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, error);
    };

    if (rec.HasMvccSnapshot() && !rec.GetLockTxId()) {
        badRequest(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST,
            "MvccSnapshot without LockTxId is not implemented");
        return writeOp;
    }

    if (!writeTx->Ready()) {
        badRequest(NEvWrite::TConvertor::ConvertErrCode(writeOp->GetWriteTx()->GetErrCode()), TStringBuilder() << "Cannot parse tx " << writeOp->GetTxId() << ". " << writeOp->GetWriteTx()->GetErrCode() << ": " << writeOp->GetWriteTx()->GetErrStr());
        return writeOp;
    }

    writeTx->ExtractKeys(txc.DB.GetScheme(), true);

    if (!writeTx->Ready()) {
        badRequest(NEvWrite::TConvertor::ConvertErrCode(writeOp->GetWriteTx()->GetErrCode()), TStringBuilder() << "Cannot parse tx keys " << writeOp->GetTxId() << ". " << writeOp->GetWriteTx()->GetErrCode() << ": " << writeOp->GetWriteTx()->GetErrStr());
        return writeOp;
    }

    switch (rec.txmode()) {
        case NKikimrDataEvents::TEvWrite::MODE_PREPARE:
            break;
        case NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE:
            writeOp->SetVolatilePrepareFlag();
            break;
        case NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE:
            writeOp->SetImmediateFlag();
            break;
        default:
            badRequest(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder() << "Unknown txmode: " << rec.txmode());
            return writeOp;
    }

    // Make config checks for immediate op.
    if (writeOp->IsImmediate()) {
        if (Config.NoImmediate() || (Config.ForceOnlineRW())) {
            LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD, "Force immediate writeOp " << writeOp->GetTxId() << " to online according to config, at tablet #" << Self->TabletID());
            writeOp->SetForceOnlineFlag();
        } else {
            if (Config.DirtyImmediate())
                writeOp->SetForceDirtyFlag();
        }
    }

    return writeOp;
}

void TPipeline::BuildDataTx(TActiveTransaction *tx, TTransactionContext &txc, const TActorContext &ctx)
{
    auto dataTx = tx->BuildDataTx(Self, txc, ctx);
    Y_ABORT_UNLESS(dataTx->Ready());
    // TODO: we should have no requirement to have keys
    // for restarted immediate tx.
    if (dataTx->ProgramSize() || dataTx->IsKqpDataTx())
        dataTx->ExtractKeys(false);
}

void TPipeline::RegisterDistributedWrites(const TOperation::TPtr& op, NTable::TDatabase& db)
{
    if (op->HasFlag(TTxFlags::DistributedWritesRegistered)) {
        return;
    }

    // Try to cache write keys if possible
    if (!op->IsImmediate() && op->HasKeysInfo()) {
        auto& keysInfo = op->GetKeysInfo();
        if (keysInfo.WritesCount > 0) {
            TConflictsCache::TPendingWrites writes;
            for (const auto& vk : keysInfo.Keys) {
                const auto& k = *vk.Key;
                if (vk.IsWrite && k.Range.Point && Self->IsUserTable(k.TableId)) {
                    writes.emplace_back(Self->GetLocalTableId(k.TableId), k.Range.GetOwnedFrom());
                }
            }
            if (!writes.empty()) {
                Self->GetConflictsCache().RegisterDistributedWrites(op->GetTxId(), std::move(writes), db);
            }
            op->SetFlag(TTxFlags::DistributedWritesRegistered);
        }
    }
}

EExecutionStatus TPipeline::RunExecutionUnit(TOperation::TPtr op, TTransactionContext &txc, const TActorContext &ctx)
{
    Y_ABORT_UNLESS(!op->IsExecutionPlanFinished());
    auto &unit = GetExecutionUnit(op->GetCurrentUnit());

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Trying to execute " << *op << " at " << Self->TabletID()
                << " on unit " << unit.GetKind());

    if (!unit.IsReadyToExecute(op)) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Operation " << *op << " at " << Self->TabletID()
                    << " is not ready to execute on unit " << unit.GetKind());
        return EExecutionStatus::Continue;
    }

    NCpuTime::TCpuTimer timer;
    auto status = unit.Execute(op, txc, ctx);
    op->AddExecutionTime(timer.GetTime());

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Execution status for " << *op << " at " << Self->TabletID()
                << " is " << status);

    if (status == EExecutionStatus::Executed
        || status == EExecutionStatus::ExecutedNoMoreRestarts
        || status == EExecutionStatus::DelayComplete
        || status == EExecutionStatus::DelayCompleteNoMoreRestarts)
        MoveToNextUnit(op);

    return status;
}

EExecutionStatus TPipeline::RunExecutionPlan(TOperation::TPtr op,
                                             TVector<EExecutionUnitKind> &completeList,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    bool canRestart = true;
    while (!op->IsExecutionPlanFinished()) {
        auto &unit = GetExecutionUnit(op->GetCurrentUnit());

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Trying to execute " << *op << " at " << Self->TabletID()
                    << " on unit " << unit.GetKind());

        if (!unit.IsReadyToExecute(op)) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Operation " << *op << " at " << Self->TabletID()
                        << " is not ready to execute on unit " << unit.GetKind());

            return EExecutionStatus::Continue;
        }

        const bool mightRestart = unit.GetExecutionMightRestart();

        if (mightRestart && !canRestart) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Operation " << *op << " at " << Self->TabletID()
                        << " cannot execute on unit " << unit.GetKind()
                        << " because no more restarts are allowed");

            return EExecutionStatus::Reschedule;
        }

        NWilson::TSpan unitSpan(TWilsonTablet::TabletDetailed, txc.TransactionExecutionSpan.GetTraceId(), "Datashard.Unit");
        
        NCpuTime::TCpuTimer timer;
        auto status = unit.Execute(op, txc, ctx);
        op->AddExecutionTime(timer.GetTime());
        
        if (unitSpan) {
            unitSpan.Attribute("Type", TypeName(unit))
                    .Attribute("Status", static_cast<int>(status))
                    .EndOk();
        }

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Execution status for " << *op << " at " << Self->TabletID()
                    << " is " << status);

        if (status == EExecutionStatus::Executed) {
            MoveToNextUnit(op);
        } else if (status == EExecutionStatus::ExecutedNoMoreRestarts) {
            canRestart = false;
            MoveToNextUnit(op);
        } else if (status == EExecutionStatus::WaitComplete) {
            completeList.push_back(unit.GetKind());
            return status;
        } else if (status == EExecutionStatus::DelayComplete) {
            completeList.push_back(unit.GetKind());
            MoveToNextUnit(op);
        } else if (status == EExecutionStatus::DelayCompleteNoMoreRestarts) {
            canRestart = false;
            completeList.push_back(unit.GetKind());
            MoveToNextUnit(op);
        } else {
            Y_VERIFY_S(status == EExecutionStatus::Restart
                       || status == EExecutionStatus::Continue
                       || status == EExecutionStatus::Reschedule,
                       "Unexpected execution status " << status);

            if (status == EExecutionStatus::Restart) {
                Y_VERIFY_DEBUG_S(mightRestart,
                        "Unexpected execution status " << status
                        << " from unit " << unit.GetKind()
                        << " not marked as restartable");
                Y_VERIFY_S(canRestart,
                        "Unexpected execution status " << status
                        << " from unit " << unit.GetKind()
                        << " when restarts are not allowed");
            }

            return status;
        }
    }

    return EExecutionStatus::Executed;
}

void TPipeline::MoveToNextUnit(TOperation::TPtr op)
{
    Y_ABORT_UNLESS(!op->IsExecutionPlanFinished());
    GetExecutionUnit(op->GetCurrentUnit()).RemoveOperation(op);

    LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                "Advance execution plan for " << *op << " at " << Self->TabletID()
                << " executing on unit " << op->GetCurrentUnit());

    op->AdvanceExecutionPlan();
    if (!op->IsExecutionPlanFinished()) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Add " << *op << " at " << Self->TabletID() << " to execution unit "
                    << op->GetCurrentUnit());

        GetExecutionUnit(op->GetCurrentUnit()).AddOperation(op);
    } else {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Execution plan for " << *op << " at " << Self->TabletID()
                    << " has finished");
    }
}

void TPipeline::RunCompleteList(TOperation::TPtr op,
                                TVector<EExecutionUnitKind> &completeList,
                                const TActorContext &ctx)
{
    for (auto kind : completeList) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "Complete execution for " << *op << " at " << Self->TabletID()
                    << " on unit " << kind);

        TInstant start = AppData()->TimeProvider->Now();
        GetExecutionUnit(kind).Complete(op, ctx);
        op->SetCompleteTime(kind, AppData()->TimeProvider->Now() - start);

        if (!op->IsExecutionPlanFinished() && op->GetCurrentUnit() == kind) {
            Y_ABORT_UNLESS(completeList.back() == kind);
            MoveToNextUnit(op);
        }
    }
}

void TPipeline::HoldExecutionProfile(TOperation::TPtr op)
{
    auto &units = op->GetExecutionProfile().UnitProfiles;
    TStoredExecutionProfile profile;
    profile.OpInfo = *op;
    profile.UnitProfiles.reserve(units.size());
    for (auto unit : op->GetExecutionPlan()) {
        if (units.contains(unit)) {
            profile.UnitProfiles.emplace_back(unit, units.at(unit));
        }
    }

    SlowOpProfiles.emplace_back(profile);

    while (SlowOpProfiles.size() > Self->GetDataTxProfileBufferSize()) {
        SlowOpProfiles.pop_front();
    }
}

void TPipeline::FillStoredExecutionProfiles(NKikimrTxDataShard::TEvGetSlowOpProfilesResponse &rec) const
{
    for (auto &profile : SlowOpProfiles) {
        auto &entry = *rec.AddProfiles();
        profile.OpInfo.Serialize(*entry.MutableBasicInfo());

        for (auto &pr : profile.UnitProfiles) {
            auto &unit = *entry.MutableExecutionProfile()->AddUnitProfiles();
            unit.SetUnitKind(ToString(pr.first));
            unit.SetWaitTime(pr.second.WaitTime.GetValue());
            unit.SetExecuteTime(pr.second.ExecuteTime.GetValue());
            unit.SetCommitTime(pr.second.CommitTime.GetValue());
            unit.SetCompleteTime(pr.second.CompleteTime.GetValue());
            unit.SetExecuteCount(pr.second.ExecuteCount);
        }
    }
}

bool TPipeline::AddWaitingSchemeOp(const TOperation::TPtr& op) {
    auto itHash = WaitingSchemeOps.find(op);
    if (itHash != WaitingSchemeOps.end()) {
        return false;
    }

    auto itOrder = WaitingSchemeOpsOrder.insert(WaitingSchemeOpsOrder.end(), op);
    auto res = WaitingSchemeOps.emplace(op, itOrder);
    Y_ABORT_UNLESS(res.second, "Unexpected duplicate when inserting op into THashMap");
    return true;
}

bool TPipeline::RemoveWaitingSchemeOp(const TOperation::TPtr& op) {
    auto itHash = WaitingSchemeOps.find(op);
    if (itHash == WaitingSchemeOps.end()) {
        return false;
    }

    WaitingSchemeOpsOrder.erase(itHash->second);
    WaitingSchemeOps.erase(itHash);
    return true;
}

void TPipeline::ActivateWaitingSchemeOps(const TActorContext& ctx) const {
    TVector<TOperation::TPtr> activated(Reserve(WaitingSchemeOpsOrder.size()));
    for (const auto& op : WaitingSchemeOpsOrder) {
        if (op->IsInProgress()) {
            // Skip ops that are currently active
            continue;
        }
        op->IncrementInProgress();
        activated.push_back(op);
    }
    for (auto& op : activated) {
        Self->ExecuteProgressTx(std::move(op), ctx);
    }
}

void TPipeline::MaybeActivateWaitingSchemeOps(const TActorContext& ctx) const {
    if (Self->TxPlanWaiting() == 0) {
        ActivateWaitingSchemeOps(ctx);
    }
}

bool TPipeline::CheckInflightLimit() const {
    // check in-flight limit
    size_t totalInFly = (
        Self->TxInFly() +
        Self->ImmediateInFly() +
        Self->ReadIteratorsInFly() +
        Self->MediatorStateWaitingMsgs.size() +
        Self->ProposeQueue.Size() +
        Self->TxWaiting());

    if (totalInFly > Self->GetMaxTxInFly()) {
        return false; // let tx to be rejected
    }

    return true;
}

TPipeline::TWaitingDataTxOp::TWaitingDataTxOp(TAutoPtr<IEventHandle>&& ev)
    : Event(std::move(ev))
{
    if (Event->TraceId) {
        Span = NWilson::TSpan(15 /*max verbosity*/, std::move(Event->TraceId), "DataShard.WaitSnapshot");
        Event->TraceId = Span.GetTraceId();
    }
}

bool TPipeline::AddWaitingTxOp(TEvDataShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx) {
    if (!CheckInflightLimit())
        return false;

    if (Self->MvccSwitchState == TSwitchState::SWITCHING) {
        WaitingDataTxOps.emplace(TRowVersion::Min(), IEventHandle::Upcast<TEvDataShard::TEvProposeTransaction>(std::move(ev)));  // postpone tx processing till mvcc state switch is finished
    } else {
        Y_DEBUG_ABORT_UNLESS(ev->Get()->Record.HasMvccSnapshot());
        TRowVersion snapshot(ev->Get()->Record.GetMvccSnapshot().GetStep(), ev->Get()->Record.GetMvccSnapshot().GetTxId());
        WaitingDataTxOps.emplace(snapshot, IEventHandle::Upcast<TEvDataShard::TEvProposeTransaction>(std::move(ev)));
        const ui64 waitStep = snapshot.Step;
        TRowVersion unreadableEdge;
        if (!Self->WaitPlanStep(waitStep) && snapshot < (unreadableEdge = GetUnreadableEdge())) {
            ActivateWaitingTxOps(unreadableEdge, ctx);  // Async MediatorTimeCastEntry update, need to reschedule the op
        }
    }

    return true;
}

bool TPipeline::AddWaitingTxOp(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    if (!CheckInflightLimit())
        return false;

    if (Self->MvccSwitchState == TSwitchState::SWITCHING) {
        WaitingDataTxOps.emplace(TRowVersion::Min(), IEventHandle::Upcast<NEvents::TDataEvents::TEvWrite>(std::move(ev)));  // postpone tx processing till mvcc state switch is finished
    } else {
        Y_DEBUG_ABORT_UNLESS(ev->Get()->Record.HasMvccSnapshot());
        TRowVersion snapshot(ev->Get()->Record.GetMvccSnapshot().GetStep(), ev->Get()->Record.GetMvccSnapshot().GetTxId());
        WaitingDataTxOps.emplace(snapshot, IEventHandle::Upcast<NEvents::TDataEvents::TEvWrite>(std::move(ev)));
        const ui64 waitStep = snapshot.Step;
        TRowVersion unreadableEdge;
        if (!Self->WaitPlanStep(waitStep) && snapshot < (unreadableEdge = GetUnreadableEdge())) {
            ActivateWaitingTxOps(unreadableEdge, ctx);  // Async MediatorTimeCastEntry update, need to reschedule the op
        }
    }

    return true;
}

void TPipeline::ActivateWaitingTxOps(TRowVersion edge, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " ActivateWaitingTxOps for version# " << edge
        << ", txOps: " << (WaitingDataTxOps.empty() ? "empty" : ToString(WaitingDataTxOps.begin()->first.Step))
        << ", readIterators: "
        << (WaitingDataReadIterators.empty() ? "empty" : ToString(WaitingDataReadIterators.begin()->first.Step)));

    bool isEmpty = WaitingDataTxOps.empty() && WaitingDataReadIterators.empty();
    if (isEmpty || Self->MvccSwitchState == TSwitchState::SWITCHING)
        return;

    bool activated = false;

    for (;;) {
        auto minWait = TRowVersion::Max();
        for (auto it = WaitingDataTxOps.begin(); it != WaitingDataTxOps.end();) {
            if (it->first > TRowVersion::Min() && it->first >= edge) {
                minWait = it->first;
                break;
            }
            it->second.Span.EndOk();
            ctx.Send(it->second.Event.Release());
            it = WaitingDataTxOps.erase(it);
            activated = true;
        }

        for (auto it = WaitingDataReadIterators.begin(); it != WaitingDataReadIterators.end();) {
            if (it->first > TRowVersion::Min() && it->first >= edge) {
                minWait = Min(minWait, it->first);
                break;
            }
            it->second.Span.EndOk();
            ctx.Send(it->second.Event.Release());
            it = WaitingDataReadIterators.erase(it);
            activated = true;
        }

        if (minWait == TRowVersion::Max() ||
            Self->WaitPlanStep(minWait.Step) ||
            minWait >= (edge = GetUnreadableEdge()))
        {
            break;
        }

        // Async MediatorTimeCastEntry update, need to rerun activation
    }

    if (activated) {
        Self->UpdateProposeQueueSize();
    }
}

void TPipeline::ActivateWaitingTxOps(const TActorContext& ctx) {
    bool isEmpty = WaitingDataTxOps.empty() && WaitingDataReadIterators.empty();
    if (isEmpty || Self->MvccSwitchState == TSwitchState::SWITCHING)
        return;

    ActivateWaitingTxOps(GetUnreadableEdge(), ctx);
}

TPipeline::TWaitingReadIterator::TWaitingReadIterator(TEvDataShard::TEvRead::TPtr&& ev)
    : Event(std::move(ev))
{
    if (Event->TraceId) {
        Span = NWilson::TSpan(15 /*max verbosity*/, std::move(Event->TraceId), "DataShard.Read.WaitSnapshot");
        Event->TraceId = Span.GetTraceId();
    }
}

void TPipeline::AddWaitingReadIterator(
    const TRowVersion& version,
    TEvDataShard::TEvRead::TPtr ev,
    const TActorContext& ctx)
{
    // Combined with registration for convenience
    RegisterWaitingReadIterator(TReadIteratorId(ev->Sender, ev->Get()->Record.GetReadId()), ev->Get());

    if (Y_UNLIKELY(Self->MvccSwitchState == TSwitchState::SWITCHING)) {
        // postpone tx processing till mvcc state switch is finished
        WaitingDataReadIterators.emplace(TRowVersion::Min(), std::move(ev));
        return;
    }

    auto readId = ev->Get()->Record.GetReadId();
    WaitingDataReadIterators.emplace(version, std::move(ev));

    const ui64 waitStep = version.Step;
    TRowVersion unreadableEdge = TRowVersion::Min();
    if (!Self->WaitPlanStep(waitStep) && version < (unreadableEdge = GetUnreadableEdge())) {
        // Async MediatorTimeCastEntry update, need to reschedule transaction
        ActivateWaitingTxOps(unreadableEdge, ctx);
    }

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, Self->TabletID() << " put read iterator# " << readId
        << " to wait version# " << version
        << ", waitStep# " << waitStep
        << ", current unreliable edge# " << unreadableEdge);
}

bool TPipeline::HasWaitingReadIterator(const TReadIteratorId& readId) {
    return WaitingReadIteratorsById.contains(readId);
}

bool TPipeline::CancelWaitingReadIterator(const TReadIteratorId& readId) {
    auto it = WaitingReadIteratorsById.find(readId);
    if (it != WaitingReadIteratorsById.end()) {
        it->second->Cancelled = true;
        WaitingReadIteratorsById.erase(it);
        return true;
    }

    return false;
}

void TPipeline::RegisterWaitingReadIterator(const TReadIteratorId& readId, TEvDataShard::TEvRead* event) {
    auto res = WaitingReadIteratorsById.emplace(readId, event);
    Y_ABORT_UNLESS(res.second);
}

bool TPipeline::HandleWaitingReadIterator(const TReadIteratorId& readId, TEvDataShard::TEvRead* event) {
    auto it = WaitingReadIteratorsById.find(readId);
    if (it != WaitingReadIteratorsById.end() && it->second == event) {
        WaitingReadIteratorsById.erase(it);
    }

    return !event->Cancelled;
}

TRowVersion TPipeline::GetReadEdge() const {
    if (Self->TransQueue.PlannedTxs) {
        for (auto order : Self->TransQueue.PlannedTxs) {
            if (!Self->TransQueue.FindTxInFly(order.TxId)->IsReadOnly())
                return TRowVersion(order.Step, order.TxId);
        }
        return TRowVersion(Self->TransQueue.PlannedTxs.rbegin()->Step, Max<ui64>());
    }

    ui64 step = LastCompleteTx.Step;
    if (Self->MediatorTimeCastEntry)
        step = Max(step, Self->MediatorTimeCastEntry->Get(Self->TabletID()));

    return TRowVersion(step, Max<ui64>());
}

TRowVersion TPipeline::GetUnreadableEdge() const {
    const auto last = TRowVersion(
        GetLastActivePlannedOpStep(),
        GetLastActivePlannedOpId());
    auto it = Self->TransQueue.PlannedTxs.upper_bound(TStepOrder(last.Step, last.TxId));
    while (it != Self->TransQueue.PlannedTxs.end()) {
        const auto next = TRowVersion(it->Step, it->TxId);
        if (!Self->TransQueue.FindTxInFly(next.TxId)->IsReadOnly()) {
            // If there's any non-read-only planned tx we don't have in the
            // dependency tracker yet, we absolutely cannot read from that
            // version.
            return next;
        }
        ++it;
    }

    // It looks like we have an empty plan queue (or it's read-only), so we use
    // a rough estimate of a point in time we would use for immediate writes
    // in the distant future. That point in time possibly has some unfinished
    // transactions, but they would be resolved using dependency tracker. Here
    // we use an estimate of the observed mediator step (including possible past
    // generations). Note that we also update CompleteEdge when the distributed
    // queue is empty, but we have been performing immediate writes and thus
    // observing an updated mediator timecast step.
    const ui64 mediatorStep = Self->GetMaxObservedStep();

    // Using an observed mediator step we conclude that we have observed all
    // distributed transactions up to the end of that step.
    const TRowVersion mediatorEdge(mediatorStep, ::Max<ui64>());

    // We are prioritizing reads, and we are ok with blocking immediate writes
    // in the current step. So the first unreadable version is actually in
    // the next step.
    return mediatorEdge.Next();
}

void TPipeline::AddCompletingOp(const TOperation::TPtr& op) {
    CompletingOps.emplace(op->GetTxId(), op);
}

void TPipeline::RemoveCompletingOp(const TOperation::TPtr& op) {
    CompletingOps.erase(op->GetTxId());
}

TOperation::TPtr TPipeline::FindCompletingOp(ui64 txId) const {
    auto it = CompletingOps.find(txId);
    if (it != CompletingOps.end()) {
        return it->second;
    }
    return nullptr;
}

void TPipeline::AddCommittingOp(const TRowVersion& version) {
    if (!Self->IsMvccEnabled())
        return;
    CommittingOps.Add(version);
}

void TPipeline::AddCommittingOp(const TOperation::TPtr& op) {
    if (!Self->IsMvccEnabled() || op->IsReadOnly())
        return;

    TRowVersion version = Self->GetReadWriteVersions(op.Get()).WriteVersion;
    if (op->IsImmediate())
        CommittingOps.Add(op->GetTxId(), version);
    else
        CommittingOps.Add(version);
}

void TPipeline::RemoveCommittingOp(const TRowVersion& version) {
    if (!Self->IsMvccEnabled())
        return;
    CommittingOps.Remove(version);
}

void TPipeline::RemoveCommittingOp(const TOperation::TPtr& op) {
    if (!Self->IsMvccEnabled() || op->IsReadOnly())
        return;

    if (op->IsImmediate())
        CommittingOps.Remove(op->GetTxId());
    else
        CommittingOps.Remove(TRowVersion(op->GetStep(), op->GetTxId()));
}

bool TPipeline::WaitCompletion(const TOperation::TPtr& op) const {
    if (Self->IsFollower() || !Self->IsMvccEnabled() || !op->IsMvccSnapshotRead() || op->HasWaitCompletionFlag())
        return true;

    // don't send errors early
    if(!op->Result() || op->Result()->GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE)
        return true;

    return HasCommittingOpsBelow(op->GetMvccSnapshot());
}

bool TPipeline::HasCommittingOpsBelow(TRowVersion upperBound) const {
    return CommittingOps.HasOpsBelow(upperBound);
}

bool TPipeline::PromoteCompleteEdgeUpTo(const TRowVersion& version, TTransactionContext& txc) {
    if (Self->IsMvccEnabled()) {
        auto it = Self->TransQueue.PlannedTxs.lower_bound(TStepOrder(version.Step, version.TxId));
        if (it != Self->TransQueue.PlannedTxs.begin()) {
            // Promote complete edge to the last distributed transaction that is
            // less than the specified version
            --it;
            return Self->PromoteCompleteEdge(TRowVersion(it->Step, it->TxId), txc);
        }
    }
    return false;
}

bool TPipeline::MarkPlannedLogicallyCompleteUpTo(const TRowVersion& version, TTransactionContext& txc) {
    bool hadWrites = PromoteCompleteEdgeUpTo(version, txc);
    auto processOp = [&](const auto& pr) -> bool {
        TRowVersion prVersion(pr.first.Step, pr.first.TxId);
        if (version <= prVersion) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(!pr.second->IsImmediate());
        Y_DEBUG_ABORT_UNLESS(!pr.second->HasFlag(TTxFlags::BlockingImmediateOps));
        pr.second->SetFlag(TTxFlags::BlockingImmediateOps);
        pr.second->PromoteImmediateConflicts();
        // TODO: we don't want to persist these flags in the future
        PersistTxFlags(pr.second, txc);
        hadWrites = true;
        return true;
    };
    auto it = ActivePlannedOpsLogicallyCompleteEnd;
    while (it != ActivePlannedOpsLogicallyIncompleteEnd) {
        if (!processOp(*it)) {
            return hadWrites;
        }
        // This operation is now in a logically "complete" set
        ActivePlannedOpsLogicallyCompleteEnd = ++it;
    }
    while (it != ActivePlannedOps.end()) {
        if (!processOp(*it)) {
            return hadWrites;
        }
        // This operation is now in a logically "complete" set
        ActivePlannedOpsLogicallyIncompleteEnd = ++it;
        ActivePlannedOpsLogicallyCompleteEnd = it;
    }
    return hadWrites;
}

bool TPipeline::MarkPlannedLogicallyIncompleteUpTo(const TRowVersion& version, TTransactionContext& txc) {
    bool hadWrites = false;
    auto processOp = [&](const auto& pr) -> bool {
        TRowVersion prVersion(pr.first.Step, pr.first.TxId);
        if (version < prVersion) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(!pr.second->IsImmediate());
        Y_DEBUG_ABORT_UNLESS(!pr.second->HasFlag(TTxFlags::BlockingImmediateOps));
        Y_DEBUG_ABORT_UNLESS(!pr.second->HasFlag(TTxFlags::BlockingImmediateWrites));
        pr.second->SetFlag(TTxFlags::BlockingImmediateWrites);
        pr.second->PromoteImmediateWriteConflicts();
        // TODO: we don't want to persist these flags in the future
        PersistTxFlags(pr.second, txc);
        Self->GetSnapshotManager().PromoteIncompleteEdge(pr.second.Get(), txc);
        hadWrites = true;
        return true;
    };
    auto it = ActivePlannedOpsLogicallyIncompleteEnd;
    while (it != ActivePlannedOps.end()) {
        if (!processOp(*it)) {
            return hadWrites;
        }
        // This operation is now in a logically "incomplete" set
        ActivePlannedOpsLogicallyIncompleteEnd = ++it;
    }
    return hadWrites;
}

bool TPipeline::AddLockDependencies(const TOperation::TPtr& op, TLocksUpdate& guardLocks) {
    bool addedDependencies = false;

    guardLocks.FlattenBreakLocks();
    for (auto& lock : guardLocks.BreakLocks) {
        // We cannot break frozen locks
        // Find their corresponding operations and reschedule
        if (lock.IsFrozen()) {
            if (auto conflictOp = FindOp(lock.GetLastOpId())) {
                if (conflictOp != op) {
                    // FIXME: make sure this op is not complete
                    op->AddDependency(conflictOp);
                    addedDependencies = true;
                }
            }
        }
    }

    return addedDependencies;
}

void TPipeline::ProvideGlobalTxId(const TOperation::TPtr& op, ui64 globalTxId) {
    Y_ABORT_UNLESS(op->HasWaitingForGlobalTxIdFlag());
    ui64 localTxId = op->GetTxId();

    auto itImmediate = ImmediateOps.find(localTxId);
    Y_ABORT_UNLESS(itImmediate != ImmediateOps.end());
    ImmediateOps.erase(itImmediate);
    auto itActive = ActiveOps.find(op->GetStepOrder());
    Y_ABORT_UNLESS(itActive != ActiveOps.end());
    ActiveOps.erase(itActive);
    bool removedCandidate = false;
    auto itCandidate = CandidateOps.find(op->GetStepOrder());
    if (itCandidate != CandidateOps.end()) {
        CandidateOps.erase(itCandidate);
        removedCandidate = true;
    }

    op->SetGlobalTxId(globalTxId);
    op->SetWaitingForGlobalTxIdFlag(false);
    auto resImmediate = ImmediateOps.emplace(op->GetTxId(), op);
    Y_ABORT_UNLESS(resImmediate.second);
    auto resActive = ActiveOps.emplace(op->GetStepOrder(), op);
    Y_ABORT_UNLESS(resActive.second);
    if (removedCandidate) {
        auto resCandidate = CandidateOps.emplace(op->GetStepOrder(), op);
        Y_ABORT_UNLESS(resCandidate.second);
    }
}

}}
