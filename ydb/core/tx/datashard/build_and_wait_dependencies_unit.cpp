#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <ydb/core/tx/locks/time_counters.h>

namespace NKikimr {
namespace NDataShard {

class TBuildAndWaitDependenciesUnit : public TExecutionUnit {
public:
    TBuildAndWaitDependenciesUnit(TDataShard &dataShard,
                                  TPipeline &pipeline);
    ~TBuildAndWaitDependenciesUnit() override;

    bool HasDirectBlockers(const TOperation::TPtr& op) const;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    void BuildDependencies(const TOperation::TPtr &op);
    bool BuildVolatileDependencies(const TOperation::TPtr &op);
};

TBuildAndWaitDependenciesUnit::TBuildAndWaitDependenciesUnit(TDataShard &dataShard,
                                                             TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::BuildAndWaitDependencies, false, dataShard, pipeline)
{
}

TBuildAndWaitDependenciesUnit::~TBuildAndWaitDependenciesUnit()
{
}

/**
 * Returns true if operation has blockers that prevent it from starting
 */
bool TBuildAndWaitDependenciesUnit::HasDirectBlockers(const TOperation::TPtr& op) const
{
    Y_DEBUG_ABORT_UNLESS(op->IsWaitingDependencies());

    return !op->GetDependencies().empty()
        || !op->GetSpecialDependencies().empty()
        || op->HasVolatileDependencies();
}

bool TBuildAndWaitDependenciesUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    // Dependencies were not built yet. Allow to execute to build dependencies.
    if (!op->IsWaitingDependencies())
        return true;

    // Perform fast checks for existing blockers
    if (HasDirectBlockers(op))
        return false;

    // Looks like nothing else is preventing us from starting
    return true;
}

EExecutionStatus TBuildAndWaitDependenciesUnit::Execute(TOperation::TPtr op,
                                                        TTransactionContext &txc,
                                                        const TActorContext &ctx)
{
    // Build dependencies if not yet.
    if (!op->IsWaitingDependencies()) {
        BuildDependencies(op);
        BuildVolatileDependencies(op);

        // After dependencies are built we can add operation to active ops.
        // For planned operations it means we can load and process the next
        // one.
        Pipeline.AddActiveOp(op);
        Pipeline.AddExecuteBlocker(op);
        op->SetWaitingDependenciesFlag();

        if (!op->IsImmediate()) {
            if (Pipeline.RemoveProposeDelayer(op->GetTxId())) {
                DataShard.CheckDelayedProposeQueue(ctx);
            }

            // We have added a new operation to dependency tracker
            // This means our unreadable edge may have changed and we need
            // to activate some waiting snapshot reads
            Pipeline.ActivateWaitingTxOps(ctx);
        }

        if (!IsReadyToExecute(op)) {
            // Cache write keys while operation waits in the queue
            Pipeline.RegisterDistributedWrites(op, txc.DB);

            TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
            if (tx) {
                // We should put conflicting tx into cache
                // or release its data.
                ui64 mem = tx->GetMemoryConsumption();
                if (DataShard.TryCaptureTxCache(mem)) {
                    tx->SetTxCacheUsage(mem);
                } else {
                    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                               "TBuildAndWaitDependenciesUnit at " << DataShard.TabletID()
                               << " released data for tx " << tx->GetTxId());

                    DataShard.IncCounter(COUNTER_INACTIVE_TX_DATA_RELEASES);
                    tx->ReleaseTxData(txc, ctx);
                }
            }

            DataShard.IncCounter(COUNTER_TX_WAIT_ORDER);

            return EExecutionStatus::Continue;
        }
    } else if (BuildVolatileDependencies(op)) {
        // We acquired new volatile dependencies, wait for them too
        Y_ABORT_UNLESS(!IsReadyToExecute(op));
        return EExecutionStatus::Continue;
    }

    DataShard.IncCounter(COUNTER_WAIT_DEPENDENCIES_LATENCY_MS, op->GetCurrentElapsedAndReset().MilliSeconds());

    op->ResetWaitingDependenciesFlag();
    op->MarkAsExecuting();

    // Replicate legacy behavior when mvcc is not enabled
    // When mvcc enabled we don't mark transactions until as late as possible
    if (!DataShard.IsMvccEnabled()) {
        bool hadWrites = false;
        if (!op->IsImmediate()) {
            // If we start planned operation out of order, then all preceding
            // planned operations must become blocking for conflicting immediate
            // operations. This is to avoid any P2-I-P1 ordering where it is
            // revealed that planned operations are executed out of order.
            hadWrites |= Pipeline.MarkPlannedLogicallyCompleteUpTo(TRowVersion(op->GetStep(), op->GetTxId()), txc);
        }

        if (hadWrites) {
            return EExecutionStatus::ExecutedNoMoreRestarts;
        }
    }

    return EExecutionStatus::Executed;
}

void TBuildAndWaitDependenciesUnit::BuildDependencies(const TOperation::TPtr &op) {
    TMicrosecTimerCounter measureBuildDependencies(DataShard, COUNTER_BUILD_DEPENDENCIES_USEC);

    Pipeline.GetDepTracker().AddOperation(op);

    // Scheme operations must have a hard dependency on non-completed earlier
    // operations, even when those operations are using a snapshot. Snapshots
    // don't preserve schema, so dropping columns or tables is unsafe until
    // they are completed.
    if (op->IsSchemeTx() && !op->IsReadOnly()) {
        auto *tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        for (const auto &pr : Pipeline.GetActivePlannedOps()) {
            Y_VERIFY_S(pr.first < op->GetStepOrder(),
                "unexpected tx " << pr.first.ToString()
                << " when adding " << op->GetStepOrder().ToString());
            if (!op->IsCompleted()) {
                op->AddSpecialDependency(pr.second);
            }
        }

        // For DropTable we must also wait for all immediate operations to complete
        auto &schemeTx = tx->GetSchemeTx();
        if (schemeTx.HasDropTable()) {
            for (const auto &pr : Pipeline.GetImmediateOps()) {
                op->AddSpecialDependency(pr.second);
            }
        }
    }

    op->ResetCurrentTimer();
}

bool TBuildAndWaitDependenciesUnit::BuildVolatileDependencies(const TOperation::TPtr &op) {
    // Scheme operations need to wait for all volatile transactions below them
    if (op->IsSchemeTx()) {
        TRowVersion current(op->GetStep(), op->GetTxId());
        for (auto* info : DataShard.GetVolatileTxManager().GetVolatileTxByVersion()) {
            if (current < info->Version) {
                break;
            }
            op->AddVolatileDependency(info->TxId);
            bool added = DataShard.GetVolatileTxManager()
                .AttachWaitingRemovalOperation(info->TxId, op->GetTxId());
            Y_ABORT_UNLESS(added);
        }
    }

    return op->HasVolatileDependencies();
}

void TBuildAndWaitDependenciesUnit::Complete(TOperation::TPtr,
                                             const TActorContext &)
{
}

THolder<TExecutionUnit> CreateBuildAndWaitDependenciesUnit(TDataShard &dataShard,
                                                           TPipeline &pipeline)
{
    return THolder(new TBuildAndWaitDependenciesUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
