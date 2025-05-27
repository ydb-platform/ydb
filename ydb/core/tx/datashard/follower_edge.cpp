#include "datashard_impl.h"

namespace NKikimr::NDataShard {

std::tuple<TRowVersion, bool, ui64> TDataShard::CalculateFollowerReadEdge() const {
    Y_ENSURE(!IsFollower());

    TRowVersion volatileUncertain = VolatileTxManager.GetMinUncertainVersion();

    for (auto order : TransQueue.GetPlan()) {
        // When we have planned operations we assume the first one may be used
        // for new writes, so we mark is as non-repeatable. We could skip
        // readonly operations, but there's little benefit in that, and it's
        // complicated to determine which is the first readable given we may
        // have executed some out of order.
        return { Min(volatileUncertain, TRowVersion(order.Step, order.TxId)), false, 0 };
    }

    if (!volatileUncertain.IsMax()) {
        // We have some uncertainty in an unresolved volatile commit
        // Allow followers to read from it in non-repeatable snapshot modes
        // FIXME: when at least one write is committed at this version, it
        // should stop being non-repeatable, and followers need to resolve
        // other possibly out-of-order commits.
        return { volatileUncertain, false, 0 };
    }

    // This is the max version where we had any writes
    TRowVersion maxWrite(SnapshotManager.GetCompleteEdge().Step, Max<ui64>());
    if (maxWrite < SnapshotManager.GetImmediateWriteEdge()) {
        maxWrite = SnapshotManager.GetImmediateWriteEdge();
    }

    // This is the next version that would be used for new writes
    TRowVersion nextWrite = GetMvccTxVersion(EMvccTxMode::ReadWrite);

    if (maxWrite < nextWrite) {
        return { maxWrite, true, 0 };
    }

    TRowVersion maxObserved(GetMaxObservedStep(), Max<ui64>());
    if (maxObserved < maxWrite) {
        return { maxObserved, true, maxWrite.Step };
    }

    return { maxWrite, false, maxWrite.Next().Step };
}

bool TDataShard::PromoteFollowerReadEdge(TTransactionContext& txc) {
    Y_ENSURE(!IsFollower());

    if (HasFollowers()) {
        auto [version, repeatable, waitStep] = CalculateFollowerReadEdge();

        if (waitStep) {
            WaitPlanStep(waitStep);
        }

        return SnapshotManager.PromoteFollowerReadEdge(version, repeatable, txc);
    }

    return false;
}

class TDataShard::TTxUpdateFollowerReadEdge
    : public NTabletFlatExecutor::TTransactionBase<TDataShard>
{
public:
    TTxUpdateFollowerReadEdge(TDataShard* self)
        : TBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_UPDATE_FOLLOWER_READ_EDGE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Y_ENSURE(Self->UpdateFollowerReadEdgePending);
        Self->UpdateFollowerReadEdgePending = false;
        Self->PromoteFollowerReadEdge(txc);
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }
};

bool TDataShard::PromoteFollowerReadEdge() {
    Y_ENSURE(!IsFollower());

    if (HasFollowers()) {
        auto [currentEdge, currentRepeatable] = SnapshotManager.GetFollowerReadEdge();
        auto [nextEdge, nextRepeatable, waitStep] = CalculateFollowerReadEdge();

        if (currentEdge < nextEdge || currentEdge == nextEdge && !currentRepeatable && nextRepeatable) {
            if (!UpdateFollowerReadEdgePending) {
                UpdateFollowerReadEdgePending = true;
                Execute(new TTxUpdateFollowerReadEdge(this));
            }
            return true;
        } else if (waitStep) {
            WaitPlanStep(waitStep);
        }
    }

    return false;
}

void TDataShard::OnFollowersCountChanged() {
    if (HasFollowers()) {
        PromoteFollowerReadEdge();
    }
}

bool TDataShard::HasFollowers() const {
    return Executor()->GetStats().FollowersCount > 0;
}

} // namespace NKikimr::NDataShard
