#include "datashard_txs.h"

namespace NKikimr::NDataShard {

void TDataShard::CheckMvccStateChangeCanStart(const TActorContext& ctx) {
    switch (MvccSwitchState) {
        case TSwitchState::READY:
            switch (State) {
                case TShardState::WaitScheme:
                case TShardState::SplitDstReceivingSnapshot:
                    // Recheck after while
                    return;

                case TShardState::Ready:
                case TShardState::Frozen: {
                    const auto enable = AppData(ctx)->FeatureFlags.GetEnableMvcc();
                    if (enable && *enable != IsMvccEnabled()) {
                        MvccSwitchState = TSwitchState::SWITCHING;
                    } else {
                        MvccSwitchState = TSwitchState::DONE;
                        return;
                    }

                    break;
                }

                case TShardState::Uninitialized:
                case TShardState::Unknown:
                    // We cannot start checking before shard initialization

                    Y_VERIFY_DEBUG(false, "Unexpected shard state State:%d", State);
                    [[fallthrough]];

                case TShardState::Readonly:
                    // Don't switch the state on follower
                    [[fallthrough]];

                case TShardState::Offline:
                case TShardState::PreOffline:
                case TShardState::SplitSrcWaitForNoTxInFlight:
                case TShardState::SplitSrcMakeSnapshot:
                case TShardState::SplitSrcSendingSnapshot:
                case TShardState::SplitSrcWaitForPartitioningChanged:
                    // Don't switch the state while splitting or stopping
                    [[fallthrough]];

                default:
                    // How we ran into it??

                    MvccSwitchState = TSwitchState::DONE;

                    return;
            }
            [[fallthrough]];

        case TSwitchState::SWITCHING: {
            ui64 txInFly = TxInFly();
            ui64 immediateTxInFly = ImmediateInFly();
            SetCounter(COUNTER_MVCC_STATE_CHANGE_WAIT_TX_IN_FLY, txInFly);
            SetCounter(COUNTER_MVCC_STATE_CHANGE_WAIT_IMMEDIATE_TX_IN_FLY, immediateTxInFly);
            if (txInFly == 0 && immediateTxInFly == 0 && !Pipeline.HasWaitingSchemeOps())
                Execute(CreateTxExecuteMvccStateChange(), ctx);
            break;
        }
        case TSwitchState::DONE:
            return;
    }
}

TDataShard::TTxExecuteMvccStateChange::TTxExecuteMvccStateChange(TDataShard* ds)
    : TBase(ds) {}

bool TDataShard::TTxExecuteMvccStateChange::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    if (Self->MvccSwitchState == TSwitchState::DONE)
        return true; // already switched

    if (Self->State == TShardState::Ready || Self->State == TShardState::Frozen) {
        Y_VERIFY(Self->TxInFly() == 0 && Self->ImmediateInFly() == 0);

        auto [step, txId] = Self->LastCompleteTxVersion();
        Self->SnapshotManager.ChangeMvccState(step, txId, txc,
            *AppData(ctx)->FeatureFlags.GetEnableMvcc() ? EMvccState::MvccEnabled : EMvccState::MvccDisabled);

        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, TStringBuilder() << "TTxExecuteMvccStateChange.Execute"
            << " MVCC state switched to" << (*AppData(ctx)->FeatureFlags.GetEnableMvcc() ? " enabled" : " disabled") << " state");

        ActivateWaitingOps = true;
    }

    Self->MvccSwitchState = TSwitchState::DONE;

    return true;
}

void TDataShard::TTxExecuteMvccStateChange::Complete(const TActorContext& ctx) {
    if (ActivateWaitingOps)
        Self->Pipeline.ActivateWaitingTxOps(ctx);
}

NTabletFlatExecutor::ITransaction* TDataShard::CreateTxExecuteMvccStateChange() {
    return new TTxExecuteMvccStateChange(this);
}

}
