#include "propose_tx.h"

namespace NKikimr::NColumnShard {

void IProposeTxOperator::DoSendReply(TColumnShard& owner, const TActorContext& ctx) {
    if (owner.CurrentSchemeShardId) {
        AFL_VERIFY(owner.CurrentSchemeShardId);
        ctx.Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(BuildProposeResultEvent(owner).release(), (ui64)owner.CurrentSchemeShardId, true));
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "scheme_shard_tablet_not_initialized")("source", GetTxInfo().Source);
        ctx.Send(GetTxInfo().Source, BuildProposeResultEvent(owner).release());
    }
}

std::unique_ptr<NKikimr::TEvColumnShard::TEvProposeTransactionResult> IProposeTxOperator::BuildProposeResultEvent(const TColumnShard& owner) const {
    const auto& txInfo = GetTxInfo();
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> evResult =
        std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(owner.TabletID(), txInfo.TxKind, txInfo.TxId,
            GetProposeStartInfoVerified().GetStatus(), GetProposeStartInfoVerified().GetStatusMessage());
    if (IsFail()) {
        owner.Counters.GetTabletCounters()->IncCounter(COUNTER_PREPARE_ERROR);
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("message", GetProposeStartInfoVerified().GetStatusMessage())("tablet_id", owner.TabletID())(
            "tx_id", txInfo.TxId);
    } else {
        evResult->Record.SetMinStep(txInfo.MinStep);
        evResult->Record.SetMaxStep(txInfo.MaxStep);
        if (owner.ProcessingParams) {
            evResult->Record.MutableDomainCoordinators()->CopyFrom(owner.ProcessingParams->GetCoordinators());
        }
        owner.Counters.GetTabletCounters()->IncCounter(COUNTER_PREPARE_SUCCESS);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", GetProposeStartInfoVerified().GetStatusMessage())("tablet_id", owner.TabletID())(
            "tx_id", txInfo.TxId);
    }
    return evResult;
}

}
