#include "propose_tx.h"

namespace NKikimr::NColumnShard {

void IProposeTxOperator::DoSendReply(TColumnShard& owner, const TActorContext& ctx) {
    const auto& txInfo = GetTxInfo();
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> evResult = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
        owner.TabletID(), txInfo.TxKind, txInfo.TxId, GetProposeStartInfoVerified().GetStatus(), GetProposeStartInfoVerified().GetStatusMessage());
    if (IsFail()) {
        owner.Stats.GetTabletCounters().IncCounter(COUNTER_PREPARE_ERROR);
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("message", GetProposeStartInfoVerified().GetStatusMessage())("tablet_id", owner.TabletID())("tx_id", txInfo.TxId);
    } else {
        evResult->Record.SetMinStep(txInfo.MinStep);
        evResult->Record.SetMaxStep(txInfo.MaxStep);
        if (owner.ProcessingParams) {
            evResult->Record.MutableDomainCoordinators()->CopyFrom(owner.ProcessingParams->GetCoordinators());
        }
        owner.Stats.GetTabletCounters().IncCounter(COUNTER_PREPARE_SUCCESS);
    }
    ctx.Send(txInfo.Source, evResult.release());
}

}
