#include "propose_tx.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

void IProposeTxOperator::DoSendReply(TColumnShard& owner, const TActorContext& ctx) {
    ctx.Send(GetTxInfo().Source, BuildProposeResultEvent(owner).release());
}

std::unique_ptr<NKikimr::TEvColumnShard::TEvProposeTransactionResult> IProposeTxOperator::BuildProposeResultEvent(
    const TColumnShard& owner) const {
    const auto& txInfo = GetTxInfo();
    std::unique_ptr<TEvColumnShard::TEvProposeTransactionResult> evResult =
        std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(owner.TabletID(), txInfo.TxKind, txInfo.TxId,
            GetProposeStartInfoVerified().GetStatus(), GetProposeStartInfoVerified().GetStatusMessage());
    if (IsFail()) {
        owner.Counters.GetTabletCounters()->IncCounter(COUNTER_PREPARE_ERROR);
        YDB_LOG_ERROR("",
            {"message", GetProposeStartInfoVerified().GetStatusMessage()},
            {"tablet_id", owner.TabletID()},
            {"tx_id", txInfo.TxId});
    } else {
        evResult->Record.SetMinStep(txInfo.MinStep);
        evResult->Record.SetMaxStep(txInfo.MaxStep);
        if (owner.ProcessingParams) {
            evResult->Record.MutableDomainCoordinators()->CopyFrom(owner.ProcessingParams->GetCoordinators());
        }
        owner.Counters.GetTabletCounters()->IncCounter(COUNTER_PREPARE_SUCCESS);
        YDB_LOG_DEBUG("",
            {"message", GetProposeStartInfoVerified().GetStatusMessage()},
            {"tablet_id", owner.TabletID()},
            {"tx_id", txInfo.TxId});
    }
    return evResult;
}

}   // namespace NKikimr::NColumnShard
