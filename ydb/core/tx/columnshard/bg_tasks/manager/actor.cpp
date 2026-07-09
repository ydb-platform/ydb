#include "actor.h"

#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_progress.h>
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_state.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NBackground {

bool TSessionActor::SendTabletTransaction(std::unique_ptr<NTabletFlatExecutor::ITransaction>&& tx) {
    if (Send<TEvExecuteGeneralLocalTransaction>(TabletActorId, std::move(tx))) {
        return true;
    }
    AFL_WARN(NKikimrServices::TX_BACKGROUND)("event", "tablet_transaction_send_failed")("tablet_id", TabletId)("self_id", SelfId());
    return false;
}

void TSessionActor::SaveSessionProgress() {
    AFL_VERIFY(!SaveSessionProgressTx);
    const ui64 txId = GetNextTxId();
    SaveSessionProgressTx.emplace(txId);
    auto tx = std::make_unique<TTxSaveSessionProgress>(Session, SelfId(), Adapter, txId);
    if (!SendTabletTransaction(std::move(tx))) {
        SaveSessionProgressTx.reset();
        PassAway();
    }
}

void TSessionActor::SaveSessionState() {
    if (SaveSessionStateTx) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "save_session_state_skipped")("self_id", SelfId())("tablet_id", TabletId)(
            "in_flight_tx", *SaveSessionStateTx);
        return;
    }
    const ui64 txId = GetNextTxId();
    SaveSessionStateTx.emplace(txId);
    auto tx = std::make_unique<TTxSaveSessionState>(Session, SelfId(), Adapter, txId);
    if (!SendTabletTransaction(std::move(tx))) {
        SaveSessionStateTx.reset();
        PassAway();
    }
}

void TSessionActor::Handle(TEvLocalTransactionCompleted::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "session_actor_local_tx_completed")("self_id", SelfId())("tablet_id", TabletId)(
        "internal_tx_id", ev->Get()->GetInternalTxId())("save_progress_tx", SaveSessionProgressTx ? *SaveSessionProgressTx : 0)(
        "save_state_tx", SaveSessionStateTx ? *SaveSessionStateTx : 0);
    if (SaveSessionProgressTx && *SaveSessionProgressTx == ev->Get()->GetInternalTxId()) {
        SaveSessionProgressTx.reset();
        OnSessionProgressSaved();
    } else if (SaveSessionStateTx && *SaveSessionStateTx == ev->Get()->GetInternalTxId()) {
        SaveSessionStateTx.reset();
        OnSessionStateSaved();
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "session_actor_on_tx_completed")("self_id", SelfId())("tablet_id", TabletId)(
            "internal_tx_id", ev->Get()->GetInternalTxId());
        OnTxCompleted(ev->Get()->GetInternalTxId());
    }
}

void TSessionActor::Handle(TEvSessionControl::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "session_actor_handle_control")("self_id", SelfId())("tablet_id", TabletId)(
        "save_state_tx", SaveSessionStateTx ? *SaveSessionStateTx : 0)("save_progress_tx", SaveSessionProgressTx ? *SaveSessionProgressTx : 0);
    TSessionControlContainer control;
    {
        auto conclusion = control.DeserializeFromProto(ev->Get()->Record);
        if (conclusion.IsFail()) {
            control.GetChannelContainer()->OnFail(conclusion.GetErrorMessage());
            return;
        }
    }
    {
        auto conclusion = control.GetLogicControlContainer()->Apply(Session->GetLogicContainer().GetObjectPtrVerified());
        if (conclusion.IsFail()) {
            control.GetChannelContainer()->OnFail(conclusion.GetErrorMessage());
            return;
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "session_actor_control_saving_state")("self_id", SelfId())("tablet_id", TabletId);
    SaveSessionState();
}

}   // namespace NKikimr::NOlap::NBackground
