#include "actor.h"
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_progress.h>
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_state.h>

namespace NKikimr::NOlap::NBackground {

void TSessionActor::SaveSessionProgress() {
    AFL_VERIFY(!SaveSessionProgressTx);
    const ui64 txId = GetNextTxId();
    SaveSessionProgressTx.emplace(txId);
    auto tx = std::make_unique<TTxSaveSessionProgress>(Session, SelfId(), Adapter, txId);
    AFL_VERIFY(Send<TEvExecuteGeneralLocalTransaction>(TabletActorId, std::move(tx)));
}

void TSessionActor::SaveSessionState() {
    AFL_VERIFY(!SaveSessionStateTx);
    const ui64 txId = GetNextTxId();
    SaveSessionStateTx.emplace(txId);
    auto tx = std::make_unique<TTxSaveSessionState>(Session, SelfId(), Adapter, txId);
    AFL_VERIFY(Send<TEvExecuteGeneralLocalTransaction>(TabletActorId, std::move(tx)));
}

void TSessionActor::Handle(TEvLocalTransactionCompleted::TPtr& ev) {
    if (SaveSessionProgressTx && *SaveSessionProgressTx == ev->Get()->GetInternalTxId()) {
        SaveSessionProgressTx.reset();
        OnSessionProgressSaved();
    } else if (SaveSessionStateTx && *SaveSessionStateTx == ev->Get()->GetInternalTxId()) {
        SaveSessionStateTx.reset();
        OnSessionStateSaved();
    } else {
        OnTxCompleted(ev->Get()->GetInternalTxId());
    }
}

void TSessionActor::Handle(TEvSessionControl::TPtr& ev) {
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
    SaveSessionState();
}

}