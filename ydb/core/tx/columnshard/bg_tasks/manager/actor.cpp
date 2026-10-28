#include "actor.h"

#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_progress.h>
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_state.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_BACKGROUND

namespace NKikimr::NOlap::NBackground {

bool TSessionActor::SendTabletTransaction(std::unique_ptr<NTabletFlatExecutor::ITransaction>&& tx) {
    if (Send<TEvExecuteGeneralLocalTransaction>(TabletActorId, std::move(tx))) {
        return true;
    }
    YDB_LOG_WARN("",
        {"event", "tablet_transaction_send_failed"},
        {"tabletId", TabletId},
        {"selfId", SelfId()});
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
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "save_session_state_skipped"},
            {"selfId", SelfId()},
            {"tabletId", TabletId},
            {"inFlightTx", *SaveSessionStateTx});
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
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "session_actor_local_tx_completed"},
        {"selfId", SelfId()},
        {"tabletId", TabletId},
        {"internalTxId", ev->Get()->GetInternalTxId()},
        {"saveProgressTx", SaveSessionProgressTx ? *SaveSessionProgressTx : 0},
        {"saveStateTx", SaveSessionStateTx ? *SaveSessionStateTx : 0});
    if (SaveSessionProgressTx && *SaveSessionProgressTx == ev->Get()->GetInternalTxId()) {
        SaveSessionProgressTx.reset();
        OnSessionProgressSaved();
    } else if (SaveSessionStateTx && *SaveSessionStateTx == ev->Get()->GetInternalTxId()) {
        SaveSessionStateTx.reset();
        OnSessionStateSaved();
    } else {
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "session_actor_on_tx_completed"},
            {"selfId", SelfId()},
            {"tabletId", TabletId},
            {"internalTxId", ev->Get()->GetInternalTxId()});
        OnTxCompleted(ev->Get()->GetInternalTxId());
    }
}

void TSessionActor::Handle(TEvSessionControl::TPtr& ev) {
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "session_actor_handle_control"},
        {"selfId", SelfId()},
        {"tabletId", TabletId},
        {"saveStateTx", SaveSessionStateTx ? *SaveSessionStateTx : 0},
        {"saveProgressTx", SaveSessionProgressTx ? *SaveSessionProgressTx : 0});
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
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "session_actor_control_saving_state"},
        {"selfId", SelfId()},
        {"tabletId", TabletId});
    SaveSessionState();
}

}   // namespace NKikimr::NOlap::NBackground
