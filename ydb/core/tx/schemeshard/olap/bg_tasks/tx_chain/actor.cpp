#include "actor.h"
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

void TTxChainActor::OnBootstrap(const TActorContext& ctx) {
    Become(&TTxChainActor::StateInProgress);
    SessionLogic = Session->GetLogicAsVerifiedPtr<TTxChainSession>();
    AFL_VERIFY(SessionLogic->GetStepForExecute() < SessionLogic->GetTxData().GetTransactions().size());
    TxAllocatorClient = RegisterWithSameMailbox(CreateTxAllocatorClient(&AppDataVerified()));
    ctx.Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(1));
}

void TTxChainActor::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    AFL_VERIFY(!WaitTx);
    AFL_VERIFY(ev->Get()->TxIds.size() == 1);
    const ui64 txId = ev->Get()->TxIds.front();
    auto evModification = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, (ui64)TabletId);
    *evModification->Record.AddTransaction() = SessionLogic->GetTxData().GetTransactions()[SessionLogic->GetStepForExecute()];
    NActors::TActivationContext::AsActorContext().Send(TabletActorId, evModification.release());

    auto evRegister = std::make_unique<TEvSchemeShard::TEvNotifyTxCompletion>(txId);
    NActors::TActivationContext::AsActorContext().Send(TabletActorId, evRegister.release());
    WaitTx = true;
}

void TTxChainActor::Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& /*ev*/) {
    AFL_VERIFY(WaitTx);
}

void TTxChainActor::Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& /*ev*/) {
    AFL_VERIFY(WaitTx);
    SessionLogic->NextStep();
    SaveSessionProgress();
    WaitTx = false;
}

void TTxChainActor::Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    AFL_VERIFY(WaitTx);
    AFL_VERIFY(ev->Get()->IsAccepted() || ev->Get()->IsDone())("problem", ev->ToString());
}

void TTxChainActor::OnSessionProgressSaved() {
    if (SessionLogic->GetStepForExecute() < SessionLogic->GetTxData().GetTransactions().size()) {
        NActors::TActivationContext::AsActorContext().Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(1));
    } else {
        Session->FinishActor();
    }
}

}