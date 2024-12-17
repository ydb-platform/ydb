#include "actor.h"
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

void TTxChainActor::OnBootstrap(const TActorContext& ctx) {
    Become(&TTxChainActor::StateInProgress);
    SessionLogic = Session->GetLogicAsVerifiedPtr<TTxChainSession>();
    AFL_VERIFY(SessionLogic->GetStepForExecute() < SessionLogic->GetTxData().GetTransactions().size());
    if (SessionLogic->HasCurrentTxId()) {
        SendCurrentTxToSS();
    } else {
        TxAllocatorClient = RegisterWithSameMailbox(CreateTxAllocatorClient(&AppDataVerified()));
        ctx.Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(1));
    }
}

void TTxChainActor::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    AFL_VERIFY(ev->Get()->TxIds.size() == 1);
    const ui64 txId = ev->Get()->TxIds.front();
    SessionLogic->SetCurrentTxId(txId);
    SaveSessionState();
}

void TTxChainActor::Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev) {
    AFL_VERIFY(SessionLogic->GetCurrentTxIdVerified() == ev->Get()->Record.GetTxId());
}

void TTxChainActor::Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
    AFL_VERIFY(SessionLogic->GetCurrentTxIdVerified() == ev->Get()->Record.GetTxId());
    SessionLogic->ResetCurrentTxId();
    SessionLogic->NextStep();
    SaveSessionProgress();
}

void TTxChainActor::Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    AFL_VERIFY(SessionLogic->GetCurrentTxIdVerified() == ev->Get()->Record.GetTxId());
    AFL_VERIFY(ev->Get()->IsAccepted() || ev->Get()->IsDone())("problem", ev->ToString());
}

void TTxChainActor::OnSessionProgressSaved() {
    if (SessionLogic->IsFinished()) {
        SaveSessionState();
    } else {
        NActors::TActivationContext::AsActorContext().Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(1));
    }
}

}