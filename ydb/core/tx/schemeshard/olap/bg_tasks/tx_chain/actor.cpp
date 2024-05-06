#include "actor.h"
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

void TTxChainActor::SendTransactionForExecute(const ui64 txId, const NKikimrSchemeOp::TModifyScheme& modification) const {
    auto ev = std::make_unique<NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(txId, (ui64)TabletId);
    *ev->Record.AddTransaction() = modification;
    NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
        new TEvPipeCache::TEvForward(ev.release(), (ui64)TabletId, true), IEventHandle::FlagTrackDelivery, SessionLogic->GetStepForExecute());
}

void TTxChainActor::OnBootstrap(const TActorContext& ctx) {
    Become(&TTxChainActor::StateInProgress);
    SessionLogic = Session->GetLogicAsVerifiedPtr<TTxChainSession>();
    AFL_VERIFY(SessionLogic->GetStepForExecute() < SessionLogic->GetTxData().GetTransactions().size());
    TxAllocatorClient = RegisterWithSameMailbox(CreateTxAllocatorClient(&AppDataVerified()));
    ctx.Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(1));
}

void TTxChainActor::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    AFL_VERIFY(ev->Get()->TxIds.size() == 1);
    SendTransactionForExecute(ev->Get()->TxIds.front(), SessionLogic->GetTxData().GetTransactions()[SessionLogic->GetStepForExecute()]);
}

void TTxChainActor::Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& /*ev*/) {
    SessionLogic->NextStep();
    SaveSessionProgress();
}

void TTxChainActor::OnSessionStateSaved() {
    if (SessionLogic->GetStepForExecute() < SessionLogic->GetTxData().GetTransactions().size()) {
        NActors::TActivationContext::AsActorContext().Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(1));
    }
}

}