#pragma once
#include "session.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/actor.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

class TTxChainActor: public NKikimr::NOlap::NBackground::TSessionActor {
private:
    using TBase = NKikimr::NOlap::NBackground::TSessionActor;
    std::shared_ptr<TTxChainSession> SessionLogic;
    NActors::TActorId TxAllocatorClient;

    void SendCurrentTxToSS() {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("chain_tx", SessionLogic->GetTxData().GetTransactions()[SessionLogic->GetStepForExecute()].DebugString());
        auto evModification = std::make_unique<NEvSchemeShard::TEvModifySchemeTransaction>(SessionLogic->GetCurrentTxIdVerified(), (ui64)TabletId);
        *evModification->Record.AddTransaction() = SessionLogic->GetTxData().GetTransactions()[SessionLogic->GetStepForExecute()];
        NActors::TActivationContext::AsActorContext().Send(TabletActorId, evModification.release());

        auto evRegister = std::make_unique<NEvSchemeShard::TEvNotifyTxCompletion>(SessionLogic->GetCurrentTxIdVerified());
        NActors::TActivationContext::AsActorContext().Send(TabletActorId, evRegister.release());
    }

protected:
    virtual void OnTxCompleted(const ui64 /*txInternalId*/) override {

    }
    virtual void OnSessionProgressSaved() override;
    virtual void OnSessionStateSaved() override {
        if (SessionLogic->IsFinished()) {
            Session->FinishActor();
        } else {
            SendCurrentTxToSS();
        }
    }
    virtual void OnBootstrap(const TActorContext& /*ctx*/) override;

    void Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev);
    void Handle(NEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev);
    void Handle(NEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev);
    void Handle(NEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev);
public:
    TTxChainActor(const std::shared_ptr<NKikimr::NOlap::NBackground::TSession>& session, const std::shared_ptr<NKikimr::NOlap::NBackground::ITabletAdapter>& adapter)
        : TBase(session, adapter)
    {
        AFL_VERIFY(!!Session);
        AFL_VERIFY(!!Adapter);
    }

    STATEFN(StateInProgress) {
        const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_BACKGROUND)("SelfId", SelfId())("TabletId", TabletId);
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvTxAllocatorClient::TEvAllocateResult, Handle);
            hFunc(NEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            hFunc(NEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        default:
            TBase::StateInProgress(ev);
        }
    }

};
}
