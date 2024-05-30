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
        auto evModification = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(SessionLogic->GetCurrentTxIdVerified(), (ui64)TabletId);
        *evModification->Record.AddTransaction() = SessionLogic->GetTxData().GetTransactions()[SessionLogic->GetStepForExecute()];
        NActors::TActivationContext::AsActorContext().Send(TabletActorId, evModification.release());

        auto evRegister = std::make_unique<TEvSchemeShard::TEvNotifyTxCompletion>(SessionLogic->GetCurrentTxIdVerified());
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
    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev);
    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev);
    void Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev);
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
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvTxAllocatorClient::TEvAllocateResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        default:
            TBase::StateInProgress(ev);
        }
    }

};
}