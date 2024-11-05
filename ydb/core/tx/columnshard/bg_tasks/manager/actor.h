#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NOlap::NBackground {

class TSessionActor: public NActors::TActorBootstrapped<TSessionActor> {
private:
    ui64 TxCounter = 0;
    std::optional<ui64> SaveSessionProgressTx;
    std::optional<ui64> SaveSessionStateTx;
    virtual void OnTxCompleted(const ui64 txInternalId) = 0;
    virtual void OnSessionProgressSaved() = 0;
    virtual void OnSessionStateSaved() = 0;
    virtual void OnBootstrap(const TActorContext& ctx) = 0;
protected:
    const TTabletId TabletId;
    const NActors::TActorId TabletActorId;
    std::shared_ptr<ITabletAdapter> Adapter;
    std::shared_ptr<TSession> Session;
    ui64 GetNextTxId() {
        return ++TxCounter;
    }
protected:
    void ExecuteTransaction(std::unique_ptr<NTabletFlatExecutor::ITransaction>&& tx) {
        AFL_VERIFY(Send<TEvExecuteGeneralLocalTransaction>(TabletActorId, std::move(tx)));
    }

    void SaveSessionProgress();

    void SaveSessionState();

    template <class T>
    T* GetShardVerified() const {
        return &Adapter->GetTabletExecutorVerifiedAs<T>();
    }

public:
    TSessionActor(const std::shared_ptr<TSession>& session, const std::shared_ptr<ITabletAdapter>& adapter)
        : TabletId(adapter->GetTabletId())
        , TabletActorId(adapter->GetTabletActorId())
        , Adapter(adapter)
        , Session(session)
    {
        AFL_VERIFY(!!Session);
        AFL_VERIFY(!!Adapter);
    }

    void Handle(TEvLocalTransactionCompleted::TPtr& ev);

    void Handle(TEvSessionControl::TPtr& ev);

    STATEFN(StateInProgress) {
        const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_BACKGROUND)("SelfId", SelfId())("TabletId", TabletId);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLocalTransactionCompleted, Handle);
            hFunc(TEvSessionControl, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        default:
            AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        OnBootstrap(ctx);
    }

};

}