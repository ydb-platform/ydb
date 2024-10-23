#pragma once
#include "counters.h"
#include "manager.h"

#include <ydb/core/tx/priorities/usage/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPrioritiesQueue {

class TDistributor: public TActorBootstrapped<TDistributor> {
private:
    std::shared_ptr<TCounters> Counters;
    const TString QueueName = "common";
    const TConfig Config;
    std::unique_ptr<TManager> Manager;

    void Handle(TEvExecution::TEvRegisterClient::TPtr& ev) {
        Manager->RegisterClient(ev->Get()->GetClientId());
    }

    void Handle(TEvExecution::TEvUnregisterClient::TPtr& ev) {
        Manager->UnregisterClient(ev->Get()->GetClientId());
    }

    void Handle(TEvExecution::TEvAsk::TPtr& ev) {
        Manager->Ask(ev->Get()->GetClientId(), ev->Get()->GetCount(), ev->Get()->GetRequest(), ev->Get()->GetPriority());
    }

    void Handle(TEvExecution::TEvAskMax::TPtr& ev) {
        Manager->AskMax(ev->Get()->GetClientId(), ev->Get()->GetCount(), ev->Get()->GetRequest(), ev->Get()->GetPriority());
    }

    void Handle(TEvExecution::TEvFree::TPtr& ev) {
        Manager->Free(ev->Get()->GetClientId(), ev->Get()->GetCount());
    }

public:
    STATEFN(StateMain) {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("name", QueueName)("actor_id", SelfId());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecution::TEvRegisterClient, Handle);
            hFunc(TEvExecution::TEvUnregisterClient, Handle);
            hFunc(TEvExecution::TEvAsk, Handle);
            hFunc(TEvExecution::TEvAskMax, Handle);
            hFunc(TEvExecution::TEvFree, Handle);
            default:
                AFL_ERROR(NKikimrServices::TX_PRIORITIES_QUEUE)("problem", "unexpected event for task executor")("ev_type", ev->GetTypeName());
                break;
        }
    }

    TDistributor(const TConfig& config, const TString& queueName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals);

    void Bootstrap();
};

}   // namespace NKikimr::NPrioritiesQueue
