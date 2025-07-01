#pragma once
#include "counters.h"
#include "manager.h"

#include <ydb/core/tx/general_cache/source/events.h>
#include <ydb/core/tx/general_cache/usage/config.h>
#include <ydb/core/tx/general_cache/usage/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGeneralCache::NPrivate {

template <class TPolicy>
class TDistributor: public TActorBootstrapped<TDistributor<TPolicy>> {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using TManager = TManager<TPolicy>;
    using TRequest = TRequest<TPolicy>;

    using TBase = TActorBootstrapped<TDistributor<TPolicy>>;
    const NPublic::TConfig Config;
    const TActorCounters Counters;
    std::unique_ptr<TManager> Manager;

    void HandleMain(NPublic::TEvents<TPolicy>::TEvAskData::TPtr& ev) {
        Manager->AddRequest(std::make_shared<TRequest>(ev->Get()->ExtractAddresses(), ev->Get()->ExtractCallback(), ev->Get()->GetConsumer()));
    }

    void HandleMain(NPublic::TEvents<TPolicy>::TEvUpdateMaxCacheSize::TPtr& ev) {
        Manager->UpdateMaxCacheSize(ev->Get()->GetMaxCacheSize());
    }

    void HandleMain(NSource::TEvents<TPolicy>::TEvObjectsInfo::TPtr& ev) {
        Manager->OnRequestResult(ev->Get()->GetSourceId(), ev->Get()->ExtractObjects(), ev->Get()->ExtractRemoved(), ev->Get()->ExtractErrors());
    }

    void HandleMain(NSource::TEvents<TPolicy>::TEvAdditionalObjectsInfo::TPtr& ev) {
        Manager->OnAdditionalObjectsInfo(ev->Get()->GetSourceId(), ev->Get()->ExtractAddObjects(), ev->Get()->ExtractRemoveObjects());
    }

    void HandleMain(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        Manager->AbortSource(Manager->GetSourceByCookie(ev->Cookie));
    }

    void HandleMain(NPublic::TEvents<TPolicy>::TEvKillSource::TPtr& ev) {
        Manager->AbortSource(ev->Get()->GetSourceId());
    }

public:
    STATEFN(StateMain) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("name", ConveyorName)
        //            ("workers", Workers.size())("waiting", Waiting.size())("actor_id", SelfId());
        switch (ev->GetTypeRewrite()) {
            hFunc(NPublic::TEvents<TPolicy>::TEvAskData, HandleMain);
            hFunc(NPublic::TEvents<TPolicy>::TEvKillSource, HandleMain);
            hFunc(NPublic::TEvents<TPolicy>::TEvUpdateMaxCacheSize, HandleMain);
            hFunc(NSource::TEvents<TPolicy>::TEvObjectsInfo, HandleMain);
            hFunc(NSource::TEvents<TPolicy>::TEvAdditionalObjectsInfo, HandleMain);
            hFunc(NActors::TEvents::TEvUndelivered, HandleMain);
            default:
                AFL_ERROR(NKikimrServices::TX_CONVEYOR)("problem", "unexpected event for general cache")("ev_type", ev->GetTypeName());
                break;
        }
    }

    TDistributor(const NPublic::TConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
        : Config(config)
        , Counters(TPolicy::GetCacheName(), conveyorSignals, config) {
    }

    ~TDistributor() {
    }

    void Bootstrap() {
        Manager = std::make_unique<TManager>(TBase::SelfId(), Counters.GetManager());
        TBase::Become(&TDistributor::StateMain);
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
