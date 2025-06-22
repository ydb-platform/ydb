#pragma once
#include "events.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TSharedMetadataAccessorCacheActor: public TActorBootstrapped<TSharedMetadataAccessorCacheActor> {
private:
    THashMap<TActorId, TLocalManager> Managers;
    ui64 TotalMemorySize = 1 << 30;
    std::shared_ptr<IGranuleDataAccessor::TSharedMetadataAccessorCache> MetadataCache;
    std::shared_ptr<IAccessorCallbackWithOwner> AccessorsCallback;

    void StartStopping() {
        AFL_VERIFY(false)("shared_metadata_accessor_cache", "trying to call StartStopping()");
        PassAway();
    }

    void Handle(TEvRegisterController::TPtr& ev) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "TEvRegisterController")("owner", ev->Get()->GetOwner());
        auto controller = ev->Get()->ExtractController();
        auto owner = ev->Get()->GetOwner();
        controller->SetCache(MetadataCache);
        controller->SetOwner(owner);
        auto manager = Managers.find(owner);
        if (manager == Managers.end()) {
            manager = Managers.emplace(owner, std::make_shared<TCallbackWrapper>(AccessorsCallback, owner)).first;
        }
        manager->second.RegisterController(move(controller), ev->Get()->GetIsUpdateFlag());
    }
    void Handle(TEvUnregisterController::TPtr& ev) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "TEvUnregisterController")("owner", ev->Get()->GetOwner());
        if (auto manager = Managers.find(ev->Get()->GetOwner()); manager != Managers.end()) {
            manager->second.UnregisterController(ev->Get()->GetPathId());
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "owner_not_found");
        }
    }
    void Handle(TEvAddPortion::TPtr& ev) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "TEvAddPortion")("owner", ev->Get()->GetOwner());
        if (auto manager = Managers.find(ev->Get()->GetOwner()); manager != Managers.end()) {
            for (auto&& a : ev->Get()->ExtractAccessors()) {
                manager->second.AddPortion(std::move(a));
            }
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "owner_not_found");
        }
    }

    void Handle(TEvRemovePortion::TPtr& ev) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "TEvRemovePortion")("owner", ev->Get()->GetOwner());
        if (auto manager = Managers.find(ev->Get()->GetOwner()); manager != Managers.end()) {
            manager->second.RemovePortion(ev->Get()->GetPortion());
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "owner_not_found");
        }
    }
   void Handle(TEvAskServiceDataAccessors::TPtr& ev) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "TEvAskServiceDataAccessors")("owner", ev->Get()->GetOwner());
        if (auto manager = Managers.find(ev->Get()->GetOwner()); manager != Managers.end()) {
            manager->second.AskData(ev->Get()->GetRequest());
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "owner_not_found");
        }
    }
    void Handle(TEvClearCache::TPtr& ev) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "TEvClearCache")("owner", ev->Get()->GetOwner());
        if (auto manager = Managers.find(ev->Get()->GetOwner()); manager != Managers.end()) {
            Managers.erase(manager);
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("shared_metadata_accessor_cache", "owner_not_found");
        }

    }

public:

    static inline TActorId MakeActorId(ui32 nodeId) {
        char x[12] = {'s', 'h', 'a', 'r', 'e',
            'd', 'm', 'e', 't', 'a', 'd', 't'};
        return TActorId(nodeId, TStringBuf(x, 12));
    }

    static NActors::IActor* CreateActor();

    TSharedMetadataAccessorCacheActor() = default;
    ~TSharedMetadataAccessorCacheActor() = default;

    void Bootstrap();

    STFUNC(StateWait) {
        const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("self_id", SelfId());
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, StartStopping);
            hFunc(TEvRegisterController, Handle);
            hFunc(TEvUnregisterController, Handle);
            hFunc(TEvAskServiceDataAccessors, Handle);
            hFunc(TEvRemovePortion, Handle);
            hFunc(TEvAddPortion, Handle);
            hFunc(TEvClearCache, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
