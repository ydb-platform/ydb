#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NColumnShard::NLoading {
class TSpecialValuesInitializer;
};

namespace NKikimr::NColumnShard {

class TSpaceWatcher : public TActorBootstrapped<TSpaceWatcher> {
    TColumnShard* Self;
    NActors::TActorId FindSubDomainPathIdActor;
    std::optional<NKikimr::TLocalPathId> SubDomainPathId;
    std::optional<NKikimr::TLocalPathId> WatchingSubDomainPathId;
    bool SubDomainOutOfSpace = false;

public:
    friend class TColumnShard;
    friend class TTxInit;
    friend class TTxPersistSubDomainOutOfSpace;
    friend class TTxPersistSubDomainPathId;
    friend class NKikimr::NColumnShard::NLoading::TSpecialValuesInitializer;
    friend class TTxMonitoring;

public:
    TSpaceWatcher(TColumnShard* self)
        : Self(self) {
    }

    void PersistSubDomainPathId(ui64 localPathId, NTabletFlatExecutor::TTransactionContext &txc);
    void StopWatchingSubDomainPathId();
    void StartWatchingSubDomainPathId();
    void StartFindSubDomainPathId(bool delayFirstRequest = true);

    void Bootstrap(const TActorContext& /*ctx*/) {
        Become(&TThis::StateWork);
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext&);
    void Handle(NActors::TEvents::TEvPoison::TPtr& ev, const TActorContext&);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound, Handle);
            HFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                    LOG_S_WARN("TSpaceWatcher.StateWork at " << " unhandled event type: " << ev->GetTypeName()
                                                             << " event: " << ev->ToString());
                break;
        }
    }
};

}
