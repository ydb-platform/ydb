#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <util/datetime/base.h>

namespace NMVP {
namespace NOIDC {

class TContextStorage;

class TContextStorageRefresher : public NActors::TActorBootstrapped<TContextStorageRefresher> {
private:
    using TBase = NActors::TActor<TContextStorageRefresher>;

    TContextStorage* const ContextStorage;
    static constexpr TDuration PERIODIC_CHECK = TDuration::Seconds(30);

public:
    TContextStorageRefresher(TContextStorage* const contextStorage);
    void Bootstrap();
    void HandleRefresh();

    static TContextStorageRefresher* CreateRestoreContextRefresher(TContextStorage* const contextStorage);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleRefresh);
        }
    }
};

} // NOIDC
} // NMVP
