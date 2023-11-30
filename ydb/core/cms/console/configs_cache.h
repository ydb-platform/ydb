#pragma once

#include "defs.h"
#include "console.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NConsole {

using TSaveCallback = std::function<void(const NKikimrConfig::TAppConfig&)>;
using TLoadCallback = std::function<void(NKikimrConfig::TAppConfig&)>;

class TConfigsCache : public TActorBootstrapped<TConfigsCache> {

using TBase = TActorBootstrapped<TConfigsCache>;

public:
    TConfigsCache(TSaveCallback save, TLoadCallback load)
        : Save(std::move(save))
        , Load(std::move(load)) {}

    void Bootstrap(const TActorContext &ctx);

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::CONFIGS_CACHE);
        switch (ev->GetTypeRewrite()) {
            hFuncTraced(TEvConsole::TEvConfigSubscriptionNotification, Handle);
            HFuncTraced(TEvConsole::TEvConfigSubscriptionError, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            default:
                Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Handle(TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev);

    void Handle(TEvConsole::TEvConfigSubscriptionError::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx);

protected:
    void Die(const TActorContext &ctx) override;

private:
    const TSaveCallback Save;
    const TLoadCallback Load;

    TActorId SubscriptionClient;

    NKikimrConfig::TAppConfig CurrentConfig;

    ::NMonitoring::TDynamicCounters::TCounterPtr OutdatedConfiguration;
};

IActor *CreateConfigsCacheActor(const TString &pathToConfigCacheFile);

inline TActorId MakeConfigsCacheActorID(ui32 node = 0) {
    return TActorId(node, "configscache");
}
}
