#pragma once
#include "config.h"

#include <ydb/services/metadata/service.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NMetadataProvider {

class TService: public NActors::TActorBootstrapped<TService> {
private:
    using TBase = NActors::TActor<TService>;
    std::map<TString, NActors::TActorId> Accessors;
    const TConfig Config;
public:
    void Handle(TEvSubscribeExternal::TPtr& ev);
    void Handle(TEvUnsubscribeExternal::TPtr& ev);

    void Bootstrap(const NActors::TActorContext& /*ctx*/) {
        Become(&TThis::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSubscribeExternal, Handle);
            hFunc(TEvUnsubscribeExternal, Handle);
            default:
                Y_VERIFY(false);
        }
    }

    TService(const TConfig& config)
        : Config(config)
    {
    }
};

NActors::IActor* CreateService(const TConfig& config);

}
