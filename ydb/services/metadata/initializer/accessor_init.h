#pragma once
#include "common.h"

#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadataProvider {

class TDSAccessorInitialized: public NActors::TActorBootstrapped<TDSAccessorInitialized> {
private:
    TDeque<ITableModifier::TPtr> Modifiers;
protected:
    const TConfig& Config;
    virtual void RegisterState() = 0;
    virtual void OnInitialized() = 0;
public:
    void Bootstrap();
    TDSAccessorInitialized(const TConfig& config, const TVector<ITableModifier::TPtr>& modifiers);
    void Handle(NInternal::NRequest::TEvRequestFinished::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestFinished, Handle);
            default:
                break;
        }
    }
};

}
