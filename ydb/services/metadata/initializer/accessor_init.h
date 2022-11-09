#pragma once
#include "common.h"
#include "events.h"

#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadataInitializer {

class TDSAccessorInitialized: public NActors::TActorBootstrapped<TDSAccessorInitialized> {
private:
    TDeque<ITableModifier::TPtr> Modifiers;
    const NInternal::NRequest::TConfig Config;
    IController::TPtr Controller;

    void Handle(TEvInitializerPreparationStart::TPtr& ev);
    void Handle(TEvInitializerPreparationFinished::TPtr& ev);
    void Handle(TEvInitializerPreparationProblem::TPtr& ev);
    void Handle(NInternal::NRequest::TEvRequestFinished::TPtr& ev);
protected:
    virtual void RegisterState() = 0;
    virtual void OnInitialized() = 0;
    virtual void Prepare(IController::TPtr controller) = 0;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::METADATA_INITIALIZER;
    }

    void Bootstrap();
    TDSAccessorInitialized(const NInternal::NRequest::TConfig& config);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestFinished, Handle);
            hFunc(TEvInitializerPreparationStart, Handle);
            hFunc(TEvInitializerPreparationFinished, Handle);
            hFunc(TEvInitializerPreparationProblem, Handle);
            default:
                break;
        }
    }
};

}
