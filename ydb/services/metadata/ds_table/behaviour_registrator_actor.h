#pragma once
#include "scheme_describe.h"
#include "registration.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/initializer/common.h>
#include <ydb/services/metadata/initializer/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NMetadata::NProvider {

class TEvStartRegistration: public TEventLocal<TEvStartRegistration, EEvents::EvStartRegistration> {
};

class TBehaviourRegistrator: public NActors::TActorBootstrapped<TBehaviourRegistrator> {
private:
    using TBase = NActors::TActorBootstrapped<TBehaviourRegistrator>;
    class TInternalController;

    IClassBehaviour::TPtr Behaviour;
    std::shared_ptr<TRegistrationData> RegistrationData;
    const NRequest::TConfig ReqConfig;
    std::shared_ptr<TInternalController> InternalController;

    void Handle(TEvTableDescriptionSuccess::TPtr& ev);
    void Handle(TEvTableDescriptionFailed::TPtr& ev);
    void Handle(TEvStartRegistration::TPtr& ev);
    void Handle(NInitializer::TEvInitializationFinished::TPtr& ev);
public:
    TBehaviourRegistrator(IClassBehaviour::TPtr b, std::shared_ptr<TRegistrationData> registrationData, const NRequest::TConfig& reqConfig)
        : Behaviour(b)
        , RegistrationData(registrationData)
        , ReqConfig(reqConfig) {

    }

    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {

            hFunc(TEvTableDescriptionSuccess, Handle);
            hFunc(TEvTableDescriptionFailed, Handle);

            hFunc(NInitializer::TEvInitializationFinished, Handle);
            hFunc(TEvStartRegistration, Handle);
            default:
                Y_ABORT_UNLESS(false);
        }
    }
};

}
