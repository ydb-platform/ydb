#pragma once
#include "accessor_snapshot_simple.h"
#include "registration.h"

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/services/metadata/request/config.h>

namespace NKikimr::NMetadata::NProvider {

class TEvStartMetadataService: public TEventLocal<TEvStartMetadataService, EEvents::EvStartMetadataService> {
};

class TInitializerSnapshotReaderController: public TDSAccessorSimple::TEvController {
private:
    const NActors::TActorIdentity ActorId;
public:
    TInitializerSnapshotReaderController(const NActors::TActorIdentity& actorId)
        : TDSAccessorSimple::TEvController(actorId)
        , ActorId(actorId) {

    }
};

class TInitializerSnapshotReader: public NActors::TActorBootstrapped<TInitializerSnapshotReader> {
private:
    std::shared_ptr<TRegistrationData> RegistrationData;
    const NRequest::TConfig ReqConfig;
    std::shared_ptr<TInitializerSnapshotReaderController> InternalController;

    void Handle(TDSAccessorSimple::TEvController::TEvResult::TPtr& ev);
    void Handle(TEvStartMetadataService::TPtr& ev);
    void Handle(TDSAccessorSimple::TEvController::TEvError::TPtr& ev);
    void Handle(TDSAccessorSimple::TEvController::TEvTableAbsent::TPtr& ev);

public:
    TInitializerSnapshotReader(std::shared_ptr<TRegistrationData> registrationData, const NRequest::TConfig& reqConfig)
        : RegistrationData(registrationData)
        , ReqConfig(reqConfig) {

    }

    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TDSAccessorSimple::TEvController::TEvResult, Handle);
            hFunc(TDSAccessorSimple::TEvController::TEvError, Handle);
            hFunc(TDSAccessorSimple::TEvController::TEvTableAbsent, Handle);

            hFunc(TEvStartMetadataService, Handle);
            default:
                Y_VERIFY(false);
        }
    }

};

}
