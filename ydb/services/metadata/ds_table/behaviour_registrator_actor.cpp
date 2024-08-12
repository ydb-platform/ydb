#include "behaviour_registrator_actor.h"

#include "accessor_snapshot_simple.h"
#include <ydb/services/metadata/initializer/accessor_init.h>

namespace NKikimr::NMetadata::NProvider {

class TBehaviourRegistrator::TInternalController: public NInitializer::IInitializerOutput, public ISchemeDescribeController {
private:
    const NActors::TActorIdentity ActorId;
public:
    TInternalController(const NActors::TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void OnInitializationFinished(const TString& id) const override {
        ActorId.Send(ActorId, new NInitializer::TEvInitializationFinished(id));
    }
    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& requestId) override {
        ActorId.Send(ActorId, new TEvTableDescriptionFailed(errorMessage, requestId));
    }
    virtual void OnDescriptionSuccess(TTableInfo&& result, const TString& requestId) override {
        ActorId.Send(ActorId, new TEvTableDescriptionSuccess(std::move(result->Columns), requestId));
    }
};

void TBehaviourRegistrator::Handle(TEvTableDescriptionSuccess::TPtr& ev) {
    const TString& initId = Behaviour->GetTypeId();
    Y_ABORT_UNLESS(initId == ev->Get()->GetRequestId());
    auto it = RegistrationData->InRegistration.find(initId);
    Y_ABORT_UNLESS(it != RegistrationData->InRegistration.end());
    it->second->GetOperationsManager()->SetActualSchema(ev->Get()->GetSchema());
    RegistrationData->InitializationFinished(initId);
}

void TBehaviourRegistrator::Handle(TEvTableDescriptionFailed::TPtr& ev) {
    const TString& initId = Behaviour->GetTypeId();
    Y_ABORT_UNLESS(initId == ev->Get()->GetRequestId());
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "metadata service cannot receive table description for " << initId << Endl;
    Schedule(TDuration::Seconds(1), new TEvStartRegistration());
}

void TBehaviourRegistrator::Handle(TEvStartRegistration::TPtr& /*ev*/) {
    NInitializer::TDSAccessorInitialized::Execute(ReqConfig,
        Behaviour->GetTypeId(), Behaviour->GetInitializer(), InternalController, RegistrationData->GetSnapshotOwner());
}

void TBehaviourRegistrator::Handle(NInitializer::TEvInitializationFinished::TPtr& ev) {
    const TString& initId = Behaviour->GetTypeId();
    Y_ABORT_UNLESS(initId == ev->Get()->GetInitializationId());

    auto it = RegistrationData->InRegistration.find(initId);
    Y_ABORT_UNLESS(it != RegistrationData->InRegistration.end());
    if (it->second->GetOperationsManager()) {
        Register(new TSchemeDescriptionActor(InternalController, initId, it->second->GetStorageTablePath()));
    } else {
        RegistrationData->InitializationFinished(initId);
    }
}

void TBehaviourRegistrator::Bootstrap() {
    InternalController = std::make_shared<TInternalController>(SelfId());
    TBase::Become(&TBehaviourRegistrator::StateMain);
    TBase::Sender<TEvStartRegistration>().SendTo(SelfId());
}

}
