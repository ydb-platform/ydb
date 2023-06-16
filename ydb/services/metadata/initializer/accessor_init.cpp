#include "accessor_init.h"
#include "controller.h"
#include "manager.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NInitializer {

void TDSAccessorInitialized::Bootstrap() {
    Become(&TDSAccessorInitialized::StateMain);
    InternalController = std::make_shared<TInitializerInput>(SelfId());
    Send(SelfId(), new TEvInitializerPreparationStart);
}

void TDSAccessorInitialized::Handle(TEvInitializerPreparationStart::TPtr& /*ev*/) {
    InitializationBehaviour->Prepare(InternalController);
}

class TModifierController: public NMetadata::NInitializer::IModifierExternalController {
private:
    const TActorIdentity OwnerId;
public:
    TModifierController(const TActorIdentity& ownerId)
        : OwnerId(ownerId)
    {

    }
    virtual void OnModificationFinished(const TString& /*modificationId*/) override {
        OwnerId.Send(OwnerId, new NModifications::TEvModificationFinished());
    }
    virtual void OnModificationFailed(const TString& errorMessage, const TString& /*modificationId*/) override {
        OwnerId.Send(OwnerId, new NModifications::TEvModificationProblem(errorMessage));
    }
};

void TDSAccessorInitialized::Handle(TEvInitializerPreparationFinished::TPtr& ev) {
    auto modifiers = ev->Get()->GetModifiers();
    for (auto&& i : modifiers) {
        TDBInitializationKey key(ComponentId, i->GetModificationId());
        if (InitializationSnapshot && InitializationSnapshot->GetObjects().contains(key)) {
            continue;
        }
        Modifiers.emplace_back(i);
    }
    if (Modifiers.size()) {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
        Modifiers.front()->Execute(std::make_shared<TModifierController>(SelfId()), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "initialization finished";
        ExternalController->OnInitializationFinished(ComponentId);
    }
}

void TDSAccessorInitialized::Handle(TEvInitializerPreparationProblem::TPtr& ev) {
    ALS_ERROR(NKikimrServices::METADATA_INITIALIZER) << "preparation problems: " << ev->Get()->GetErrorMessage();
    Schedule(TDuration::Seconds(1), new TEvInitializerPreparationStart);
}

void TDSAccessorInitialized::DoNextModifier() {
    Modifiers.pop_front();
    if (Modifiers.size()) {
        Modifiers.front()->Execute(std::make_shared<TModifierController>(SelfId()), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "initialization finished";
        ExternalController->OnInitializationFinished(ComponentId);
    }
}

void TDSAccessorInitialized::Handle(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    Y_VERIFY(Modifiers.size());
    Modifiers.front()->Execute(std::make_shared<TModifierController>(SelfId()), Config);
}

void TDSAccessorInitialized::Handle(TEvAlterFinished::TPtr& /*ev*/) {
    DoNextModifier();
}

void TDSAccessorInitialized::Handle(TEvAlterProblem::TPtr& ev) {
    AFL_ERROR(NKikimrServices::METADATA_INITIALIZER)("event", "alter_problem")("message", ev->Get()->GetErrorMessage());
    Schedule(TDuration::Seconds(1), new NModifications::TEvModificationFinished);
}

void TDSAccessorInitialized::Handle(NModifications::TEvModificationFinished::TPtr& /*ev*/) {
    ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
    Y_VERIFY(Modifiers.size());
    if (NProvider::TServiceOperator::IsEnabled() && InitializationSnapshot) {
        TDBInitialization dbInit(ComponentId, Modifiers.front()->GetModificationId());
        NModifications::IOperationsManager::TExternalModificationContext extContext;
        extContext.SetUserToken(NACLib::TSystemUsers::Metadata());
        auto alterCommand = std::make_shared<NModifications::TCreateCommand<TDBInitialization>>(
            dbInit.SerializeToRecord(), TDBInitialization::GetBehaviour(), InternalController,
            NModifications::IOperationsManager::TInternalModificationContext(extContext));
        Sender<NProvider::TEvObjectsOperation>(alterCommand)
            .SendTo(NProvider::MakeServiceId(SelfId().NodeId()));
    } else {
        DoNextModifier();
    }
}

void TDSAccessorInitialized::Handle(NModifications::TEvModificationProblem::TPtr& ev) {
    AFL_ERROR(NKikimrServices::METADATA_INITIALIZER)("event", "modification_problem")("message", ev->Get()->GetErrorMessage());
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup);
}

TDSAccessorInitialized::TDSAccessorInitialized(const NRequest::TConfig& config,
    const TString& componentId,
    IInitializationBehaviour::TPtr initializationBehaviour,
    IInitializerOutput::TPtr controller, std::shared_ptr<TSnapshot> initializationSnapshot)
    : Config(config)
    , InitializationBehaviour(initializationBehaviour)
    , ExternalController(controller)
    , InitializationSnapshot(initializationSnapshot)
    , ComponentId(componentId)
{
}

}
