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
        Modifiers.front()->Execute(SelfId(), Config);
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
        Modifiers.front()->Execute(SelfId(), Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "initialization finished";
        ExternalController->OnInitializationFinished(ComponentId);
    }
}

void TDSAccessorInitialized::Handle(NRequest::TEvRequestFinished::TPtr& /*ev*/) {
    ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
    Y_VERIFY(Modifiers.size());
    if (NProvider::TServiceOperator::IsEnabled() && InitializationSnapshot) {
        TDBInitialization dbInit(ComponentId, Modifiers.front()->GetModificationId());
        auto alterCommand = std::make_shared<NModifications::TCreateCommand<TDBInitialization>>(
            dbInit.SerializeToRecord(), TDBInitialization::GetBehaviour(), InternalController, NModifications::IOperationsManager::TModificationContext());
        Sender<NProvider::TEvObjectsOperation>(alterCommand)
            .SendTo(NProvider::MakeServiceId(SelfId().NodeId()));
    } else {
        DoNextModifier();
    }
}

void TDSAccessorInitialized::Handle(NModifications::TEvModificationFinished::TPtr& /*ev*/) {
    DoNextModifier();
}

void TDSAccessorInitialized::Handle(NModifications::TEvModificationProblem::TPtr& /*ev*/) {
    Schedule(TDuration::Seconds(1), new NRequest::TEvRequestFinished);
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
