#include "accessor_init.h"
#include "controller.h"
#include "manager.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadataInitializer {

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
        ExternalController->InitializationFinished(ComponentId);
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
        ExternalController->InitializationFinished(ComponentId);
    }
}

void TDSAccessorInitialized::Handle(NInternal::NRequest::TEvRequestFinished::TPtr& /*ev*/) {
    ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
    Y_VERIFY(Modifiers.size());
    if (NMetadataProvider::TServiceOperator::IsEnabled() && InitializationSnapshot) {
        TDBInitialization dbInit(ComponentId, Modifiers.front()->GetModificationId());
        auto manager = std::make_shared<NMetadataInitializer::TManager>();
        auto alterCommand = std::make_shared<NMetadataManager::TAlterCommand<TDBInitialization>>(
            dbInit.SerializeToRecord(), manager, InternalController, NMetadata::IOperationsManager::TModificationContext());
        Sender<NMetadataProvider::TEvAlterObjects>(alterCommand)
            .SendTo(NMetadataProvider::MakeServiceId(SelfId().NodeId()));
    } else {
        DoNextModifier();
    }
}

void TDSAccessorInitialized::Handle(NMetadataManager::TEvModificationFinished::TPtr& /*ev*/) {
    DoNextModifier();
}

void TDSAccessorInitialized::Handle(NMetadataManager::TEvModificationProblem::TPtr& /*ev*/) {
    Schedule(TDuration::Seconds(1), new NInternal::NRequest::TEvRequestFinished);
}

TDSAccessorInitialized::TDSAccessorInitialized(const NInternal::NRequest::TConfig& config,
    const TString& componentId,
    NMetadata::IInitializationBehaviour::TPtr initializationBehaviour,
    IInitializerOutput::TPtr controller, std::shared_ptr<NMetadataInitializer::TSnapshot> initializationSnapshot)
    : Config(config)
    , InitializationBehaviour(initializationBehaviour)
    , ExternalController(controller)
    , InitializationSnapshot(initializationSnapshot)
    , ComponentId(componentId)
{
}

}
