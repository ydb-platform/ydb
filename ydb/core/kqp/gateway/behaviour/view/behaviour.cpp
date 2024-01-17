#include "behaviour.h"
#include "manager.h"

namespace NKikimr::NKqp {

TViewBehaviour::TFactory::TRegistrator<TViewBehaviour> TViewBehaviour::Registrator(TViewConfig::GetTypeId());

NMetadata::NInitializer::IInitializationBehaviour::TPtr TViewBehaviour::ConstructInitializer() const {
    return nullptr;
}

NMetadata::NModifications::IOperationsManager::TPtr TViewBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TViewManager>();
}

TString TViewBehaviour::GetInternalStorageTablePath() const {
    return TViewConfig::GetTypeId();
}

TString TViewBehaviour::GetTypeId() const {
    return TViewConfig::GetTypeId();
}

}
