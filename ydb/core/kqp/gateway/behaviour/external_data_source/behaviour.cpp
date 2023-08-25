#include "behaviour.h"
#include "manager.h"

namespace NKikimr::NKqp {

TExternalDataSourceBehaviour::TFactory::TRegistrator<TExternalDataSourceBehaviour> TExternalDataSourceBehaviour::Registrator(TExternalDataSourceConfig::GetTypeId());

NMetadata::NModifications::IOperationsManager::TPtr TExternalDataSourceBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TExternalDataSourceManager>();
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TExternalDataSourceBehaviour::ConstructInitializer() const {
    return nullptr;
}

TString TExternalDataSourceBehaviour::GetInternalStorageTablePath() const {
    return TExternalDataSourceConfig::GetTypeId();
}


TString TExternalDataSourceBehaviour::GetTypeId() const {
    return TExternalDataSourceConfig::GetTypeId();
}

}
