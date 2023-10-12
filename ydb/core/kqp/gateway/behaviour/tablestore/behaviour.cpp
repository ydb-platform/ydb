#include "behaviour.h"
#include "manager.h"

namespace NKikimr::NKqp {

TTableStoreBehaviour::TFactory::TRegistrator<TTableStoreBehaviour> TTableStoreBehaviour::Registrator(TTableStoreConfig::GetTypeId());

NMetadata::NModifications::IOperationsManager::TPtr TTableStoreBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TTableStoreManager>(false);
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TTableStoreBehaviour::ConstructInitializer() const {
    return nullptr;
}

TString TTableStoreBehaviour::GetInternalStorageTablePath() const {
    return TTableStoreConfig::GetTypeId();
}


TString TTableStoreBehaviour::GetTypeId() const {
    return TTableStoreConfig::GetTypeId();
}

}
