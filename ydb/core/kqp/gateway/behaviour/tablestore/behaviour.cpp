#include "behaviour.h"
#include "manager.h"
#include "initializer.h"

namespace NKikimr::NKqp {

TTableStoreBehaviour::TFactory::TRegistrator<TTableStoreBehaviour> TTableStoreBehaviour::Registrator(TTableStoreConfig::GetTypeId());

NMetadata::NModifications::IOperationsManager::TPtr TTableStoreBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TTableStoreManager>();
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TTableStoreBehaviour::ConstructInitializer() const {
    return std::make_shared<TTableStoreInitializer>();
}

TString TTableStoreBehaviour::GetInternalStorageTablePath() const {
    return TTableStoreConfig::GetTypeId();
}


TString TTableStoreBehaviour::GetTypeId() const {
    return TTableStoreConfig::GetTypeId();
}

}
