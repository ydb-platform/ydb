#include "behaviour.h"
#include "manager.h"

#include <ydb/services/metadata/abstract/initialization.h>


namespace NKikimr::NKqp {

TResourcePoolBehaviour::TFactory::TRegistrator<TResourcePoolBehaviour> TResourcePoolBehaviour::Registrator(TResourcePoolConfig::GetTypeId());

NMetadata::NInitializer::IInitializationBehaviour::TPtr TResourcePoolBehaviour::ConstructInitializer() const {
    return nullptr;
}

NMetadata::NModifications::IOperationsManager::TPtr TResourcePoolBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TResourcePoolManager>();
}

TString TResourcePoolBehaviour::GetInternalStorageTablePath() const {
    return TResourcePoolConfig::GetTypeId();
}


TString TResourcePoolBehaviour::GetTypeId() const {
    return TResourcePoolConfig::GetTypeId();
}

}  // namespace NKikimr::NKqp
