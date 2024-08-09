#include "behaviour.h"
#include "initializer.h"
#include "manager.h"


namespace NKikimr::NKqp {

TResourcePoolClassifierBehaviour::TFactory::TRegistrator<TResourcePoolClassifierBehaviour> TResourcePoolClassifierBehaviour::Registrator(TResourcePoolClassifierConfig::GetTypeId());

NMetadata::NInitializer::IInitializationBehaviour::TPtr TResourcePoolClassifierBehaviour::ConstructInitializer() const {
    return std::make_shared<TResourcePoolClassifierInitializer>();
}

NMetadata::NModifications::IOperationsManager::TPtr TResourcePoolClassifierBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TResourcePoolClassifierManager>();
}

TString TResourcePoolClassifierBehaviour::GetInternalStorageTablePath() const {
    return "resource_pools/resource_pools_classifiers";
}

TString TResourcePoolClassifierBehaviour::GetTypeId() const {
    return TResourcePoolClassifierConfig::GetTypeId();
}

NMetadata::IClassBehaviour::TPtr TResourcePoolClassifierBehaviour::GetInstance() {
    static std::shared_ptr<TResourcePoolClassifierBehaviour> result = std::make_shared<TResourcePoolClassifierBehaviour>();
    return result;
}

}  // namespace NKikimr::NKqp
