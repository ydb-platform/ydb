#include "access_behaviour.h"
#include "checker_access.h"
#include "initializer.h"
#include "manager.h"

namespace NKikimr::NMetadata::NSecret {

TAccessBehaviour::TFactory::TRegistrator<TAccessBehaviour> TAccessBehaviour::Registrator(TAccess::GetTypeId());

TString TAccessBehaviour::GetInternalStorageTablePath() const {
    return "secrets/access";
}

NModifications::IOperationsManager::TPtr TAccessBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TAccessManager>();
}

NInitializer::IInitializationBehaviour::TPtr TAccessBehaviour::ConstructInitializer() const {
    return std::make_shared<TAccessInitializer>();
}

TString TAccessBehaviour::GetTypeId() const {
    return TAccess::GetTypeId();
}

IClassBehaviour::TPtr TAccessBehaviour::GetInstance() {
    static std::shared_ptr<TAccessBehaviour> result = std::make_shared<TAccessBehaviour>();
    return result;
}

}
