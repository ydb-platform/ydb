#include "secret_behaviour.h"
#include "checker_secret.h"
#include "manager.h"

namespace NKikimr::NMetadata::NSecret {

TSecretBehaviour::TFactory::TRegistrator<TSecretBehaviour> TSecretBehaviour::Registrator(TSecret::GetTypeId());

TString TSecretBehaviour::GetInternalStorageTablePath() const {
    return "secrets/values";
}

NInitializer::IInitializationBehaviour::TPtr TSecretBehaviour::ConstructInitializer() const {
    return std::make_shared<TSecretInitializer>();
}

NModifications::IOperationsManager::TPtr TSecretBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TSecretManager>();
}

TString TSecretBehaviour::GetTypeId() const {
    return TSecret::GetTypeId();
}

IClassBehaviour::TPtr TSecretBehaviour::GetInstance() {
    static std::shared_ptr<TSecretBehaviour> result = std::make_shared<TSecretBehaviour>();
    return result;
}

}
