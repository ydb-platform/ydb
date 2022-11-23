#include "manager.h"
#include "initializer.h"
namespace NKikimr::NMetadata::NSecret {

TSecretManager::TFactory::TRegistrator<TSecretManager> TSecretManager::Registrator(TSecretManager::GetTypeIdStatic());
TAccessManager::TFactory::TRegistrator<TAccessManager> TAccessManager::Registrator(TAccessManager::GetTypeIdStatic());

IInitializationBehaviour::TPtr TAccessManager::DoGetInitializationBehaviour() const {
    return std::make_shared<TAccessInitializer>();
}

IInitializationBehaviour::TPtr TSecretManager::DoGetInitializationBehaviour() const {
    return std::make_shared<TSecretInitializer>();
}

}
