#include "manager.h"
#include "initializer.h"

namespace NKikimr::NColumnShard::NTiers {

TTiersManager::TFactory::TRegistrator<TTiersManager> TTiersManager::Registrator(TTiersManager::GetTypeIdStatic());

NMetadata::IInitializationBehaviour::TPtr TTiersManager::DoGetInitializationBehaviour() const {
    return std::make_shared<TTiersInitializer>();
}

}
