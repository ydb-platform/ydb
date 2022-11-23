#include "manager.h"
#include "initializer.h"

namespace NKikimr::NMetadataInitializer {

TManager::TFactory::TRegistrator<TManager> TManager::Registrator(TManager::GetTypeIdStatic());

NMetadata::IInitializationBehaviour::TPtr TManager::DoGetInitializationBehaviour() const {
    return std::make_shared<TInitializer>();
}

}
