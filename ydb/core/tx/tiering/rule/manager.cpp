#include "manager.h"
#include "initializer.h"

namespace NKikimr::NColumnShard::NTiers {

TTieringRulesManager::TFactory::TRegistrator<TTieringRulesManager> TTieringRulesManager::Registrator(TTieringRulesManager::GetTypeIdStatic());

NMetadata::IInitializationBehaviour::TPtr TTieringRulesManager::DoGetInitializationBehaviour() const {
    return std::make_shared<TTierRulesInitializer>();
}

}
