#include "behaviour.h"
#include "initializer.h"
#include "checker.h"
#include "manager.h"

namespace NKikimr::NColumnShard::NTiers {

TTieringRuleBehaviour::TFactory::TRegistrator<TTieringRuleBehaviour> TTieringRuleBehaviour::Registrator(TTieringRule::GetTypeId());

TString TTieringRuleBehaviour::GetInternalStorageTablePath() const {
    return "tiering/rules";
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TTieringRuleBehaviour::ConstructInitializer() const {
    return std::make_shared<TTierRulesInitializer>();
}

NMetadata::NModifications::IOperationsManager::TPtr TTieringRuleBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TTieringRulesManager>();
}

TString TTieringRuleBehaviour::GetTypeId() const {
    return TTieringRule::GetTypeId();
}

}
