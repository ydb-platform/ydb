#include "behaviour.h"
#include "manager.h"

#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NColumnShard::NTiers {

TTieringRuleBehaviour::TFactory::TRegistrator<TTieringRuleBehaviour> TTieringRuleBehaviour::Registrator(TTieringRule::GetTypeId());

TString TTieringRuleBehaviour::GetInternalStorageTablePath() const {
    return "tiering/rules";
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TTieringRuleBehaviour::ConstructInitializer() const {
    return nullptr;
}

NMetadata::NModifications::IOperationsManager::TPtr TTieringRuleBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TTieringRulesManager>();
}

TString TTieringRuleBehaviour::GetTypeId() const {
    return TTieringRule::GetTypeId();
}

}
