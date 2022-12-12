#include "behaviour.h"
#include "manager.h"
#include "initializer.h"

#include <ydb/core/tx/tiering/tier/checker.h>

#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NColumnShard::NTiers {

TTierConfigBehaviour::TFactory::TRegistrator<TTierConfigBehaviour> TTierConfigBehaviour::Registrator(TTierConfig::GetTypeId());

TString TTierConfigBehaviour::GetInternalStorageTablePath() const {
    return "tiering/tiers";
}

NMetadata::NModifications::IOperationsManager::TPtr TTierConfigBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TTiersManager>();
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TTierConfigBehaviour::ConstructInitializer() const {
    return std::make_shared<TTiersInitializer>();
}

TString TTierConfigBehaviour::GetTypeId() const {
    return TTierConfig::GetTypeId();
}

}
