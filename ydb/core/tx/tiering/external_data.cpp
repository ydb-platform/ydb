#include "external_data.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/tiering/tier/manager.h>
#include <ydb/core/tx/tiering/rule/manager.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

void TSnapshotConstructor::EnrichSnapshotData(ISnapshot::TPtr original, NMetadataProvider::ISnapshotAcceptorController::TPtr controller) const {
    controller->Enriched(original);
}

TSnapshotConstructor::TSnapshotConstructor() {
}

std::vector<NKikimr::NMetadata::IOperationsManager::TPtr> TSnapshotConstructor::DoGetManagers() const {
    std::vector<NMetadata::IOperationsManager::TPtr> result;
    result.emplace_back(std::make_shared<TTiersManager>());
    result.emplace_back(std::make_shared<TTieringRulesManager>());
    return result;
}

}
