#include "external_data.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/tiering/tier/manager.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

void TSnapshotConstructor::EnrichSnapshotData(ISnapshot::TPtr original, NMetadata::NFetcher::ISnapshotAcceptorController::TPtr controller) const {
    controller->OnSnapshotEnriched(original);
}

TSnapshotConstructor::TSnapshotConstructor() {
}

std::vector<NMetadata::IClassBehaviour::TPtr> TSnapshotConstructor::DoGetManagers() const {
    return { TTierConfig::GetBehaviour() };
}

}
