#include "external_data.h"
#include "owner_enrich.h"
#include "snapshot_enrich.h"

#include <ydb/core/base/path.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

void TSnapshotConstructor::EnrichSnapshotData(ISnapshot::TPtr original, NMetadataProvider::ISnapshotAcceptorController::TPtr controller) const
{
    if (!TablesDecoder) {
        TablesDecoder = std::make_shared<TTablesDecoderCache>();
    }
    TActivationContext::AsActorContext().Register(new TActorSnapshotEnrich(original, controller, TablesDecoder));
}

TSnapshotConstructor::TSnapshotConstructor(const TString& ownerPath)
    : OwnerPath(ownerPath)
{
    Y_VERIFY(!!OwnerPath);
    const TString ownerPathNormal = TFsPath(OwnerPath).Fix().GetPath();
    const TString tenantPathNormal = TFsPath(AppData()->TenantName).Fix().GetPath();
    Y_VERIFY(ownerPathNormal.StartsWith(tenantPathNormal));
    TablePath = "/" + AppData()->TenantName + "/.configs/tiering/";
    Tables.emplace_back(TablePath + "tiers");
    Tables.emplace_back(TablePath + "rules");
}

void TSnapshotConstructor::DoPrepare(NMetadataInitializer::IController::TPtr controller) const {
    TActivationContext::AsActorContext().Register(new TActorOwnerEnrich(OwnerPath, controller, Tables));
}

}
