#include "ss_fetcher.h"
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr::NColumnShard::NTiers {

TFetcherCheckUserTieringPermissions::TFactory::TRegistrator<TFetcherCheckUserTieringPermissions>
    TFetcherCheckUserTieringPermissions::Registrator(TFetcherCheckUserTieringPermissions::GetTypeIdStatic());

void TFetcherCheckUserTieringPermissions::DoProcess(NSchemeShard::TSchemeShard& schemeShard, NKikimrScheme::TEvProcessingResponse& result) const {
    TResult content;
    content.MutableContent().SetOperationAllow(true);
    ui32 access = 0;
    access |= NACLib::EAccessRights::AlterSchema;

    if (ActivityType == NMetadata::NModifications::IOperationsManager::EActivityType::Undefined) {
        content.Deny("undefined activity type");
    } else {
        bool denied = false;
        for (auto&& i : TieringRuleIds) {
            const auto& pathIds = schemeShard.ColumnTables.GetTablesWithTiering(i);
            for (auto&& pathId : pathIds) {
                auto path = NSchemeShard::TPath::Init(pathId, &schemeShard);
                if (!path.IsResolved() || path.IsUnderDeleting() || path.IsDeleted()) {
                    continue;
                }
                if (ActivityType == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
                    denied = true;
                    content.Deny("tiering in using by table");
                    break;
                } else if (ActivityType == NMetadata::NModifications::IOperationsManager::EActivityType::Alter) {
                    if (!UserToken) {
                        continue;
                    }
                    TSecurityObject sObject(path->Owner, path->ACL, path->IsContainer());
                    if (!sObject.CheckAccess(access, *UserToken)) {
                        denied = true;
                        content.Deny("no alter permissions for affected table");
                        break;
                    }
                }
            }
            if (denied) {
                break;
            }
        }
    }
    result.MutableContent()->SetData(content.SerializeToString());
}

bool TFetcherCheckUserTieringPermissions::DoDeserializeFromProto(const TProtoClass& protoData) {
    if (!TryFromString(protoData.GetActivityType(), ActivityType)) {
        ALS_ERROR(0) << "Cannot parse activity type: undefined value = " << protoData.GetActivityType();
        return false;
    }
    if (protoData.GetUserToken()) {
        NACLib::TUserToken uToken(protoData.GetUserToken());
        UserToken = uToken;
    }
    for (auto&& i : protoData.GetTieringRuleIds()) {
        TieringRuleIds.emplace(i);
    }
    return true;
}

NKikimr::NColumnShard::NTiers::TFetcherCheckUserTieringPermissions::TProtoClass TFetcherCheckUserTieringPermissions::DoSerializeToProto() const {
    TProtoClass result;
    result.SetActivityType(::ToString(ActivityType));
    if (UserToken) {
        result.SetUserToken(UserToken->SerializeAsString());
    }
    for (auto&& i : TieringRuleIds) {
        *result.AddTieringRuleIds() = i;
    }
    return result;
}

}
