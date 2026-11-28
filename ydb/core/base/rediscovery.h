#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

inline constexpr char RediscoveryAttribute[] = "__ydb_rediscovery";

inline bool IsTriggerRediscovery(const NKikimrSchemeOp::TAlterUserAttributes& patch) {
    if (patch.UserAttributesSize() != 1) {
        return false;
    }
    const auto& attr = patch.GetUserAttributes(0);
    return attr.GetKey() == RediscoveryAttribute && attr.HasValue() && attr.GetValue().empty();
}

inline bool CanTriggerRediscovery(const TAppData* appData, const NACLib::TUserToken* token, const TString& owner) {
    return token && !token->GetUserSID().empty() && (IsDatabaseAdministrator(token, owner) ||
        (!appData->AdministrationAllowedSIDs.empty() && IsAdministrator(appData, token)));
}

} // namespace NKikimr
