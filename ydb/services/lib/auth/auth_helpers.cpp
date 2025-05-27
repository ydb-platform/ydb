#include "auth_helpers.h"
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NTopicHelpers {

EAuthResult CheckAccess(
        const NKikimr::TAppData& appData,
        const NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry& describeEntry,
        const TString& serializedToken,
        const TString& entityName, TString& error
) {
    if (serializedToken.empty()) {
        if (appData.EnforceUserTokenRequirement || appData.PQConfig.GetRequireCredentialsInNewProtocol()) {
            error = "Unauthenticated access is forbidden, please provide credentials";
            return EAuthResult::TokenRequired;
        }
    } else {
        NACLib::TUserToken token(serializedToken);
        if (!describeEntry.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow, token)) {
            error = "Access to " + entityName + " is denied for subject '" + token.GetUserSID() + "'";
            return EAuthResult::AccessDenied;
        }
    }
    return EAuthResult::AuthOk;
}

} //namespace
