#include "upload_rows_common_impl.h"

namespace NKikimr::NTxProxy {

bool CheckAccess(const TString& table, const TString& token, const NSchemeCache::TSchemeCacheNavigate* resolveResult, TString& errorMessage) {
    if (token.empty())
        return true;

    NACLib::TUserToken userToken(token);
    const ui32 access = NACLib::EAccessRights::UpdateRow;
    if (!resolveResult) {
        TStringStream explanation;
        explanation << "Access denied for " << userToken.GetUserSID()
                    << " table '" << table
                    << "' has not been resolved yet";

        errorMessage = explanation.Str();
        return false;
    }
    for (const NSchemeCache::TSchemeCacheNavigate::TEntry& entry : resolveResult->ResultSet) {
        if (entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            && entry.SecurityObject != nullptr
            && !entry.SecurityObject->CheckAccess(access, userToken))
        {
            TStringStream explanation;
            explanation << "Access denied for " << userToken.GetUserSID()
                        << " with access " << NACLib::AccessRightsToString(access)
                        << " to table '" << table << "'";

            errorMessage = explanation.Str();
            return false;
        }
    }
    return true;
}

}
