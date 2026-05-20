#pragma once

#include "appdata_fwd.h"
#include <ydb/library/aclib/aclib.h>

namespace NKikimr {

// Check token against given list of allowed sids
bool IsTokenAllowed(const NACLib::TUserToken* userToken, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const NACLib::TUserToken* userToken, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);
bool IsTokenAllowed(const TString& userTokenSerialized, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const TString& userTokenSerialized, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);

// Check token against AdministrationAllowedSIDs
bool IsAdministrator(const TAppData* appData, const TString& userTokenSerialized);
bool IsAdministrator(const TAppData* appData, const NACLib::TUserToken* userToken);

template <class TSecurityConfig>
bool IsStrictDatabaseOnlyToken(const NACLib::TUserToken* userToken, const TSecurityConfig& securityConfig) {
    return IsTokenAllowed(userToken, securityConfig.GetDatabaseAllowedSIDs())
        && !IsTokenAllowed(userToken, securityConfig.GetViewerAllowedSIDs())
        && !IsTokenAllowed(userToken, securityConfig.GetMonitoringAllowedSIDs())
        && !IsTokenAllowed(userToken, securityConfig.GetAdministrationAllowedSIDs());
}

template <class TSecurityConfig>
bool IsStrictDatabaseOnlyToken(const TString& userTokenSerialized, const TSecurityConfig& securityConfig) {
    return IsTokenAllowed(userTokenSerialized, securityConfig.GetDatabaseAllowedSIDs())
        && !IsTokenAllowed(userTokenSerialized, securityConfig.GetViewerAllowedSIDs())
        && !IsTokenAllowed(userTokenSerialized, securityConfig.GetMonitoringAllowedSIDs())
        && !IsTokenAllowed(userTokenSerialized, securityConfig.GetAdministrationAllowedSIDs());
}

// Check token against database owner
bool IsDatabaseAdministrator(const NACLib::TUserToken* userToken, const NACLib::TSID& databaseOwner);

} // namespace NKikimr
