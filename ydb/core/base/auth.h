#pragma once

#include "appdata_fwd.h"
#include <ydb/library/aclib/aclib.h>

namespace NKikimrScheme {
    class TEvModifySchemeTransaction;
}

namespace NKikimr {

// Check token against given list of allowed sids
bool IsTokenAllowed(const NACLib::TUserToken* userToken, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const NACLib::TUserToken* userToken, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);
bool IsTokenAllowed(const TString& userTokenSerialized, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const TString& userTokenSerialized, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);

// Check token against AdministrationAllowedSIDs
bool IsAdministrator(const TAppData* appData, const TString& userTokenSerialized);
bool IsAdministrator(const TAppData* appData, const NACLib::TUserToken* userToken);

// Check token against database owner
bool IsDatabaseAdministrator(const NACLib::TUserToken* userToken, const NACLib::TSID& databaseOwner);

// Computes the owner that should be set for a scheme modification record.
// If neither the AlwaysSetSystemOwner setting nor the EnableIdmPermissionsManagement
// feature flag is enabled, the owner is the acting user (from userToken, if given)
// or, if no user token is given, the owner already present in the record.
// Otherwise the owner is forced to the system basic owner,
// unless objects created by the system itself.
TString ChooseAppropriateOwner(const NKikimrScheme::TEvModifySchemeTransaction& record,
    const TAppData* appData, const std::optional<NACLib::TUserToken>& userToken = std::nullopt);

enum class EAccessLevel {
    None /* "none" */,
    Database /* "database" */,
    Viewer /* "viewer" */,
    Monitoring /* "monitoring" */,
    Administration /* "administration" */,
};

// EAccessLevel::None means that no access level was matched for the given token and security config.
// It is not the same as an anonymous request: a missing token may still resolve to any level
// when the corresponding allowed_sids list is empty.
EAccessLevel GetHighestAccessLevel(const TAppData* appData, const NACLib::TUserToken* userToken);
EAccessLevel GetHighestAccessLevel(const TAppData* appData, const TString& userTokenSerialized);

bool IsStrictDatabaseOnlyToken(const TAppData* appData, const TString& userTokenSerialized);

} // namespace NKikimr
