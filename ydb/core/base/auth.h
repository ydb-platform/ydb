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

bool IsStrictDatabaseOnlyToken(const TAppData* appData, const TString& userTokenSerialized);

// Check token against database owner
bool IsDatabaseAdministrator(const NACLib::TUserToken* userToken, const NACLib::TSID& databaseOwner);

// When the AlwaysSetSystemOwner setting is enabled, forces the owner of a scheme
// modification record to the system basic owner, unless the record is already owned
// by the system metadata user (objects created by the system itself keep their owner).
void SetSystemOwnerIfNeeded(NKikimrScheme::TEvModifySchemeTransaction& record, const TAppData* appData);

} // namespace NKikimr
