#pragma once

#include "appdata_fwd.h"
#include <ydb/library/aclib/aclib.h>

namespace NKikimr {

// Check token against given list of allowed sids
bool IsTokenAllowed(const NACLib::TUserToken* userToken, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const NACLib::TUserToken* userToken, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);
bool IsTokenAllowed(const TString& userTokenSerialized, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const TString& userTokenSerialized, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);

// Check token against given list of allowed sids considering security settings
bool IsTokenAllowed(const TAppData* appData, const NACLib::TUserToken* userToken, const TVector<TString>& allowedSIDs);
bool IsTokenAllowed(const TAppData* appData, const NACLib::TUserToken* userToken, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs);

// Check token against AdministrationAllowedSIDs
bool IsAdministrator(const TAppData* appData, const TString& userTokenSerialized);
bool IsAdministrator(const TAppData* appData, const NACLib::TUserToken* userToken);

// Check token against database owner
bool IsDatabaseAdministrator(const NACLib::TUserToken* userToken, const NACLib::TSID& databaseOwner);

}
