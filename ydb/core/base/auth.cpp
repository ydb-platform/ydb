#include <ydb/library/aclib/aclib.h>

#include "auth.h"

namespace NKikimr {

namespace {

template <class Iterable>
bool IsTokenAllowedImpl(const NACLib::TUserToken* userToken, const Iterable& allowedSIDs) {
    // empty set contains any element
    if (allowedSIDs.empty()) {
        return true;
    }

    // empty element does not belong to any non-empty set
    if (!userToken || userToken->GetUserSID().empty()) {
        return false;
    }

    // its enough to a single sid (user or group) from the token to be in the set
    for (const auto& sid : allowedSIDs) {
        if (userToken->IsExist(sid)) {
            return true;
        }
    }

    return false;
}

NACLib::TUserToken ParseUserToken(const TString& userTokenSerialized) {
    NACLibProto::TUserToken tokenPb;
    if (!tokenPb.ParseFromString(userTokenSerialized)) {
        // we want to treat invalid token as empty,
        // so then result object must be recreated (or cleared)
        // because failed parsing makes result object dirty
        tokenPb = NACLibProto::TUserToken();
    }
    return NACLib::TUserToken(tokenPb);
}

template <class Iterable>
bool IsTokenAllowedImpl(const TAppData* appData, const NACLib::TUserToken* userToken, const Iterable& allowedSIDs) {
    if (appData && !appData->EnforceUserTokenRequirement) {
        if (!appData->EnforceUserTokenCheckRequirement || !userToken || userToken->GetSerializedToken().empty()) {
            return true;
        }
    }
    return IsTokenAllowed(userToken, allowedSIDs);
}

}

bool IsTokenAllowed(const NACLib::TUserToken* userToken, const TVector<TString>& allowedSIDs) {
    return IsTokenAllowedImpl(userToken, allowedSIDs);
}

bool IsTokenAllowed(const NACLib::TUserToken* userToken, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs) {
    return IsTokenAllowedImpl(userToken, allowedSIDs);
}

bool IsTokenAllowed(const TString& userTokenSerialized, const TVector<TString>& allowedSIDs) {
    NACLib::TUserToken userToken = ParseUserToken(userTokenSerialized);
    return IsTokenAllowed(&userToken, allowedSIDs);
}

bool IsTokenAllowed(const TString& userTokenSerialized, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs) {
    NACLib::TUserToken userToken = ParseUserToken(userTokenSerialized);
    return IsTokenAllowed(&userToken, allowedSIDs);
}

bool IsTokenAllowed(const TAppData* appData, const NACLib::TUserToken* userToken, const TVector<TString>& allowedSIDs) {
    return IsTokenAllowedImpl(appData, userToken, allowedSIDs);
}

bool IsTokenAllowed(const TAppData* appData, const NACLib::TUserToken* userToken, const NProtoBuf::RepeatedPtrField<TString>& allowedSIDs) {
    return IsTokenAllowedImpl(appData, userToken, allowedSIDs);
}

bool IsAdministrator(const TAppData* appData, const TString& userTokenSerialized) {
    return IsTokenAllowed(userTokenSerialized, appData->AdministrationAllowedSIDs);
}

bool IsAdministrator(const TAppData* appData, const NACLib::TUserToken* userToken) {
    return IsTokenAllowed(userToken, appData->AdministrationAllowedSIDs);
}

bool IsDatabaseAdministrator(const NACLib::TUserToken* userToken, const NACLib::TSID& databaseOwner) {
    // no database, no access
    if (databaseOwner.empty()) {
        return false;
    }
    // empty token can't have raised access level
    if (!userToken || userToken->GetUserSID().empty()) {
        return false;
    }
    return userToken->IsExist(databaseOwner);
}

}
