#include "auth.h"

namespace NKikimr {

namespace {

bool HasToken(const TAppData* appData, const NACLib::TUserToken& userToken) {
    for (const auto& sid : appData->AdministrationAllowedSIDs) {
        if (userToken.IsExist(sid)) {
            return true;
        }
    }

    return false;
}

}

bool IsAdministrator(const TAppData* appData, const TString& userToken) {
    if (appData->AdministrationAllowedSIDs.empty()) {
        return true;
    }

    if (!userToken) {
        return false;
    }

    NACLibProto::TUserToken tokenPb;
    if (!tokenPb.ParseFromString(userToken)) {
        return false;
    }

    return HasToken(appData, NACLib::TUserToken(std::move(tokenPb)));
}

bool IsAdministrator(const TAppData* appData, const NACLib::TUserToken* userToken) {
    if (appData->AdministrationAllowedSIDs.empty()) {
        return true;
    }

    if (!userToken || userToken->GetSerializedToken().empty()) {
        return false;
    }

    return HasToken(appData, *userToken);
}

}
