#pragma once

#include "secret_id.h"

#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NMetadata::NSecret {

class ISecretAccessor {
public:
    bool CheckSecretAccess(const TSecretIdOrValue& sIdOrValue, const NACLib::TUserToken& userToken) const = 0;
    bool PatchString(TString& stringForPath) const = 0;
    TConclusion<TString> GetSecretValue(const TSecretIdOrValue& secretId) const = 0;
    std::vector<TSecretId> GetSecretIds(const std::optional<NACLib::TUserToken>& userToken, const TString& secretId) const = 0;
};

}   // namespace NKikimr::NMetadata::NSecret
