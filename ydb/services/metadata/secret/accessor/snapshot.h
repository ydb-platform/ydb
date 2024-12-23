#pragma once

#include "secret_id.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NMetadata::NSecret {

class ISecretAccessor {
public:
    virtual bool CheckSecretAccess(const TSecretIdOrValue& sIdOrValue, const NACLib::TUserToken& userToken) const = 0;
    virtual bool PatchString(TString& stringForPath) const = 0;
    virtual TConclusion<TString> GetSecretValue(const TSecretIdOrValue& secretId) const = 0;
    virtual std::vector<TSecretId> GetSecretIds(const std::optional<NACLib::TUserToken>& userToken, const TString& secretId) const = 0;
};

}   // namespace NKikimr::NMetadata::NSecret
