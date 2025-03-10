#pragma once
#include "secret.h"
#include "access.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/secret/accessor/snapshot.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NSecret {

class TSnapshot: public NFetcher::ISnapshot, public ISecretAccessor {
private:
    using TBase = NFetcher::ISnapshot;
    using TSecrets = std::map<TSecretId, TSecret>;
    using TIdsByName = THashMap<TString, std::vector<TSecretId>>;
    YDB_READONLY_DEF(TSecrets, Secrets);
    YDB_READONLY_DEF(std::vector<TAccess>, Access);
    YDB_READONLY_DEF(TIdsByName, IndexByName);
private:
    void BuildIndex();
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    using TBase::TBase;
    bool CheckSecretAccess(const TSecretIdOrValue& sIdOrValue, const NACLib::TUserToken& userToken) const override;
    bool PatchString(TString& stringForPath) const override;
    TConclusion<TString> GetSecretValue(const TSecretIdOrValue& secretId) const override;
};

}
