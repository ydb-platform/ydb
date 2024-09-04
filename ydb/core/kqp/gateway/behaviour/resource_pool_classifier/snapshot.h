#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/fetcher.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierSnapshot : public NMetadata::NFetcher::ISnapshot {
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TConfigsMap = std::unordered_map<TString, std::unordered_map<TString, TResourcePoolClassifierConfig>>;

    YDB_ACCESSOR_DEF(TConfigsMap, ResourcePoolClassifierConfigs);

protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;

public:
    using TBase::TBase;

    std::optional<TResourcePoolClassifierConfig> GetClassifierConfig(const TString& database, const TString& name) const;
};

}  // namespace NKikimr::NKqp
