#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/fetcher.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierSnapshot : public NMetadata::NFetcher::ISnapshot {
    using TConfigsMap = TMap<TString, TResourcePoolClassifierConfig>;
    YDB_ACCESSOR_DEF(TConfigsMap, ResourcePoolClassifierConfigs);

protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
};

}  // namespace NKikimr::NKqp
