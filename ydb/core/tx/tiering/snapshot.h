#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersSnapshot: public NMetadata::NFetcher::ISnapshot {
private:
    using TBase = NMetadata::NFetcher::ISnapshot;
    using TConfigsMap = TMap<TString, TTierConfig>;
    YDB_ACCESSOR_DEF(TConfigsMap, TierConfigs);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    std::optional<TTierConfig> GetTierById(const TString& tierName) const;
    using TBase::TBase;
};

}
