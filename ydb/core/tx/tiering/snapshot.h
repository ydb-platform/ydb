#pragma once
#include "rule.h"
#include "tier_config.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TConfigsSnapshot: public NMetadataProvider::ISnapshot {
private:
    using TBase = NMetadataProvider::ISnapshot;
    using TConfigsMap = TMap<TGlobalTierId, TTierConfig>;
    YDB_ACCESSOR_DEF(TConfigsMap, TierConfigs);
    using TTieringMap = TMap<TString, TTableTiering>;
    YDB_ACCESSOR_DEF(TTieringMap, TableTierings);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    std::vector<TTierConfig> GetTiersForPathId(const ui64 pathId) const;
    const TTableTiering* GetTableTiering(const TString& tablePath) const;
    void RemapTablePathToId(const TString& path, const ui64 pathId);
    std::optional<TTierConfig> GetValue(const TGlobalTierId& key) const;
    using TBase::TBase;
};

}
