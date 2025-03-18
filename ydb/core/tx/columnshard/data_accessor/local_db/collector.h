#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>

#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

class TCollector: public IGranuleDataAccessor {
private:
    const NActors::TActorId TabletActorId;
    struct TMetadataSizeProvider {
        size_t operator()(const TPortionDataAccessor& data) {
            return data.GetMetadataSize();
        }
    };

    TLRUCache<ui64, TPortionDataAccessor, TNoopDelete, TMetadataSizeProvider> AccessorsCache;
    using TBase = IGranuleDataAccessor;
    virtual void DoAskData(const std::vector<TPortionInfo::TConstPtr>& portions,
        const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer) override;
    virtual TDataCategorized DoAnalyzeData(const std::vector<TPortionInfo::TConstPtr>& portions, const TString& consumer) override;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) override;

public:
    TCollector(const NColumnShard::TInternalPathId pathId, const ui64 maxSize, const NActors::TActorId& actorId)
        : TBase(pathId)
        , TabletActorId(actorId)
        , AccessorsCache(maxSize) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
