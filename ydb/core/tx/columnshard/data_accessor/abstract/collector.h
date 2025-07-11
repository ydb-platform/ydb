#pragma once
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
class TPortionInfo;
}

namespace NKikimr::NOlap {
class TGranuleMeta;
class TPortionInfo;
}

namespace NKikimr::NOlap::NDataAccessorControl {
class IAccessorCallback {
public:
    virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) = 0;
    virtual ~IAccessorCallback() = default;
};

class TConsumerPortions {
private:
    YDB_READONLY_DEF(std::vector<ui64>, PortionIds);

public:
    void AddPortion(const std::shared_ptr<const TPortionInfo>& p);
    void AddPortion(const ui64 portionId) {
        PortionIds.emplace_back(portionId);
    }

    ui32 GetPortionsCount() const {
        return PortionIds.size();
    }

    std::vector<TPortionInfo::TConstPtr> GetPortions(const TGranuleMeta& granule) const;
};

class TPortionsByConsumer {
private:
    THashMap<NGeneralCache::TPortionsMetadataCachePolicy::EConsumer, TConsumerPortions> Consumers;

public:
    ui64 GetPortionsCount() const {
        ui64 result = 0;
        for (auto&& i : Consumers) {
            result += i.second.GetPortionsCount();
        }
        return result;
    }

    bool IsEmpty() const {
        return Consumers.empty();
    }

    TConsumerPortions& UpsertConsumer(const NGeneralCache::TPortionsMetadataCachePolicy::EConsumer consumer) {
        auto it = Consumers.find(consumer);
        if (it == Consumers.end()) {
            it = Consumers.emplace(consumer, TConsumerPortions()).first;
        }
        return it->second;
    }

    const THashMap<NGeneralCache::TPortionsMetadataCachePolicy::EConsumer, TConsumerPortions>& GetConsumers() const {
        return Consumers;
    }
};

class TDataCategorized {
private:
    YDB_ACCESSOR_DEF(TPortionsByConsumer, PortionsToAsk);
    YDB_READONLY_DEF(std::vector<TPortionDataAccessor>, CachedAccessors);

public:
    void AddFromCache(const TPortionDataAccessor& accessor) {
        CachedAccessors.emplace_back(accessor);
    }
};

class IGranuleDataAccessor {
public:
    struct TMetadataSizeProvider {
        size_t operator()(const TPortionDataAccessor& data) const {
            return data.GetMetadataSize();
        }
    };

    using TSharedMetadataAccessorCache = TLRUCache<std::tuple<TActorId, TInternalPathId, ui64>, TPortionDataAccessor, TNoopDelete, IGranuleDataAccessor::TMetadataSizeProvider>;
private:
    const TInternalPathId PathId;

    virtual void DoAskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) = 0;
    virtual TDataCategorized DoAnalyzeData(const TPortionsByConsumer& portions) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;
    virtual void DoSetCache(std::shared_ptr<TSharedMetadataAccessorCache>) = 0;
    virtual void DoSetOwner(const TActorId& owner) = 0;

public:

    virtual ~IGranuleDataAccessor() = default;

    TInternalPathId GetPathId() const {
        return PathId;
    }

    IGranuleDataAccessor(const TInternalPathId pathId)
        : PathId(pathId) {
    }

    void AskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback);
    TDataCategorized AnalyzeData(const TPortionsByConsumer& portions);
    void ModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
        return DoModifyPortions(add, remove);
    }
    void SetCache(std::shared_ptr<TSharedMetadataAccessorCache> cache) {
        DoSetCache(cache);
    }
    void SetOwner(const TActorId& owner) {
        DoSetOwner(owner);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
