#pragma once
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_chunks.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
class TPortionInfo;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NDataAccessorControl {
class IAccessorCallback {
public:
    virtual void OnAccessorsFetched(std::vector<std::shared_ptr<const TPortionChunks>>&& accessors) = 0;
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

}   // namespace NKikimr::NOlap::NDataAccessorControl
