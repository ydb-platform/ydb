#pragma once
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NOlap::NDataAccessorControl {
class IAccessorCallback {
public:
    virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) = 0;
    virtual ~IAccessorCallback() = default;
};

class TActorAccessorsCallback: public IAccessorCallback {
private:
    const NActors::TActorId ActorId;

public:
    virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) override;
    TActorAccessorsCallback(const NActors::TActorId& actorId)
        : ActorId(actorId) {
    }
};

class TConsumerPortions {
private:
    YDB_READONLY_DEF(std::vector<ui64>, Portions);

public:
    void AddPortion(const ui64 p) {
        Portions.emplace_back(p);
    }
};

class TPortionsByConsumer {
private:
    THashMap<NGeneralCache::TPortionsMetadataCachePolicy::EConsumer, TConsumerPortions> Consumers;

public:
    ui64 GetPortionsCount() const {
        ui64 result = 0;
        for (auto&& i : Consumers) {
            result += i.second.GetPortions().size();
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
private:
    const TInternalPathId PathId;

    virtual void DoAskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) = 0;
    virtual TDataCategorized DoAnalyzeData(const TPortionsByConsumer& portions) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;

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
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
