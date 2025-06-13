#pragma once
#include "library/cpp/cache/cache.h"
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NOlap::NDataAccessorControl {
class IAccessorCallback {
public:
    virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) = 0;
    virtual ~IAccessorCallback() = default;
};

class Foo {
public:
    virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors, const TActorId& owner) = 0;
    virtual ~Foo() = default;
};

class TActorAccessorsCallback: public Foo {
private:
    const NActors::TActorId ActorId;

public:
    virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors, const TActorId& owner) override;
    TActorAccessorsCallback(const NActors::TActorId& actorId)
        : ActorId(actorId) {
    }
};

class TConsumerPortions {
private:
    YDB_READONLY_DEF(TString, ConsumerId);
    YDB_READONLY_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);

public:
    void AddPortion(const TPortionInfo::TConstPtr& p) {
        Portions.emplace_back(p);
    }

    TConsumerPortions(const TString& consumerId)
        : ConsumerId(consumerId) {
    }
};

class TPortionsByConsumer {
private:
    THashMap<TString, TConsumerPortions> Consumers;

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

    TConsumerPortions& UpsertConsumer(const TString& consumerId) {
        auto it = Consumers.find(consumerId);
        if (it == Consumers.end()) {
            it = Consumers.emplace(consumerId, consumerId).first;
        }
        return it->second;
    }

    const THashMap<TString, TConsumerPortions>& GetConsumers() const {
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
        size_t operator()(const TPortionDataAccessor& data) {
            return data.GetMetadataSize();
        }
    };
private:
    const TInternalPathId PathId;

    virtual void DoAskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions, const std::shared_ptr<IAccessorCallback>& callback) = 0;
    virtual TDataCategorized DoAnalyzeData(const TPortionsByConsumer& portions) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;
    virtual void DoSetCache(std::shared_ptr<TLRUCache<std::tuple<TActorId, TInternalPathId, ui64>, TPortionDataAccessor, TNoopDelete, TMetadataSizeProvider>>) = 0;
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
    void SetCache(std::shared_ptr<TLRUCache<std::tuple<TActorId, TInternalPathId, ui64>, TPortionDataAccessor, TNoopDelete, TMetadataSizeProvider>> cache) {
        DoSetCache(cache);
    }
    void SetOwner(const TActorId& owner) {
        DoSetOwner(owner);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
