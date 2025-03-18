#pragma once
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

class TDataCategorized {
private:
    YDB_READONLY_DEF(std::vector<TPortionInfo::TConstPtr>, PortionsToAsk);
    YDB_READONLY_DEF(std::vector<TPortionDataAccessor>, CachedAccessors);

public:
    void AddToAsk(const TPortionInfo::TConstPtr& p) {
        PortionsToAsk.emplace_back(p);
    }
    void AddFromCache(const TPortionDataAccessor& accessor) {
        CachedAccessors.emplace_back(accessor);
    }
};

class IGranuleDataAccessor {
private:
    const NColumnShard::TInternalPathId PathId;

    virtual void DoAskData(
        const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer) = 0;
    virtual TDataCategorized DoAnalyzeData(const std::vector<TPortionInfo::TConstPtr>& portions, const TString& consumer) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;

public:
    virtual ~IGranuleDataAccessor() = default;

    NColumnShard::TInternalPathId GetPathId() const {
        return PathId;
    }

    IGranuleDataAccessor(const NColumnShard::TInternalPathId pathId)
        : PathId(pathId) {
    }

    void AskData(
        const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer);
    TDataCategorized AnalyzeData(const std::vector<TPortionInfo::TConstPtr>& portions, const TString& consumer);
    void ModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
        return DoModifyPortions(add, remove);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
