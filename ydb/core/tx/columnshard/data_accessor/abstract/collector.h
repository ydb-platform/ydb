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

class IGranuleDataAccessor {
private:
    const ui64 PathId;

    virtual THashMap<ui64, TPortionDataAccessor> DoAskData(
        const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;

public:
    virtual ~IGranuleDataAccessor() = default;

    ui64 GetPathId() const {
        return PathId;
    }

    IGranuleDataAccessor(const ui64 pathId)
        : PathId(pathId) {
    }

    THashMap<ui64, TPortionDataAccessor> AskData(
        const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<IAccessorCallback>& callback, const TString& consumer);
    void ModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
        return DoModifyPortions(add, remove);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
