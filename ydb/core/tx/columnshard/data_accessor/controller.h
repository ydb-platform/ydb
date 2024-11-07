#pragma once
#include "request.h"

namespace NKikimr::NOlap {

class IGranuleDataAccessor {
private:
    const ui64 PathId;

    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;

public:
    virtual ~IGranuleDataAccessor() = default;

    ui64 GetPathId() const {
        return PathId;
    }

    IGranuleDataAccessor(const ui64 pathId)
        : PathId(pathId) {
    }

    void AskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
        AFL_VERIFY(request);
        AFL_VERIFY(request->HasSubscriber());
        return DoAskData(request);
    }
    void ModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
        return DoModifyPortions(add, remove);
    }
};

class TMemDataAccessor: public IGranuleDataAccessor {
private:
    using TBase = IGranuleDataAccessor;
    THashMap<ui64, TPortionDataAccessor> Accessors;
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override {
        std::vector<TPortionDataAccessor> accessors;
        auto& portions = request->StartFetching(GetPathId());
        for (auto&& i : portions) {
            auto it = Accessors.find(i->GetPortionId());
            AFL_VERIFY(it != Accessors.end());
            accessors.emplace_back(it->second);
        }
        request->AddData(GetPathId(), std::move(accessors));
    }
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) override {
        for (auto&& i : remove) {
            AFL_VERIFY(Accessors.erase(i));
        }
        for (auto&& i : add) {
            AFL_VERIFY(Accessors.emplace(i.GetPortionInfo().GetPortionId(), i).second);
        }
    }

public:
    TMemDataAccessor(const ui64 pathId)
        : TBase(pathId) {
    }
};

class TTabletDataAccessor: public IGranuleDataAccessor {
private:
    const NActors::TActorId TabletActorId;
    using TBase = IGranuleDataAccessor;
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& /*add*/, const std::vector<ui64>& /*remove*/) override {
    }

public:
    TTabletDataAccessor(const ui64 pathId)
        : TBase(pathId) {
    }
};

}   // namespace NKikimr::NOlap
