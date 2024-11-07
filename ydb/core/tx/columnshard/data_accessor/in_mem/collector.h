#pragma once

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {
class TCollector: public IGranuleDataAccessor {
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
    TCollector(const ui64 pathId)
        : TBase(pathId) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
