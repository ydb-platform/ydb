#pragma once
#include "request.h"

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

class TCollector: public IGranuleDataAccessor {
private:
    const NActors::TActorId TabletActorId;
    using TBase = IGranuleDataAccessor;
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& /*add*/, const std::vector<ui64>& /*remove*/) override {
    }

public:
    TCollector(const ui64 pathId)
        : TBase(pathId) {
    }
};

}   // namespace NKikimr::NOlap
