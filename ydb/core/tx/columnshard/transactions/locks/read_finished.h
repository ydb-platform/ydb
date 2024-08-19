#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NTxInteractions {

class TEvReadFinishedWriter: public ITxEventWriter {
private:
    YDB_READONLY(ui64, PathId, 0);
    TTxConflicts Conflicts;

    virtual TTxConflicts DoCheckInteraction(const ui64 /*selfTxId*/, TInteractionsContext& /*context*/) const override {
        return Conflicts;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override {
        return nullptr;
    }

public:
    TEvReadFinishedWriter(const ui64 pathId, const TTxConflicts& conflicts)
        : PathId(pathId)
        , Conflicts(conflicts)
    {
        AFL_VERIFY(PathId);
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
