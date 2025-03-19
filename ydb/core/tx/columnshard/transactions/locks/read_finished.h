#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NTxInteractions {

class TEvReadFinishedWriter: public ITxEventWriter {
private:
    YDB_READONLY(NColumnShard::TInternalPathId, PathId, NColumnShard::TInternalPathId{});
    TTxConflicts Conflicts;

    virtual bool DoCheckInteraction(
        const ui64 /*selfTxId*/, TInteractionsContext& /*context*/, TTxConflicts& conflicts, TTxConflicts& /*notifications*/) const override {
        conflicts = Conflicts;
        return true;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override {
        return nullptr;
    }

public:
    TEvReadFinishedWriter(const NColumnShard::TInternalPathId pathId, const TTxConflicts& conflicts)
        : PathId(pathId)
        , Conflicts(conflicts)
    {
        AFL_VERIFY(PathId);
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
