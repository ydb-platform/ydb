#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NTxInteractions {

class TEvReadFinishedWriter: public ITxEventWriter {
private:
    YDB_READONLY_DEF(TInternalPathId, PathId);
    TTxConflicts Conflicts;

    virtual bool DoCheckInteraction(
        const ui64 /*selfLockId*/, TInteractionsContext& /*context*/, TTxConflicts& conflicts, TTxConflicts& /*notifications*/) const override {
        conflicts = Conflicts;
        return true;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override {
        return nullptr;
    }

public:
    TEvReadFinishedWriter(const TInternalPathId pathId, const TTxConflicts& conflicts)
        : PathId(pathId)
        , Conflicts(conflicts)
    {
        AFL_VERIFY(PathId);
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
