#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NTxInteractions {

class TEvWriteWriter: public ITxEventWriter {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, RecordBatch);

    virtual TTxConflicts DoCheckInteraction(const ui64 selfTxId, TInteractionsContext& context) const override {
        THashSet<ui64> txIds = context.GetAffectedTxIds(PathId, RecordBatch);
        TTxConflicts result;
        for (auto&& i : txIds) {
            result.Add(selfTxId, i);
        }
        return result;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override {
        return nullptr;
    }

public:
    TEvWriteWriter(const ui64 pathId, const std::shared_ptr<arrow::RecordBatch>& batch)
        : PathId(pathId)
        , RecordBatch(batch) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(RecordBatch);
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
