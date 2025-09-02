#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NTxInteractions {

class TEvWriteWriter: public ITxEventWriter {
private:
    YDB_READONLY_DEF(TInternalPathId, PathId);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, RecordBatch);

    virtual bool DoCheckInteraction(
        const ui64 selfTxId, TInteractionsContext& context, TTxConflicts& conflicts, TTxConflicts& /*notifications*/) const override {
        THashSet<ui64> txIds = context.GetAffectedTxIds(PathId, RecordBatch);
        txIds.erase(selfTxId);
        TTxConflicts result;
        for (auto&& i : txIds) {
            result.Add(selfTxId, i);
        }
        std::swap(result, conflicts);
        return true;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override {
        return nullptr;
    }

public:
    TEvWriteWriter(const TInternalPathId pathId, const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema)
        : PathId(pathId)
        , RecordBatch(NArrow::TColumnOperator().Extract(batch, pkSchema->field_names())) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(RecordBatch);
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
