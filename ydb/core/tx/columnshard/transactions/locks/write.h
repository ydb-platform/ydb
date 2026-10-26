#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NTxInteractions {

class TEvWriteWriter: public ITxEventWriter {
private:
    YDB_READONLY_DEF(TInternalPathId, PathId);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, RecordBatch);
    std::vector<NScheme::TTypeInfo> LogicalTypes;

    virtual bool DoCheckInteraction(
        const ui64 selfLockId, TInteractionsContext& context, TTxConflicts& conflicts, TTxConflicts& /*notifications*/) const override {
        THashSet<ui64> lockIdsToBreakOnCommit = context.GetAffectedLockIds(PathId, RecordBatch, LogicalTypes);
        lockIdsToBreakOnCommit.erase(selfLockId);
        TTxConflicts result;
        for (const auto& lockIdToBreak : lockIdsToBreakOnCommit) {
            // when selfLockId commits, lockIdToBreak will get broken
            result.Add(selfLockId, lockIdToBreak);
        }
        std::swap(result, conflicts);
        return true;
    }

    virtual std::shared_ptr<ITxEvent> DoBuildEvent() override {
        return nullptr;
    }

public:
    TEvWriteWriter(const TInternalPathId pathId, const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::vector<std::pair<TString, NScheme::TTypeInfo>>& pkSchema)
        : PathId(pathId)
    {
        std::vector<std::string> names;
        names.reserve(pkSchema.size());
        LogicalTypes.reserve(pkSchema.size());
        for (const auto& [name, type] : pkSchema) {
            names.emplace_back(name);
            LogicalTypes.emplace_back(type);
        }

        RecordBatch = NArrow::TColumnOperator().Extract(batch, names);
        AFL_VERIFY(PathId);
        AFL_VERIFY(RecordBatch);
        AFL_VERIFY(LogicalTypes.size() == static_cast<size_t>(RecordBatch->num_columns()));
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
