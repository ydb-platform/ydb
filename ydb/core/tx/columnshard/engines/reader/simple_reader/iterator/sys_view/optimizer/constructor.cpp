#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

TConstructor::TConstructor(const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const ERequestSorting sorting)
    : TBase(sorting, tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    std::deque<TDataSourceConstructor> constructors;
    for (auto&& i : engineImpl->GetTables()) {
        if (unifiedPathId.HasInternalPathId() && unifiedPathId.GetInternalPathIdVerified() != i.first) {
            continue;
        }
        AFL_VERIFY(unifiedPathId.HasSchemeShardLocalPathId());
        constructors.emplace_back(unifiedPathId.GetSchemeShardLocalPathIdVerified(), TabletId, i.second);
        if (!pkFilter->IsUsed(constructors.back().GetStart().GetValue().BuildSortablePosition(),
                constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
            constructors.pop_back();
        }
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
