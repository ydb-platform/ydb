#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

TConstructor::TConstructor(const IPathIdTranslator& translator, const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const ERequestSorting sorting)
    : TBase(sorting, tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    std::deque<TDataSourceConstructor> constructors;
    for (auto&& [internalPathId, granuleMeta] : engineImpl->GetTables()) {
        if (unifiedPathId.HasInternalPathId() && unifiedPathId.GetInternalPathIdVerified() != internalPathId) {
            continue;
        }
        AFL_VERIFY(unifiedPathId.HasSchemeShardLocalPathId());
        if (unifiedPathId.HasInternalPathId()) {
            constructors.emplace_back(unifiedPathId.GetSchemeShardLocalPathIdVerified(), TabletId, granuleMeta);
            if (!pkFilter->IsUsed(constructors.back().GetStart().GetValue().BuildSortablePosition(),
                    constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
                constructors.pop_back();
            }
            continue;
        }
        for (const auto& schemeShardLocalPathId: translator.ResolveSchemeShardLocalPathIdsVerified(granuleMeta->GetPathId())) {
            constructors.emplace_back(schemeShardLocalPathId, TabletId, granuleMeta);
            if (!pkFilter->IsUsed(constructors.back().GetStart().GetValue().BuildSortablePosition(),
                    constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
                constructors.pop_back();
            }
        }
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
