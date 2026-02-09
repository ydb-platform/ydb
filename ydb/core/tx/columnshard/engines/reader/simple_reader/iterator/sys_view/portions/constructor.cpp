#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

TConstructor::TConstructor(const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
    const TSnapshot reqSnapshot, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const ERequestSorting sorting)
    : TBase(sorting, tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);

    std::deque<TDataSourceConstructor> constructors;
    for (auto&& [internalPathId, granuleMeta] : engineImpl->GetTables()) {
        if (unifiedPathId.HasInternalPathId() && unifiedPathId.GetInternalPathIdVerified() != internalPathId) {
            continue;
        }
        AFL_VERIFY(unifiedPathId.HasInternalPathId());
        AFL_VERIFY(unifiedPathId.HasSchemeShardLocalPathId());
        std::vector<TPortionInfo::TConstPtr> portionsAll;
        for (auto&& [_, portionInfo] : granuleMeta->GetPortions()) {
            AFL_VERIFY(internalPathId == portionInfo->GetPathId());
            if (reqSnapshot < portionInfo->RecordSnapshotMin()) {
                continue;
            }
            if (portionInfo->IsRemovedFor(reqSnapshot)) {
                continue;
            }
            portionsAll.emplace_back(portionInfo);
        }
        std::sort(portionsAll.begin(), portionsAll.end(), TPortionInfo::TPortionAddressComparator());

        std::vector<TPortionInfo::TConstPtr> portions;
        for (auto&& p : portionsAll) {
            portions.emplace_back(p);
            if (portions.size() == 10) {
                constructors.emplace_back(NColumnShard::TUnifiedPathId::BuildValid(unifiedPathId.GetInternalPathIdVerified(), unifiedPathId.GetSchemeShardLocalPathIdVerified()), TabletId, std::move(portions));
                if (!pkFilter->IsUsed(constructors.back().GetStart().GetValue().BuildSortablePosition(),
                        constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
                    constructors.pop_back();
                }
                portions.clear();
            }
        }
        if (portions.size()) {
            constructors.emplace_back(NColumnShard::TUnifiedPathId::BuildValid(unifiedPathId.GetInternalPathIdVerified(), unifiedPathId.GetSchemeShardLocalPathIdVerified()), TabletId, std::move(portions));
            if (!pkFilter->IsUsed(constructors.back().GetStart().GetValue().BuildSortablePosition(),
                    constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
                constructors.pop_back();
            }
            portions.clear();
        }
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
