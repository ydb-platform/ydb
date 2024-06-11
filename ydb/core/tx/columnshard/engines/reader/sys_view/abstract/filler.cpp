#include "filler.h"
#include "metadata.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

NKikimr::TConclusionStatus TMetadataFromStore::DoFillMetadata(const NColumnShard::TColumnShard* shard, const std::shared_ptr<TReadMetadataBase>& metadataExt, const TReadDescription& read) const {
    std::shared_ptr<TReadStatsMetadata> metadata = dynamic_pointer_cast<TReadStatsMetadata>(metadataExt);
    if (!metadata) {
        return TConclusionStatus::Fail("incorrect metadata class for filler");
    }
    const TColumnEngineForLogs* logsIndex = dynamic_cast<const TColumnEngineForLogs*>(shard->GetIndexOptional());
    if (!logsIndex) {
        return TConclusionStatus::Success();
    }

    THashSet<ui64> pathIds;
    for (auto&& filter : read.PKRangesFilter) {
        const ui64 fromPathId = *filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1);
        const ui64 toPathId = *filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>());
        auto pathInfos = logsIndex->GetTables(fromPathId, toPathId);
        for (auto&& pathInfo : pathInfos) {
            if (pathIds.emplace(pathInfo->GetPathId()).second) {
                metadata->IndexGranules.emplace_back(BuildGranuleView(*pathInfo, metadata->IsDescSorted()));
            }
        }
    }
    std::sort(metadata->IndexGranules.begin(), metadata->IndexGranules.end());
    if (metadata->IsDescSorted()) {
        std::reverse(metadata->IndexGranules.begin(), metadata->IndexGranules.end());
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TMetadataFromTable::DoFillMetadata(const NColumnShard::TColumnShard* shard, const std::shared_ptr<TReadMetadataBase>& metadataExt, const TReadDescription& read) const {
    std::shared_ptr<TReadStatsMetadata> metadata = dynamic_pointer_cast<TReadStatsMetadata>(metadataExt);
    if (!metadata) {
        return TConclusionStatus::Fail("incorrect metadata class for filler");
    }
    const TColumnEngineForLogs* logsIndex = dynamic_cast<const TColumnEngineForLogs*>(shard->GetIndexOptional());
    if (!logsIndex) {
        return TConclusionStatus::Success();
    }
    for (auto&& filter : read.PKRangesFilter) {
        const ui64 fromPathId = *filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1);
        const ui64 toPathId = *filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>());
        if (fromPathId <= read.PathId && read.PathId <= toPathId) {
            auto pathInfo = logsIndex->GetGranuleOptional(read.PathId);
            if (!pathInfo) {
                continue;
            }
            metadata->IndexGranules.emplace_back(BuildGranuleView(*pathInfo, metadata->IsDescSorted()));
            break;
        }
    }
    std::sort(metadata->IndexGranules.begin(), metadata->IndexGranules.end());
    if (metadata->IsDescSorted()) {
        std::reverse(metadata->IndexGranules.begin(), metadata->IndexGranules.end());
    }
    return TConclusionStatus::Success();
}

}