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

    THashSet< NColumnShard::TInternalPathId> pathIds;
    AFL_VERIFY(read.PKRangesFilter);
    for (auto&& filter : *read.PKRangesFilter) {
        const auto fromPathId = NColumnShard::TInternalPathId::FromInternalPathIdValue(*filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1));
        const auto toPathId =  NColumnShard::TInternalPathId::FromInternalPathIdValue(*filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>()));
        auto pathInfos = logsIndex->GetTables(fromPathId, toPathId);
        for (auto&& pathInfo : pathInfos) {
            if (pathIds.emplace(pathInfo->GetPathId()).second) {
                const auto localPathId = shard->GetTablesManager().GetTableLocalPathIdVerified(pathInfo->GetPathId());
                metadata->IndexGranules.emplace_back(BuildGranuleView(*pathInfo, localPathId, metadata->IsDescSorted(), metadata->GetRequestSnapshot()));
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
    AFL_VERIFY(read.PKRangesFilter);
    for (auto&& filter : *read.PKRangesFilter) {
        const auto fromPathId = NColumnShard::TInternalPathId::FromInternalPathIdValue(*filter.GetPredicateFrom().Get<arrow::UInt64Array>(0, 0, 1));
        const auto toPathId = NColumnShard::TInternalPathId::FromInternalPathIdValue(*filter.GetPredicateTo().Get<arrow::UInt64Array>(0, 0, Max<ui64>()));
        if (fromPathId <= read.PathId && read.PathId <= toPathId) {
            auto pathInfo = logsIndex->GetGranuleOptional(read.PathId);
            if (!pathInfo) {
                continue;
            }
            const auto localPathId = shard->GetTablesManager().GetTableLocalPathIdVerified(pathInfo->GetPathId());
            metadata->IndexGranules.emplace_back(BuildGranuleView(*pathInfo, localPathId, metadata->IsDescSorted(), metadata->GetRequestSnapshot()));
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