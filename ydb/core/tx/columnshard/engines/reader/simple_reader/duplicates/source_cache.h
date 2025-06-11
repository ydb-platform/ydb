#pragma once

#include "events.h"
#include "fetching.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TSourceCache : TMoveOnly {
public:
    using TCacheItem = std::shared_ptr<TColumnsData>;
    using TSourcesData = THashMap<ui64, TCacheItem>;

private:
    class TSizeProvider {
    public:
        size_t operator()(const TCacheItem& item) const {
            return item->GetRawSize();
        }
    };

    class TFetchingInfo;

private:
    const NColumnShard::TDuplicateFilteringCounters& Counters;
    std::shared_ptr<TCommonFetchingContext> FetchingContext;
    THashMap<ui64, TFetchingInfo> FetchingSources;
    TLRUCache<ui64, TCacheItem, TNoopDelete, TSizeProvider> SourcesData;

private:
    void OnFetchingResult(const ui64 sourceId, TConclusion<TColumnsData>&& result);
    void InitOwner(const TActorId& owner);

public:
    void GetSourcesData(const std::vector<std::shared_ptr<TPortionInfo>>& sources, const TEvRequestFilter::TPtr& originalRequest);

    TSourceCache(const std::shared_ptr<TReadContext>& readContext)
        : Counters(readContext->GetDuplicateFilteringCounters())
        , FetchingContext(std::make_shared<TCommonFetchingContext>(readContext,
              [&readContext]() {
                  std::set<ui32> columnIds = readContext->GetReadMetadata()->GetIndexVersions().GetLastSchema()->GetPkColumnsIds();
                  for (const auto& id : IIndexInfo::GetSnapshotColumnIds()) {
                      columnIds.insert(id);
                  }
                  return columnIds;
              }()))
        , SourcesData(NYDBTest::TControllers::GetColumnShardController()->GetDuplicateManagerCacheSize()) {
    }

    ~TSourceCache();
};

}   // namespace NKikimr::NOlap::NReader::NSimple
