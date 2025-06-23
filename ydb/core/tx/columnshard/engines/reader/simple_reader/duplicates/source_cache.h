#pragma once

#include "common.h"
#include "events.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/data_reader/contexts.h>
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

    class TResponseConstructor;
    class TFetchingInfo {
    private:
        const ui64 SourceId;
        std::vector<std::shared_ptr<TResponseConstructor>> Callbacks;

    public:
        TFetchingInfo(const ui64 sourceId)
            : SourceId(sourceId) {
        }

        void AddCallback(const std::shared_ptr<TResponseConstructor>& callback);
        void Complete(const TCacheItem& result);
        void Abort(const TString& error);
    };

private:
    const NColumnShard::TDuplicateFilteringCounters& Counters;
    const TActorId Owner;
    const std::shared_ptr<NOlap::NDataFetcher::TEnvironment> FetchingEnvironment;
    const std::shared_ptr<ISnapshotSchema> ResultSchema;
    const std::shared_ptr<TVersionedIndex> SchemaIndex;

    THashMap<ui64, TFetchingInfo> FetchingSources;
    TLRUCache<ui64, TCacheItem, TNoopDelete, TSizeProvider> SourcesData;

public:
    void GetSourcesData(const std::vector<std::shared_ptr<TPortionInfo>>& sources, const TEvRequestFilter::TPtr& originalRequest);
    void OnFetchingResult(const NPrivate::TEvDuplicateFilterDataFetched::TPtr&);
    void InitOwner(const TActorId& owner);

    TSourceCache(const std::shared_ptr<TReadContext>& readContext, const TActorId& owner)
        : Counters(readContext->GetDuplicateFilteringCounters())
        , Owner(owner)
        , FetchingEnvironment(
              std::make_shared<NDataFetcher::TEnvironment>(readContext->GetDataAccessorsManager(), readContext->GetStoragesManager()))
        , ResultSchema(std::make_shared<TFilteredSnapshotSchema>(readContext->GetReadMetadata()->GetResultSchema(),
              [&readContext]() {
                  std::set<ui32> columnIds = readContext->GetReadMetadata()->GetIndexVersions().GetLastSchema()->GetPkColumnsIds();
                  for (const auto& id : IIndexInfo::GetSnapshotColumnIds()) {
                      columnIds.insert(id);
                  }
                  return columnIds;
              }()))
        , SchemaIndex(readContext->GetReadMetadata()->GetIndexVersionsPtr())
        , SourcesData(NYDBTest::TControllers::GetColumnShardController()->GetDuplicateManagerCacheSize()) {
    }

    ~TSourceCache();
};

}   // namespace NKikimr::NOlap::NReader::NSimple
