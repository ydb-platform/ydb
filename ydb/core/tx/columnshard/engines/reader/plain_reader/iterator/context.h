#pragma once
#include "columns_set.h"
#include "fetching.h"
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/formats/arrow/reader/merger.h>

namespace NKikimr::NOlap::NReader::NPlain {

class IDataSource;

class TSpecialReadContext: public NCommon::TSpecialReadContext {
private:
    using TBase = NCommon::TSpecialReadContext;
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, MergeStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FilterStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FetchingStageMemory);

    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshotsFilter, const bool exclusiveSource, 
        const bool partialUsageByPredicate, const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const;
    TMutex Mutex;
    std::array<std::array<std::array<std::array<std::array<std::array<std::optional<std::shared_ptr<TFetchingScript>>, 2>, 2>, 2>, 2>, 2>, 2>
        CacheFetchingScripts;
    std::shared_ptr<TFetchingScript> AskAccumulatorsScript;

public:
    const ui64 ReduceMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetReduceMemoryIntervalLimit();
    const ui64 RejectMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetRejectMemoryIntervalLimit();
    const ui64 ReadSequentiallyBufferSize = TGlobalLimits::DefaultReadSequentiallyBufferSize;

    ui64 GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources);
    ui64 GetRequestedMemoryBytes() const {
        return MergeStageMemory->GetFullMemory() + FilterStageMemory->GetFullMemory() + FetchingStageMemory->GetFullMemory();
    }

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    std::unique_ptr<NArrow::NMerger::TMergePartialStream> BuildMerger() const;

    TString ProfileDebugString() const;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source);
};

}
