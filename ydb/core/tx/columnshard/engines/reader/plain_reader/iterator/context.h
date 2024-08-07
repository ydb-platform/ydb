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

class TSpecialReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TReadContext>, CommonContext);

    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, SpecColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, MergeColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ShardingColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, DeletionColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PredicateColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ProgramInputColumns);

    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, MergeStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FilterStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FetchingStageMemory);

    NIndexes::TIndexCheckerContainer IndexChecker;
    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshotsFilter, const bool exclusiveSource, 
        const bool partialUsageByPredicate, const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const;
    std::array<std::array<std::array<std::array<std::array<std::array<std::shared_ptr<TFetchingScript>, 2>, 2>, 2>, 2>, 2>, 2> CacheFetchingScripts;

public:
    static const inline ui64 DefaultRejectMemoryIntervalLimit = TGlobalLimits::DefaultRejectMemoryIntervalLimit;
    static const inline ui64 DefaultReduceMemoryIntervalLimit = TGlobalLimits::DefaultReduceMemoryIntervalLimit;
    static const inline ui64 DefaultReadSequentiallyBufferSize = TGlobalLimits::DefaultReadSequentiallyBufferSize;

    const ui64 ReduceMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetReduceMemoryIntervalLimit(DefaultReduceMemoryIntervalLimit);
    const ui64 RejectMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetRejectMemoryIntervalLimit(DefaultRejectMemoryIntervalLimit);
    const ui64 ReadSequentiallyBufferSize = DefaultReadSequentiallyBufferSize;

    ui64 GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources);
    ui64 GetRequestedMemoryBytes() const {
        return MergeStageMemory->GetFullMemory() + FilterStageMemory->GetFullMemory() + FetchingStageMemory->GetFullMemory();
    }

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    ~TSpecialReadContext() {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("profile", ProfileDebugString());
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("fetching", DebugString());
    }

    std::unique_ptr<NArrow::NMerger::TMergePartialStream> BuildMerger() const;

    TString DebugString() const;
    TString ProfileDebugString() const;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) const;
};

}
