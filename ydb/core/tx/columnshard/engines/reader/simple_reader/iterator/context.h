#pragma once
#include "fetching.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;
using TColumnsSet = NCommon::TColumnsSet;
using EStageFeaturesIndexes = NCommon::EStageFeaturesIndexes;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;

class TSpecialReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TReadContext>, CommonContext);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TProcessGuard>, ProcessMemoryGuard);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TScopeGuard>, ProcessScopeGuard);

    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, SpecColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, MergeColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ShardingColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, DeletionColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PredicateColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, AllUsageColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ProgramInputColumns);

    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, MergeStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FilterStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FetchingStageMemory);

    TAtomic AbortFlag = 0;
    NIndexes::TIndexCheckerContainer IndexChecker;
    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
        const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const;
    TMutex Mutex;
    std::array<std::array<std::array<std::array<std::array<std::optional<std::shared_ptr<TFetchingScript>>, 2>, 2>, 2>, 2>, 2>
        CacheFetchingScripts;
    std::shared_ptr<TFetchingScript> AskAccumulatorsScript;

public:
    const ui64 ReduceMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetReduceMemoryIntervalLimit();
    const ui64 RejectMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetRejectMemoryIntervalLimit();
    const ui64 ReadSequentiallyBufferSize = TGlobalLimits::DefaultReadSequentiallyBufferSize;

    ui64 GetProcessMemoryControlId() const {
        AFL_VERIFY(ProcessMemoryGuard);
        return ProcessMemoryGuard->GetProcessId();
    }
    ui64 GetRequestedMemoryBytes() const {
        return MergeStageMemory->GetFullMemory() + FilterStageMemory->GetFullMemory() + FetchingStageMemory->GetFullMemory();
    }

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    bool IsAborted() const {
        return AtomicGet(AbortFlag);
    }

    void Abort() {
        AtomicSet(AbortFlag, 1);
    }

    ~TSpecialReadContext() {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("profile", ProfileDebugString());
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("fetching", DebugString());
    }

    TString DebugString() const;
    TString ProfileDebugString() const;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
