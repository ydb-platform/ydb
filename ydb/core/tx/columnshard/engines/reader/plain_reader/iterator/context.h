#pragma once
#include "fetching.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetching.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NPlain {

class IDataSource;
using TColumnsSet = NCommon::TColumnsSet;
using EStageFeaturesIndexes = NCommon::EStageFeaturesIndexes;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;
using TFetchingScript = NCommon::TFetchingScript;

class TSpecialReadContext: public NCommon::TSpecialReadContext {
private:
    using TBase = NCommon::TSpecialReadContext;
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, MergeStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FilterStageMemory);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TStageFeatures>, FetchingStageMemory);

    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshotsFilter, const bool exclusiveSource,
        const bool partialUsageByPredicate, const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const;
    TMutex Mutex;
    std::array<std::array<std::array<std::array<std::array<std::array<std::optional<std::shared_ptr<TFetchingScript>>, 2>, 2>, 2>, 2>, 2>, 2>
        CacheFetchingScripts;
    std::shared_ptr<TFetchingScript> AskAccumulatorsScript;

    virtual std::shared_ptr<TFetchingScript> DoGetColumnsFetchingPlan(const std::shared_ptr<NCommon::IDataSource>& source) override;

public:
    const ui64 ReduceMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetReduceMemoryIntervalLimit();
    const ui64 RejectMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetRejectMemoryIntervalLimit();
    const ui64 ReadSequentiallyBufferSize = TGlobalLimits::DefaultReadSequentiallyBufferSize;

    ui64 GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources);
    ui64 GetRequestedMemoryBytes() const {
        return MergeStageMemory->GetFullMemory() + FilterStageMemory->GetFullMemory() + FetchingStageMemory->GetFullMemory();
    }

    std::unique_ptr<NArrow::NMerger::TMergePartialStream> BuildMerger() const;

    virtual TString ProfileDebugString() const override;

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);
};

}   // namespace NKikimr::NOlap::NReader::NPlain
