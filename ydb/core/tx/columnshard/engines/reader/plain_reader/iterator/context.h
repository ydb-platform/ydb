#pragma once
#include "columns_set.h"
#include "fetching.h"
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
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PredicateColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ProgramInputColumns);

    NIndexes::TIndexCheckerContainer IndexChecker;
    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<TFetchingScript> BuildColumnsFetchingPlan(const bool needSnapshotsFilter, const bool exclusiveSource, const bool partialUsageByPredicate, const bool useIndexes, const bool needFilterSharding) const;
    std::array<std::array<std::array<std::array<std::array<std::shared_ptr<TFetchingScript>, 2>, 2>, 2>, 2>, 2> CacheFetchingScripts;

public:
    static const inline ui64 DefaultRejectMemoryIntervalLimit = ((ui64)3) << 30;
    static const inline ui64 DefaultReduceMemoryIntervalLimit = DefaultRejectMemoryIntervalLimit;
    static const inline ui64 DefaultReadSequentiallyBufferSize = ((ui64)8) << 20;

    const ui64 ReduceMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetReduceMemoryIntervalLimit(DefaultReduceMemoryIntervalLimit);
    const ui64 RejectMemoryIntervalLimit = NYDBTest::TControllers::GetColumnShardController()->GetRejectMemoryIntervalLimit(DefaultRejectMemoryIntervalLimit);
    const ui64 ReadSequentiallyBufferSize = DefaultReadSequentiallyBufferSize;

    ui64 GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources, const bool isExclusive);

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    std::unique_ptr<NArrow::NMerger::TMergePartialStream> BuildMerger() const;

    TString DebugString() const {
        return TStringBuilder() << "ef=" << EFColumns->DebugString() << ";"
                                << "sharding=" << ShardingColumns->DebugString() << ";"
                                << "pk=" << PKColumns->DebugString() << ";"
                                << "ff=" << FFColumns->DebugString() << ";"
                                << "program_input=" << ProgramInputColumns->DebugString();
    }

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    std::shared_ptr<TFetchingScript> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) const;
};

}
