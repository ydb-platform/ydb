#pragma once
#include "columns_set.h"
#include "fetching.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NPlainReader {

class IDataSource;

class TSpecialReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TReadContext>, CommonContext);

    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, SpecColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, MergeColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PredicateColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ProgramInputColumns);

    NIndexes::TIndexCheckerContainer IndexChecker;
    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<IFetchingStep> BuildColumnsFetchingPlan(const bool needSnapshotsFilter, const bool exclusiveSource, const bool partialUsageByPredicate) const;
    std::array<std::array<std::array<std::shared_ptr<IFetchingStep>, 2>, 2>, 2> CacheFetchingScripts;
public:
    ui64 GetMemoryForSources(const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const bool isExclusive);

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    std::shared_ptr<NIndexedReader::TMergePartialStream> BuildMerger() const;

    TString DebugString() const {
        return TStringBuilder() <<
            "ef=" << EFColumns->DebugString() << ";" <<
            "pk=" << PKColumns->DebugString() << ";" <<
            "ff=" << FFColumns->DebugString() << ";" <<
            "program_input=" << ProgramInputColumns->DebugString()
            ;
    }

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext);

    std::shared_ptr<IFetchingStep> GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source, const bool exclusiveSource) const;
};

}
