#include "context.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<NArrow::NMerger::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_unique<NArrow::NMerger::TMergePartialStream>(ReadMetadata->GetReplaceKey(), ProgramInputColumns->GetSchema(), CommonContext->IsReverse(), IIndexInfo::GetSpecialColumnNames());
}

ui64 TSpecialReadContext::GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources, const bool isExclusive) {
    ui64 result = 0;
    bool hasSequentialReadSources = false;
    for (auto&& i : sources) {
        auto fetchingPlan = GetColumnsFetchingPlan(i.second);
        AFL_VERIFY(i.second->GetIntervalsCount());
        const ui64 sourceMemory = std::max<ui64>(1, fetchingPlan->PredictRawBytes(i.second) / i.second->GetIntervalsCount());
        if (!i.second->IsSourceInMemory()) {
            hasSequentialReadSources = true;
        }
        result += sourceMemory;
    }
    AFL_VERIFY(result);
    if (hasSequentialReadSources) {
        result += ReadSequentiallyBufferSize;
    } else {
        if (!isExclusive && !CommonContext->IsReverse()) {
            result = 2 * result; // due to in time we will have data in original portion + data in merged(or reversed) interval
        }
    }
    return result;
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) const {
    const bool needSnapshots = !source->GetExclusiveIntervalOnly() || ReadMetadata->GetRequestSnapshot() < source->GetRecordSnapshotMax() || !source->IsSourceInMemory();
    const bool partialUsageByPK = ReadMetadata->GetPKRangesFilter().IsPortionInPartialUsage(source->GetStartReplaceKey(), source->GetFinishReplaceKey(), ReadMetadata->GetIndexInfo());
    const bool useIndexes = (IndexChecker ? source->HasIndexes(IndexChecker->GetIndexIds()) : false);
    if (auto result = CacheFetchingScripts[needSnapshots ? 1 : 0][(source->GetExclusiveIntervalOnly() && source->IsSourceInMemory()) ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0]) {
        return result;
    }
    {
        std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>();
        result->SetBranchName("FAKE");
        result->AddStep(std::make_shared<TBuildFakeSpec>(source->GetRecordsCount()));
        return result;
    }
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool exclusiveSource,
    const bool partialUsageByPredicateExt, const bool useIndexes) const {
    std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>();
    const bool partialUsageByPredicate = partialUsageByPredicateExt && PredicateColumns->GetColumnsCount();
    if (!!IndexChecker && useIndexes) {
        result->AddStep(std::make_shared<TIndexBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        result->AddStep(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    if (!EFColumns->GetColumnsCount() && !partialUsageByPredicate) {
        result->SetBranchName("simple");
        TColumnsSet columnsFetch = *FFColumns;
        if (needSnapshots) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        if (!exclusiveSource) {
            columnsFetch = columnsFetch + *PKColumns + *SpecColumns;
        } else {
            if (columnsFetch.GetColumnsCount() == 1 && SpecColumns->Contains(columnsFetch)) {
                return nullptr;
            }
        }
        if (columnsFetch.GetColumnsCount()) {
            result->AddStep(std::make_shared<TColumnBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch)));
            if (!exclusiveSource) {
                result->AddStep(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(*PKColumns + *SpecColumns), "LAST"));
                auto additional = columnsFetch - (*PKColumns + *SpecColumns);
                if (!additional.IsEmpty()) {
                    result->AddStep(std::make_shared<TOptionalAssemblerStep>(std::make_shared<TColumnsSet>(columnsFetch - (*PKColumns + *SpecColumns)), "LAST"));
                }
            } else {
                result->AddStep(std::make_shared<TOptionalAssemblerStep>(std::make_shared<TColumnsSet>(columnsFetch), "LAST"));
            }
        } else {
            return nullptr;
        }
    } else if (exclusiveSource) {
        result->SetBranchName("exclusive");
        TColumnsSet columnsFetch = *EFColumns;
        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        if (partialUsageByPredicate) {
            columnsFetch = columnsFetch + *PredicateColumns;
        }
        AFL_VERIFY(columnsFetch.GetColumnsCount());
        result->AddStep(std::make_shared<TColumnBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch)));

        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            result->AddStep(std::make_shared<TAssemblerStep>(SpecColumns, "SPEC"));
            result->AddStep(std::make_shared<TSnapshotFilter>());
            columnsFetch = columnsFetch - *SpecColumns;
        }
        if (partialUsageByPredicate) {
            result->AddStep(std::make_shared<TAssemblerStep>(PredicateColumns, "PREDICATE"));
            result->AddStep(std::make_shared<TPredicateFilter>());
            columnsFetch = columnsFetch - *PredicateColumns;
        }
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (!i->IsFilterOnly()) {
                break;
            }
            TColumnsSet stepColumnIds(i->GetFilterOriginalColumnIds(), ReadMetadata->GetIndexInfo(), ReadMetadata->GetResultSchema());
            {
                auto intersectionIds = columnsFetch.Intersect(stepColumnIds);
                if (intersectionIds.size()) {
                    TColumnsSet intersection(intersectionIds, ReadMetadata->GetIndexInfo(), ReadMetadata->GetResultSchema());
                    result->AddStep(std::make_shared<TOptionalAssemblerStep>(std::make_shared<TColumnsSet>(intersection), "EF"));
                    columnsFetch = columnsFetch - intersection;
                }
            }
            result->AddStep(std::make_shared<TFilterProgramStep>(i));
        }
        AFL_VERIFY(columnsFetch.IsEmpty());
        TColumnsSet columnsAdditionalFetch = *FFColumns - *EFColumns - *SpecColumns;
        if (partialUsageByPredicate) {
            columnsAdditionalFetch = columnsAdditionalFetch - *PredicateColumns;
        }
        if (columnsAdditionalFetch.GetColumnsCount()) {
            result->AddStep(std::make_shared<TColumnBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch)));
            result->AddStep(std::make_shared<TOptionalAssemblerStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch), "LAST"));
        }
    } else {
        result->SetBranchName("merge");
        TColumnsSet columnsFetch = *MergeColumns + *EFColumns;
        AFL_VERIFY(columnsFetch.GetColumnsCount());
        result->AddStep(std::make_shared<TColumnBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch)));
        result->AddStep(std::make_shared<TAssemblerStep>(SpecColumns, "SPEC"));
        if (needSnapshots) {
            result->AddStep(std::make_shared<TSnapshotFilter>());
        }
        result->AddStep(std::make_shared<TAssemblerStep>(PKColumns, "PK"));
        if (partialUsageByPredicate) {
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        TColumnsSet columnsFetchEF = columnsFetch - *SpecColumns - *PKColumns;
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (!i->IsFilterOnly()) {
                break;
            }
            TColumnsSet stepColumnIds(i->GetFilterOriginalColumnIds(), ReadMetadata->GetIndexInfo(), ReadMetadata->GetResultSchema());
            {
                auto intersectionIds = columnsFetchEF.Intersect(stepColumnIds);
                if (intersectionIds.size()) {
                    TColumnsSet intersection(intersectionIds, ReadMetadata->GetIndexInfo(), ReadMetadata->GetResultSchema());
                    result->AddStep(std::make_shared<TOptionalAssemblerStep>(std::make_shared<TColumnsSet>(intersection), "EF"));
                    columnsFetchEF = columnsFetchEF - intersection;
                }
            }
            result->AddStep(std::make_shared<TFilterProgramStep>(i));
        }
        AFL_VERIFY(columnsFetchEF.IsEmpty());
        const TColumnsSet columnsAdditionalFetch = *FFColumns - *EFColumns - *SpecColumns - *PKColumns - *PredicateColumns;
        if (columnsAdditionalFetch.GetColumnsCount()) {
            result->AddStep(std::make_shared<TColumnBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch)));
            result->AddStep(std::make_shared<TOptionalAssemblerStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch), "LAST"));
        }
    }
    return result;
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext)
{
    ReadMetadata = dynamic_pointer_cast<const TReadMetadata>(CommonContext->GetReadMetadata());
    Y_ABORT_UNLESS(ReadMetadata);
    Y_ABORT_UNLESS(ReadMetadata->SelectInfo);

    auto readSchema = ReadMetadata->GetResultSchema();
    SpecColumns = std::make_shared<TColumnsSet>(TIndexInfo::GetSpecialColumnIdsSet(), ReadMetadata->GetIndexInfo(), readSchema);
    IndexChecker = ReadMetadata->GetProgram().GetIndexChecker();
    {
        auto predicateColumns = ReadMetadata->GetPKRangesFilter().GetColumnIds(ReadMetadata->GetIndexInfo());
        if (predicateColumns.size()) {
            PredicateColumns = std::make_shared<TColumnsSet>(predicateColumns, ReadMetadata->GetIndexInfo(), readSchema);
        } else {
            PredicateColumns = std::make_shared<TColumnsSet>();
        }
    }
    {
        auto efColumns = ReadMetadata->GetEarlyFilterColumnIds();
        if (efColumns.size()) {
            EFColumns = std::make_shared<TColumnsSet>(efColumns, ReadMetadata->GetIndexInfo(), readSchema);
        } else {
            EFColumns = std::make_shared<TColumnsSet>();
        }
    }
    if (ReadMetadata->HasProcessingColumnIds()) {
        FFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetProcessingColumnIds(), ReadMetadata->GetIndexInfo(), readSchema);
        if (SpecColumns->Contains(*FFColumns) && !EFColumns->IsEmpty()) {
            FFColumns = std::make_shared<TColumnsSet>(*EFColumns + *SpecColumns);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("ff_modified", FFColumns->DebugString());
        } else {
            AFL_VERIFY(!FFColumns->Contains(*SpecColumns))("info", FFColumns->DebugString());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("ff_first", FFColumns->DebugString());
        }
    } else {
        FFColumns = EFColumns;
    }
    if (FFColumns->IsEmpty()) {
        ProgramInputColumns = SpecColumns;
    } else {
        ProgramInputColumns = FFColumns;
    }

    PKColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetPKColumnIds(), ReadMetadata->GetIndexInfo(), readSchema);
    MergeColumns = std::make_shared<TColumnsSet>(*PKColumns + *SpecColumns);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("columns_context_info", DebugString());
    CacheFetchingScripts[0][0][0][0] = BuildColumnsFetchingPlan(false, false, false, false);
    CacheFetchingScripts[0][1][0][0] = BuildColumnsFetchingPlan(false, true, false, false);
    CacheFetchingScripts[1][0][0][0] = BuildColumnsFetchingPlan(true, false, false, false);
    CacheFetchingScripts[1][1][0][0] = BuildColumnsFetchingPlan(true, true, false, false);
    CacheFetchingScripts[0][0][1][0] = BuildColumnsFetchingPlan(false, false, true, false);
    CacheFetchingScripts[0][1][1][0] = BuildColumnsFetchingPlan(false, true, true, false);
    CacheFetchingScripts[1][0][1][0] = BuildColumnsFetchingPlan(true, false, true, false);
    CacheFetchingScripts[1][1][1][0] = BuildColumnsFetchingPlan(true, true, true, false);

    CacheFetchingScripts[0][0][0][1] = BuildColumnsFetchingPlan(false, false, false, true);
    CacheFetchingScripts[0][1][0][1] = BuildColumnsFetchingPlan(false, true, false, true);
    CacheFetchingScripts[1][0][0][1] = BuildColumnsFetchingPlan(true, false, false, true);
    CacheFetchingScripts[1][1][0][1] = BuildColumnsFetchingPlan(true, true, false, true);
    CacheFetchingScripts[0][0][1][1] = BuildColumnsFetchingPlan(false, false, true, true);
    CacheFetchingScripts[0][1][1][1] = BuildColumnsFetchingPlan(false, true, true, true);
    CacheFetchingScripts[1][0][1][1] = BuildColumnsFetchingPlan(true, false, true, true);
    CacheFetchingScripts[1][1][1][1] = BuildColumnsFetchingPlan(true, true, true, true);
}

}
