#include "context.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NPlain {

std::shared_ptr<NKikimr::NOlap::NIndexedReader::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_shared<NIndexedReader::TMergePartialStream>(ReadMetadata->GetReplaceKey(), ProgramInputColumns->GetSchema(), CommonContext->IsReverse());
}

ui64 TSpecialReadContext::GetMemoryForSources(const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const bool isExclusive) {
    ui64 result = 0;
    for (auto&& i : sources) {
        auto fetchingPlan = GetColumnsFetchingPlan(i.second, isExclusive);
        AFL_VERIFY(i.second->GetIntervalsCount());
        result += fetchingPlan->PredictRawBytes(i.second) / i.second->GetIntervalsCount();
    }
    AFL_VERIFY(result);
    return result;
}

std::shared_ptr<NKikimr::NOlap::NReader::NPlain::IFetchingStep> TSpecialReadContext::GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source, const bool exclusiveSource) const {
    const bool needSnapshots = !exclusiveSource || ReadMetadata->GetRequestSnapshot() < source->GetRecordSnapshotMax();
    const bool partialUsageByPK = ReadMetadata->GetPKRangesFilter().IsPortionInPartialUsage(source->GetStartReplaceKey(), source->GetFinishReplaceKey(), ReadMetadata->GetIndexInfo());
    const bool useIndexes = (IndexChecker ? source->HasIndexes(IndexChecker->GetIndexIds()) : false);
    auto result = CacheFetchingScripts[needSnapshots ? 1 : 0][exclusiveSource ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0];
    if (!result) {
        return std::make_shared<TBuildFakeSpec>(source->GetRecordsCount(), "fake");
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::NReader::NPlain::IFetchingStep> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool exclusiveSource, const bool partialUsageByPredicateExt, const bool useIndexes) const {
    std::shared_ptr<IFetchingStep> result = std::make_shared<TFakeStep>();
    std::shared_ptr<IFetchingStep> current = result;
    const bool partialUsageByPredicate = partialUsageByPredicateExt && PredicateColumns->GetColumnsCount();
    if (!!IndexChecker && useIndexes) {
        current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        current = current->AttachNext(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    if (!EFColumns->GetColumnsCount() && !partialUsageByPredicate) {
        TColumnsSet columnsFetch = *FFColumns;
        if (needSnapshots) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        if (!exclusiveSource) {
            columnsFetch = columnsFetch + *PKColumns + *SpecColumns;
        }
        if (columnsFetch.GetColumnsCount()) {
            current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch), "simple"));
            current = current->AttachNext(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(columnsFetch)));
        } else {
            return nullptr;
        }
    } else if (exclusiveSource) {
        TColumnsSet columnsFetch = *EFColumns;
        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        if (partialUsageByPredicate) {
            columnsFetch = columnsFetch + *PredicateColumns;
        }
        AFL_VERIFY(columnsFetch.GetColumnsCount());
        current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch), "ef"));

        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            current = current->AttachNext(std::make_shared<TAssemblerStep>(SpecColumns));
            current = current->AttachNext(std::make_shared<TSnapshotFilter>());
            columnsFetch = columnsFetch - *SpecColumns;
        }
        if (partialUsageByPredicate) {
            current = current->AttachNext(std::make_shared<TAssemblerStep>(PredicateColumns));
            current = current->AttachNext(std::make_shared<TPredicateFilter>());
            columnsFetch = columnsFetch - *PredicateColumns;
        }
        if (columnsFetch.GetColumnsCount()) {
            current = current->AttachNext(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(columnsFetch)));
        }
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (!i->IsFilterOnly()) {
                break;
            }
            current = current->AttachNext(std::make_shared<TFilterProgramStep>(i));
        }
        TColumnsSet columnsAdditionalFetch = *FFColumns - *EFColumns - *SpecColumns;
        if (partialUsageByPredicate) {
            columnsAdditionalFetch = columnsAdditionalFetch - *PredicateColumns;
        }
        if (columnsAdditionalFetch.GetColumnsCount()) {
            current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch)));
            current = current->AttachNext(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch)));
        }
    } else {
        TColumnsSet columnsFetch = *MergeColumns + *EFColumns;
        AFL_VERIFY(columnsFetch.GetColumnsCount());
        current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch), "full"));
        current = current->AttachNext(std::make_shared<TAssemblerStep>(SpecColumns));
        if (needSnapshots) {
            current = current->AttachNext(std::make_shared<TSnapshotFilter>());
        }
        current = current->AttachNext(std::make_shared<TAssemblerStep>(PKColumns));
        if (partialUsageByPredicate) {
            current = current->AttachNext(std::make_shared<TPredicateFilter>());
        }
        const TColumnsSet columnsFetchEF = columnsFetch - *SpecColumns - *PKColumns;
        current = current->AttachNext(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(columnsFetchEF)));
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (!i->IsFilterOnly()) {
                break;
            }
            current = current->AttachNext(std::make_shared<TFilterProgramStep>(i));
        }
        const TColumnsSet columnsAdditionalFetch = *FFColumns - *EFColumns - *SpecColumns - *PKColumns - *PredicateColumns;
        if (columnsAdditionalFetch.GetColumnsCount()) {
            current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch)));
            current = current->AttachNext(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(columnsAdditionalFetch)));
        }
    }
    return result->GetNextStep();
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext)
{
    ReadMetadata = dynamic_pointer_cast<const TReadMetadata>(CommonContext->GetReadMetadata());
    Y_ABORT_UNLESS(ReadMetadata);
    Y_ABORT_UNLESS(ReadMetadata->SelectInfo);

    auto readSchema = ReadMetadata->GetLoadSchema(ReadMetadata->GetRequestSnapshot());
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
