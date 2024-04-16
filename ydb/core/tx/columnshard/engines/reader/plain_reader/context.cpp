#include "context.h"
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

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
    return result;
}

std::shared_ptr<NKikimr::NOlap::NPlainReader::IFetchingStep> TSpecialReadContext::GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source, const bool exclusiveSource) const {
    const bool needSnapshots = !exclusiveSource || ReadMetadata->GetSnapshot() < source->GetRecordSnapshotMax();
    auto result = CacheFetchingScripts[needSnapshots ? 1 : 0][exclusiveSource ? 1 : 0];
    if (!result) {
        return std::make_shared<TBuildFakeSpec>(source->GetRecordsCount(), "fake");
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::NPlainReader::IFetchingStep> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool exclusiveSource) const {
    std::shared_ptr<IFetchingStep> result = std::make_shared<TFakeStep>();
    std::shared_ptr<IFetchingStep> current = result;
    if (!!IndexChecker) {
        current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        current = current->AttachNext(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    if (!EFColumns->GetColumnsCount()) {
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
        if (needSnapshots || FFColumns->Contains(SpecColumns)) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        AFL_VERIFY(columnsFetch.GetColumnsCount());
        current = current->AttachNext(std::make_shared<TBlobsFetchingStep>(std::make_shared<TColumnsSet>(columnsFetch), "ef"));

        if (needSnapshots || FFColumns->Contains(SpecColumns)) {
            current = current->AttachNext(std::make_shared<TAssemblerStep>(SpecColumns));
            current = current->AttachNext(std::make_shared<TSnapshotFilter>());
            columnsFetch = columnsFetch - *SpecColumns;
        }
        current = current->AttachNext(std::make_shared<TAssemblerStep>(std::make_shared<TColumnsSet>(columnsFetch)));
        if (!ReadMetadata->GetPKRangesFilter().IsEmpty()) {
            current = current->AttachNext(std::make_shared<TPredicateFilter>());
        }
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (!i->IsFilterOnly()) {
                break;
            }
            current = current->AttachNext(std::make_shared<TFilterProgramStep>(i));
        }
        const TColumnsSet columnsAdditionalFetch = *FFColumns - *EFColumns - *SpecColumns;
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
        if (!ReadMetadata->GetPKRangesFilter().IsEmpty()) {
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
        const TColumnsSet columnsAdditionalFetch = *FFColumns - *EFColumns - *SpecColumns - *PKColumns;
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

    auto readSchema = ReadMetadata->GetLoadSchema(ReadMetadata->GetSnapshot());
    SpecColumns = std::make_shared<TColumnsSet>(TIndexInfo::GetSpecialColumnIdsSet(), ReadMetadata->GetIndexInfo(), readSchema);
    IndexChecker = ReadMetadata->GetProgram().GetIndexChecker();
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
    CacheFetchingScripts[0][0] = BuildColumnsFetchingPlan(false, false);
    CacheFetchingScripts[0][1] = BuildColumnsFetchingPlan(false, true);
    CacheFetchingScripts[1][0] = BuildColumnsFetchingPlan(true, false);
    CacheFetchingScripts[1][1] = BuildColumnsFetchingPlan(true, true);
}

}
