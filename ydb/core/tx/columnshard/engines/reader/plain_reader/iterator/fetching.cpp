#include "fetching.h"
#include "source.h"
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

#include <ydb/library/yql/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NPlain {

bool TStepAction::DoApply(IDataReader& /*owner*/) const {
    if (FinishedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
        Source->SetIsReady();
    }
    return true;
}

bool TStepAction::DoExecute() {
    if (Source->IsAborted()) {
        return true;
    }
    NMiniKQL::TThrowingBindTerminator bind;
    while (Step) {
        if (Source->IsEmptyData()) {
            break;
        }
        TMemoryProfileGuard mGuard("SCAN_PROFILE::FETCHING::" + Step->GetName() + "::" + Step->GetBranchName(), IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        if (!Step->ExecuteInplace(Source, Step)) {
            return true;
        }
        Step = Step->GetNextStep();
    }
    Source->Finalize();
    FinishedFlag = true;
    return true;
}

bool TColumnBlobsFetchingStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const {
    return !source->StartFetchingColumns(source, step, Columns);
}

ui64 TColumnBlobsFetchingStep::DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    const ui64 result = source->GetColumnRawBytes(Columns->GetColumnIds());
    if (!result) {
        return Columns->GetColumnIds().size() * source->GetRecordsCountVerified() * sizeof(ui32); // null for all records for all columns in future will be
    } else {
        return result;
    }
}

bool TIndexBlobsFetchingStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const {
    return !source->StartFetchingIndexes(source, step, Indexes);
}

ui64 TIndexBlobsFetchingStep::DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    return source->GetIndexRawBytes(Indexes->GetIndexIdsSet());
}

bool TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

bool TOptionalAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

bool TOptionalAssemblerStep::DoInitSourceSeqColumnIds(const std::shared_ptr<IDataSource>& source) const {
    for (auto&& i : Columns->GetColumnIds()) {
        if (source->AddSequentialEntityIds(i)) {
            return true;
        }
    }
    return false;
}

bool TFilterProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    AFL_VERIFY(source);
    AFL_VERIFY(Step);
    AFL_VERIFY(source->GetStageData().GetTable());
    auto filter = Step->BuildFilter(source->GetStageData().GetTable());
    source->MutableStageData().AddFilter(filter);
    return true;
}

ui64 TFilterProgramStep::DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    return NArrow::TColumnFilter::GetPredictedMemorySize(source->GetRecordsCountOptional().value_or(0));
}

bool TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(source->GetStageData().GetTable()->BuildTable());
    source->MutableStageData().AddFilter(filter);
    return true;
}

bool TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    auto filter = MakeSnapshotFilter(source->GetStageData().GetTable()->BuildTable(), source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    source->MutableStageData().AddFilter(filter);
    return true;
}

bool TBuildFakeSpec::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& f : TIndexInfo::ArrowSchemaSnapshot()->fields()) {
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::GetConst(f->type(), std::make_shared<arrow::UInt64Scalar>(0), Count));
    }
    source->MutableStageData().AddBatch(arrow::RecordBatch::Make(TIndexInfo::ArrowSchemaSnapshot(), Count, columns));
    return true;
}

bool TApplyIndexStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    source->ApplyIndex(IndexChecker);
    return true;
}

}
