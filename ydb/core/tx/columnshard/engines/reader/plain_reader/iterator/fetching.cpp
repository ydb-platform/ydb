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
    auto executeResult = Cursor.Execute(Source);
    if (!executeResult) {
        SetErrorMessage(executeResult.GetErrorMessage());
        return false;
    }
    if (*executeResult) {
        Source->Finalize();
        FinishedFlag = true;
    }
    return true;
}

TConclusion<bool> TColumnBlobsFetchingStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingColumns(source, step, Columns);
}

ui64 TColumnBlobsFetchingStep::DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    const ui64 result = source->GetColumnRawBytes(Columns->GetColumnIds());
    if (!result) {
        return Columns->GetColumnIds().size() * source->GetRecordsCount() * sizeof(ui32); // null for all records for all columns in future will be
    } else {
        return result;
    }
}

TConclusion<bool> TIndexBlobsFetchingStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingIndexes(source, step, Indexes);
}

ui64 TIndexBlobsFetchingStep::DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    return source->GetIndexRawBytes(Indexes->GetIndexIdsSet());
}

TConclusion<bool> TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

TConclusion<bool> TOptionalAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
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

TConclusion<bool> TFilterProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    AFL_VERIFY(source);
    AFL_VERIFY(Step);
    std::shared_ptr<arrow::Table> table;
    if (source->IsSourceInMemory(Step->GetFilterOriginalColumnIds())) {
        auto filter = Step->BuildFilter(source->GetStageData().GetTable());
        if (!filter.ok()) {
            return TConclusionStatus::Fail(filter.status().message());
        }
        source->MutableStageData().AddFilter(*filter);
    }
    return true;
}

ui64 TFilterProgramStep::DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    return NArrow::TColumnFilter::GetPredictedMemorySize(source->GetRecordsCount());
}

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(source->GetStageData().GetTable()->BuildTable());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = MakeSnapshotFilter(source->GetStageData().GetTable()->BuildTable(), source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TShardingFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    auto filter = source->GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo()->GetFilter(source->GetStageData().GetTable()->BuildTable());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TBuildFakeSpec::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& f : TIndexInfo::ArrowSchemaSnapshot()->fields()) {
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::GetConst(f->type(), std::make_shared<arrow::UInt64Scalar>(0), Count));
    }
    source->MutableStageData().AddBatch(arrow::RecordBatch::Make(TIndexInfo::ArrowSchemaSnapshot(), Count, columns));
    return true;
}

TConclusion<bool> TApplyIndexStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->ApplyIndex(IndexChecker);
    return true;
}

TConclusion<bool> TFetchingScriptCursor::Execute(const std::shared_ptr<IDataSource>& source) {
    AFL_VERIFY(source);
    NMiniKQL::TThrowingBindTerminator bind;
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source->GetStageData().IsEmpty()) {
            break;
        }
        auto step = Script->GetStep(CurrentStepIdx);
        TMemoryProfileGuard mGuard("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName(), IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx);
        const TConclusion<bool> resultStep = step->ExecuteInplace(source, *this);
        if (!resultStep) {
            return resultStep;
        }
        if (!*resultStep) {
            return false;
        }
        ++CurrentStepIdx;
    }
    return true;
}

}
