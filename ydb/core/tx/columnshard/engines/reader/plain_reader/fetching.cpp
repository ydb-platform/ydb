#include "fetching.h"
#include "source.h"
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NPlainReader {

bool TStepAction::DoApply(IDataReader& /*owner*/) const {
    if (FinishedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
        Source->SetIsReady();
    }
    return true;
}

bool TStepAction::DoExecute() {
    while (Step) {
        if (!Step->ExecuteInplace(Source, Step)) {
            return true;
        }
        if (Source->IsEmptyData()) {
            FinishedFlag = true;
            return true;
        }
        Step = Step->GetNextStep();
    }
    FinishedFlag = true;
    return true;
}

bool TBlobsFetchingStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const {
    return !source->StartFetchingColumns(source, step, Columns);
}

ui64 TBlobsFetchingStep::PredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    return source->GetRawBytes(Columns->GetColumnIds());
}

bool TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

bool TFilterProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    auto filter = Step->BuildFilter(source->GetStageData().GetTable());
    source->MutableStageData().AddFilter(filter);
    return true;
}

bool TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(source->GetStageData().GetTable());
    source->MutableStageData().AddFilter(filter);
    return true;
}

bool TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    auto filter = MakeSnapshotFilter(source->GetStageData().GetTable(), source->GetContext()->GetReadMetadata()->GetSnapshot());
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

}
