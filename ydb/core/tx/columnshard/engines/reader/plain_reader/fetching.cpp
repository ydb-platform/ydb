#include "fetching.h"
#include "source.h"
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

#include <ydb/library/yql/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NPlainReader {

bool TStepAction::DoApply(IDataReader& /*owner*/) const {
    if (FinishedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
        Source->SetIsReady();
    }
    return true;
}

bool TStepAction::DoExecute() {
    NMiniKQL::TThrowingBindTerminator bind;
    while (Step) {
        if (Source->IsEmptyData()) {
            Source->Finalize();
            FinishedFlag = true;
            return true;
        }
        if (!Step->ExecuteInplace(Source, Step)) {
            return true;
        }
        if (Source->IsEmptyData()) {
            Source->Finalize();
            FinishedFlag = true;
            return true;
        }
        Step = Step->GetNextStep();
    }
    Source->Finalize();
    FinishedFlag = true;
    return true;
}

bool TBlobsFetchingStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const {
    AFL_VERIFY((!!Columns) ^ (!!Indexes));

    const bool startFetchingColumns = Columns ? source->StartFetchingColumns(source, step, Columns) : false;
    const bool startFetchingIndexes = Indexes ? source->StartFetchingIndexes(source, step, Indexes) : false;
    return !startFetchingColumns && !startFetchingIndexes;
}

ui64 TBlobsFetchingStep::PredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
    if (Columns) {
        return source->GetRawBytes(Columns->GetColumnIds());
    } else {
        AFL_VERIFY(Indexes);
        return source->GetIndexBytes(Indexes->GetIndexIdsSet());
    }
}

bool TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

bool TFilterProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    AFL_VERIFY(source);
    AFL_VERIFY(Step);
    AFL_VERIFY(source->GetStageData().GetTable());
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

bool TApplyIndexStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const {
    source->ApplyIndex(IndexChecker);
    return true;
}

}
