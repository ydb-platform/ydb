#include "fetching.h"
#include "source.h"

#include <ydb/library/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/yql/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NPlain {

TConclusion<bool> TIndexBlobsFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingIndexes(source, step, Indexes);
}

TConclusion<bool> TFilterProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    AFL_VERIFY(source);
    AFL_VERIFY(Step);
    auto filter = Step->BuildFilter(source->GetStageData().GetTable());
    if (!filter.ok()) {
        return TConclusionStatus::Fail(filter.status().message());
    }
    source->MutableStageData().AddFilter(*filter);
    return true;
}

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter =
        source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(source->GetStageData().GetTable()->BuildTableVerified());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = MakeSnapshotFilter(
        source->GetStageData().GetTable()->BuildTableVerified(), source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    if (filter.GetFilteredCount().value_or(source->GetRecordsCount()) != source->GetRecordsCount()) {
        if (source->AddTxConflict()) {
            return true;
        }
    }
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TDeletionFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filterTable = source->GetStageData().GetTable()->BuildTableOptional(std::set<std::string>({ TIndexInfo::SPEC_COL_DELETE_FLAG }));
    if (!filterTable) {
        return true;
    }
    AFL_VERIFY(filterTable->column(0)->type()->id() == arrow::boolean()->id());
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& i : filterTable->column(0)->chunks()) {
        auto filterFlags = static_pointer_cast<arrow::BooleanArray>(i);
        for (ui32 i = 0; i < filterFlags->length(); ++i) {
            filter.Add(!filterFlags->GetView(i));
        }
    }
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TShardingFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    const auto& shardingInfo = source->GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo();
    auto filter = shardingInfo->GetFilter(source->GetStageData().GetTable()->BuildTableVerified());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TApplyIndexStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->ApplyIndex(IndexChecker);
    return true;
}

NKikimr::TConclusion<bool> TFilterCutLimit::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->MutableStageData().CutFilter(source->GetRecordsCount(), Limit, Reverse);
    return true;
}

TConclusion<bool> TPortionAccessorFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingAccessor(source, step);
}

TConclusion<bool> TDetectInMem::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    if (Columns.GetColumnsCount()) {
        source->SetSourceInMemory(
            source->GetColumnRawBytes(Columns.GetColumnIds()) < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source->SetSourceInMemory(true);
    }
    AFL_VERIFY(!source->NeedAccessorsFetching());
    auto plan = source->GetContext()->GetColumnsFetchingPlan(source);
    source->InitFetchingPlan(plan);
    TFetchingScriptCursor cursor(plan, 0);
    auto task = std::make_shared<TStepAction>(source, std::move(cursor), source->GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    return false;
}

TConclusion<bool> TBuildFakeSpec::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& f : IIndexInfo::ArrowSchemaSnapshot()->fields()) {
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::GetConst(f->type(), NArrow::DefaultScalar(f->type()), source->GetRecordsCount()));
    }
    source->MutableStageData().AddBatch(std::make_shared<NArrow::TGeneralContainer>(
        arrow::RecordBatch::Make(TIndexInfo::ArrowSchemaSnapshot(), source->GetRecordsCount(), columns)));
    source->BuildStageResult(source);
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
