#include "fetching.h"
#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NPlain {

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(
        source->GetStageData().GetTable()->ToTable(source->GetContext()->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(
                                                       source->GetContext()->GetReadMetadata()->GetResultSchema()->GetIndexInfo()),
            source->GetContext()->GetCommonContext()->GetResolver(), true));
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = MakeSnapshotFilter(source->GetStageData().GetTable()->ToTable({}, source->GetContext()->GetCommonContext()->GetResolver()),
        source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    if (filter.GetFilteredCount().value_or(source->GetRecordsCount()) != source->GetRecordsCount()) {
        if (source->AddTxConflict()) {
            return true;
        }
    }
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TDeletionFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto collection = source->GetStageData().GetTable()->SelectOptional(std::vector<ui32>({ (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG }), false);
    if (!collection) {
        return true;
    }

    auto filterTable = collection->ToTable();
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
    auto filter =
        shardingInfo->GetFilter(source->GetStageData().GetTable()->ToTable({}, source->GetContext()->GetCommonContext()->GetResolver()));
    source->MutableStageData().AddFilter(filter);
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
        source->MutableStageData().GetTable()->AddVerified(IIndexInfo::GetColumnIdVerified(f->name()),
            std::make_shared<NArrow::NAccessor::TTrivialArray>(
                NArrow::TThreadSimpleArraysCache::GetConst(f->type(), NArrow::DefaultScalar(f->type()), source->GetRecordsCount())), true);
    }
    source->BuildStageResult(source);
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
