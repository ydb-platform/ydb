#include "fetching.h"
#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <yql/essentials/minikql/mkql_terminator.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NPlain {

TConclusion<TExecutionResult> TPredicateFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = source.GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(
        source.GetStageData().GetTable().ToGeneralContainer(source.GetContext()->GetCommonContext()->GetResolver(),
            source.GetContext()->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(
                source.GetContext()->GetReadMetadata()->GetResultSchema()->GetIndexInfo()), true));
    source.MutableStageData().AddFilter(filter);
    return TExecutionResult::Done();
}

TConclusion<TExecutionResult> TSnapshotFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = MakeSnapshotFilter(source.GetStageData().GetTable().ToTable({}, source.GetContext()->GetCommonContext()->GetResolver()),
        source.GetContext()->GetReadMetadata()->GetRequestSnapshot());
    if (filter.GetFilteredCount().value_or(source.GetRecordsCount()) != source.GetRecordsCount()) {
        if (source.AddTxConflict()) {
            return TExecutionResult::Done();
        }
    }
    source.MutableStageData().AddFilter(filter);
    return TExecutionResult::Done();
}

TConclusion<TExecutionResult> TDeletionFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    auto collection =
        source.GetStageData().GetTable().SelectOptional(std::vector<ui32>({ (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG }), false);
    if (!collection) {
        return TExecutionResult::Done();
    }

    auto filterTable = collection->ToTable();
    if (!filterTable) {
        return TExecutionResult::Done();
    }
    AFL_VERIFY(filterTable->column(0)->type()->id() == arrow::boolean()->id());
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& i : filterTable->column(0)->chunks()) {
        auto filterFlags = static_pointer_cast<arrow::BooleanArray>(i);
        for (ui32 i = 0; i < filterFlags->length(); ++i) {
            filter.Add(!filterFlags->GetView(i));
        }
    }
    source.MutableStageData().AddFilter(filter);
    return TExecutionResult::Done();
}

TConclusion<TExecutionResult> TShardingFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    const auto& shardingInfo = source.GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo();
    auto filter = shardingInfo->GetFilter(source.GetStageData().GetTable().ToTable({}, source.GetContext()->GetCommonContext()->GetResolver()));
    source.MutableStageData().AddFilter(filter);
    return TExecutionResult::Done();
}

TConclusion<TExecutionResult> TFilterCutLimit::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    source.MutableStageData().CutFilter(source.GetRecordsCount(), Limit, Reverse);
    return TExecutionResult::Done();
}

TConclusion<TExecutionResult> TPortionAccessorFetchingStep::DoExecuteInplace(
    NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    if (source.HasPortionAccessor()) {
        return TExecutionResult::Done();
    }
    return source.MutableAs<IDataSource>()->StartFetchingAccessor(step);
}

TConclusion<TExecutionResult> TDetectInMem::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    if (source.HasSourceInMemoryFlag()) {
        return TExecutionResult::Done();
    }
    if (Columns.GetColumnsCount()) {
        source.SetSourceInMemory(
            source.GetColumnRawBytes(Columns.GetColumnIds()) < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source.SetSourceInMemory(true);
    }
    AFL_VERIFY(!source.GetAs<IDataSource>()->NeedAccessorsFetching());
    auto plan = source.GetContext()->GetColumnsFetchingPlan(source, true);
    source.MutableAs<IDataSource>()->InitFetchingPlan(plan);
    TFetchingScriptCursor cursor(plan, 0);
    return cursor.Execute(source);
}

TConclusion<TExecutionResult> TBuildFakeSpec::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& f : IIndexInfo::ArrowSchemaSnapshot()->fields()) {
        if (source.MutableStageData().GetTable().HasColumn(IIndexInfo::GetColumnIdVerified(f->name()))) {
            auto arr = source.MutableStageData().GetTable().GetArrayVerified(IIndexInfo::GetColumnIdVerified(f->name()));
            YDB_LOG_WARN("",
                {"event", "spec_column_exists"},
                {"columnName", f->name()},
                {"col", NArrow::DebugJson(arr, 2, 2).GetStringRobust()});
        } else {
            source.MutableStageData().MutableTable().AddVerified(IIndexInfo::GetColumnIdVerified(f->name()),
                std::make_shared<NArrow::NAccessor::TTrivialArray>(
                    NArrow::TThreadSimpleArraysCache::GetConst(f->type(), NArrow::DefaultScalar(f->type()), source.GetRecordsCount())), true);
        }
    }
    return TExecutionResult::Done();
}

}   // namespace NKikimr::NOlap::NReader::NPlain
