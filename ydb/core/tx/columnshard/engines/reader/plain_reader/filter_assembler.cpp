#include "filter_assembler.h"
#include "plain_read_data.h"
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/filter.h>

namespace NKikimr::NOlap::NPlainReader {

class TFilterContext {
private:
    TPortionInfo::TPreparedBatchData BatchAssembler;
    YDB_ACCESSOR_DEF(std::shared_ptr<TSpecialReadContext>, Context);
    YDB_ACCESSOR_DEF(std::set<ui32>, ReadyColumnIds);
    std::optional<ui32> OriginalCount;
    YDB_READONLY(std::shared_ptr<NArrow::TColumnFilter>, AppliedFilter, std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter()));
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, EarlyFilter);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Table>, ResultTable);
public:
    TFilterContext(TPortionInfo::TPreparedBatchData&& batchAssembler, const std::shared_ptr<TSpecialReadContext>& context)
        : BatchAssembler(std::move(batchAssembler))
        , Context(context)
    {

    }

    ui32 GetOriginalCountVerified() const {
        AFL_VERIFY(OriginalCount);
        return *OriginalCount;
    }

    std::shared_ptr<arrow::Table> AppendToResult(const std::set<ui32>& columnIds, const std::optional<std::shared_ptr<arrow::Scalar>> constantFill = {}) {
        TPortionInfo::TPreparedBatchData::TAssembleOptions options;
        options.IncludedColumnIds = columnIds;
        for (auto it = options.IncludedColumnIds->begin(); it != options.IncludedColumnIds->end();) {
            if (!ReadyColumnIds.emplace(*it).second) {
                it = options.IncludedColumnIds->erase(it);
            } else {
                ++it;
            }
        }
        if (options.IncludedColumnIds->empty()) {
            AFL_VERIFY(ResultTable);
            return ResultTable;
        }
        if (constantFill) {
            for (auto&& i : *options.IncludedColumnIds) {
                options.ConstantColumnIds.emplace(i, *constantFill);
            }
        }
        std::shared_ptr<arrow::Table> table = BatchAssembler.AssembleTable(options);
        AFL_VERIFY(table);
        if (!OriginalCount) {
            OriginalCount = table->num_rows();
        }
        AFL_VERIFY(AppliedFilter->Apply(table));
        if (!ResultTable) {
            ResultTable = table;
        } else {
            AFL_VERIFY(NArrow::MergeBatchColumns({ResultTable, table}, ResultTable));
        }
        return ResultTable;
    }

    void ApplyFilter(const std::shared_ptr<NArrow::TColumnFilter>& filter, const bool useFilter) {
        if (useFilter) {
            filter->Apply(ResultTable);
            *AppliedFilter = AppliedFilter->CombineSequentialAnd(*filter);
        } else if (EarlyFilter) {
            *EarlyFilter = EarlyFilter->And(*filter);
        } else {
            EarlyFilter = filter;
        }
    }
};

class IFilterConstructor {
private:
    const std::set<ui32> ColumnIds;
    const std::optional<std::shared_ptr<arrow::Scalar>> ConstantFill;
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> BuildFilter(const TFilterContext& filterContext, const std::shared_ptr<arrow::Table>& data) const = 0;
public:
    IFilterConstructor(const std::set<ui32>& columnIds, const std::optional<std::shared_ptr<arrow::Scalar>> constantFill = {})
        : ColumnIds(columnIds)
        , ConstantFill(constantFill)
    {
        AFL_VERIFY(ColumnIds.size());
    }

    virtual ~IFilterConstructor() = default;

    void Execute(TFilterContext& filterContext, const bool useFilter) {
        auto result = filterContext.AppendToResult(ColumnIds, ConstantFill);
        auto localFilter = BuildFilter(filterContext, result);
        AFL_VERIFY(!!localFilter);
        filterContext.ApplyFilter(localFilter, useFilter);
    }
};

class TPredicateFilter: public IFilterConstructor {
private:
    using TBase = IFilterConstructor;
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> BuildFilter(const TFilterContext& filterContext, const std::shared_ptr<arrow::Table>& data) const override {
        return std::make_shared<NArrow::TColumnFilter>(filterContext.GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(data));
    }
public:
    TPredicateFilter(const std::shared_ptr<TSpecialReadContext>& ctx)
        : TBase(ctx->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(ctx->GetReadMetadata()->GetIndexInfo())) {

    }
};

class TRestoreMergeData: public IFilterConstructor {
private:
    using TBase = IFilterConstructor;
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> BuildFilter(const TFilterContext& /*filterContext*/, const std::shared_ptr<arrow::Table>& /*data*/) const override {
        return std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter());
    }
public:
    TRestoreMergeData(const std::shared_ptr<TSpecialReadContext>& ctx)
        : TBase(ctx->GetMergeColumns()->GetColumnIds()) {

    }
};

class TFakeSnapshotData: public IFilterConstructor {
private:
    using TBase = IFilterConstructor;
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> BuildFilter(const TFilterContext& /*filterContext*/, const std::shared_ptr<arrow::Table>& /*data*/) const override {
        return std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter());
    }
public:
    TFakeSnapshotData(const std::shared_ptr<TSpecialReadContext>& ctx)
        : TBase(ctx->GetSpecColumns()->GetColumnIds(), std::make_shared<arrow::UInt64Scalar>(0)) {

    }
};

class TSnapshotFilter: public IFilterConstructor {
private:
    using TBase = IFilterConstructor;
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> BuildFilter(const TFilterContext& filterContext, const std::shared_ptr<arrow::Table>& data) const override {
        return std::make_shared<NArrow::TColumnFilter>(MakeSnapshotFilter(data, filterContext.GetContext()->GetReadMetadata()->GetSnapshot()));
    }
public:
    TSnapshotFilter(const std::shared_ptr<TSpecialReadContext>& ctx)
        : TBase(ctx->GetSpecColumns()->GetColumnIds()) {

    }
};

class TProgramStepFilter: public IFilterConstructor {
private:
    using TBase = IFilterConstructor;
    std::shared_ptr<NSsa::TProgramStep> Step;
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> BuildFilter(const TFilterContext& /*filterContext*/, const std::shared_ptr<arrow::Table>& data) const override {
        return Step->BuildFilter(data);
    }
public:
    TProgramStepFilter(const std::shared_ptr<NSsa::TProgramStep>& step)
        : TBase(step->GetFilterOriginalColumnIds())
        , Step(step)
    {
    }
};

bool TAssembleFilter::DoExecute() {
    std::vector<std::shared_ptr<IFilterConstructor>> filters;
    if (!UseFilter) {
        filters.emplace_back(std::make_shared<TRestoreMergeData>(Context));
    }
    if (FilterColumns->GetColumnIds().contains((ui32)TIndexInfo::ESpecialColumn::PLAN_STEP)) {
        const bool needSnapshotsFilter = ReadMetadata->GetSnapshot() < RecordsMaxSnapshot;
        if (needSnapshotsFilter) {
            filters.emplace_back(std::make_shared<TSnapshotFilter>(Context));
        } else {
            filters.emplace_back(std::make_shared<TFakeSnapshotData>(Context));
        }
    }
    if (!ReadMetadata->GetPKRangesFilter().IsEmpty()) {
        filters.emplace_back(std::make_shared<TPredicateFilter>(Context));
    }

    for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
        if (!i->IsFilterOnly()) {
            break;
        }

        filters.emplace_back(std::make_shared<TProgramStepFilter>(i));
    }

    TFilterContext filterContext(BuildBatchConstructor(FilterColumns->GetFilteredSchemaVerified()), Context);

    for (auto&& f : filters) {
        f->Execute(filterContext, UseFilter);
        AFL_VERIFY(filterContext.GetResultTable());
        if (filterContext.GetResultTable()->num_rows() == 0 || (filterContext.GetEarlyFilter() && filterContext.GetEarlyFilter()->IsTotalDenyFilter())) {
            break;
        }
    }

    AppliedFilter = filterContext.GetAppliedFilter();
    EarlyFilter = filterContext.GetEarlyFilter();

    auto batch = filterContext.GetResultTable();
    if (!batch || batch->num_rows() == 0) {
        return true;
    }

    OriginalCount = filterContext.GetOriginalCountVerified();
    auto fullTable = filterContext.AppendToResult(FilterColumns->GetColumnIds());
    AFL_VERIFY(AppliedFilter->IsTotalAllowFilter() || AppliedFilter->Size() == OriginalCount)("original", OriginalCount)("af_count", AppliedFilter->Size());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "not_skip_data")
        ("original_count", OriginalCount)("filtered_count", batch->num_rows())("use_filter", UseFilter)
        ("filter_columns", FilterColumns->GetColumnIds().size())("af_count", AppliedFilter->Size())("ef_count", EarlyFilter ? EarlyFilter->Size() : 0);

    FilteredBatch = NArrow::ToBatch(fullTable, true);
    return true;
}

bool TAssembleFilter::DoApply(IDataReader& /*owner*/) const {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
    Source->InitFilterStageData(AppliedFilter, EarlyFilter, FilteredBatch, Source);
    return true;
}

}
