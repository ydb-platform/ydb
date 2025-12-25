#include "merge.h"
#include "private_events.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TFiltersBuilder {
private:
    THashMap<ui64, NArrow::TColumnFilter> Filters;
    YDB_READONLY(ui64, RowsAdded, 0);
    YDB_READONLY(ui64, RowsSkipped, 0);
    bool IsDone = false;

    void AddImpl(const ui64 sourceId, const bool value) {
        auto* findFilter = Filters.FindPtr(sourceId);
        AFL_VERIFY(findFilter);
        findFilter->Add(value);
    }

public:
    void AddRecord(const NArrow::NMerger::TBatchIterator& cursor) {
        AddImpl(cursor.GetSourceId(), true);
        ++RowsAdded;
    }

    void SkipRecord(const NArrow::NMerger::TBatchIterator& cursor) {
        AddImpl(cursor.GetSourceId(), false);
        ++RowsSkipped;
    }

    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const {
    }

    bool IsBufferExhausted() const {
        return false;
    }

    THashMap<ui64, NArrow::TColumnFilter>&& ExtractFilters() && {
        AFL_VERIFY(!IsDone);
        IsDone = true;
        return std::move(Filters);
    }

    void AddSource(const ui64 sourceId) {
        AFL_VERIFY(!IsDone);
        AFL_VERIFY(Filters.emplace(sourceId, NArrow::TColumnFilter::BuildAllowFilter()).second);
    }
};

void TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("task", "build_duplicate_filters")("info", DebugString());
    auto columnData = ColumnData.ExtractDataByPortion(Context.GetColumns());

    THashMap<TDuplicateMapInfo, NArrow::TColumnFilter> filters;
    for (const auto& [begin, end] : Context.GetIntervals()) {
        for (auto&& [portionId, filter] : BuildFiltersOnInterval(begin, end, columnData)) {
            AFL_VERIFY(filters.emplace(TDuplicateMapInfo(Context.GetContext()->GetRequest()->Get()->GetMaxVersion(), TIntervalBordersView(begin.MakeView(), end.MakeView()), portionId), std::move(filter)).second);
        }
    }
    AFL_VERIFY(filters.size() == Context.GetRequiredPortions().size() * Context.GetIntervals().size())("filters", filters.size())(
                                                                        "portions", Context.GetRequiredPortions().size())(
                                                                        "intervals", Context.GetIntervals().size());

    TActivationContext::AsActorContext().Send(
        Context.GetOwner(), new NPrivate::TEvFilterConstructionResult(std::move(filters)));
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    TActivationContext::AsActorContext().Send(
        Context.GetOwner(), new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(reason)));
}

THashMap<ui64, NArrow::TColumnFilter> TBuildDuplicateFilters::BuildFiltersOnInterval(const TColumnDataSplitter::TBorder& begin,
    const TColumnDataSplitter::TBorder& end, const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& columnData) {
    NArrow::NMerger::TMergePartialStream merger(
        Context.GetPKSchema(), nullptr, false, GetVersionColumnNames(), ScanSnapshotBatch, MinUncommittedSnapshotBatch);
    merger.PutControlPoint(*end.GetKey(), false);
    TFiltersBuilder filtersBuilder;
    THashMap<ui64, TRowRange> nonEmptySources;
    for (const auto& [portionId, data] : columnData) {
        auto position = NArrow::NMerger::TRWSortableBatchPosition(data, 0, Context.GetPKSchema()->field_names(), {}, false);

        auto findOffset = [&](const TColumnDataSplitter::TBorder& border, const ui64 beginOffset) {
            auto findBound = NArrow::NMerger::TSortableBatchPosition::FindBound(
                position, beginOffset, data->GetRecordsCount() - 1, *border.GetKey(), border.GetIsLast());
            return findBound ? findBound->GetPosition() : data->GetRecordsCount();
        };

        ui64 startOffset = findOffset(begin, 0);
        ui64 endOffsetExclusive = (startOffset != data->GetRecordsCount() ? findOffset(end, startOffset) : data->GetRecordsCount());

        merger.AddSource(data, nullptr, NArrow::NMerger::TIterationOrder::Forward(startOffset), portionId);
        filtersBuilder.AddSource(portionId);
        if (startOffset != endOffsetExclusive) {
            nonEmptySources.emplace(portionId, TRowRange(startOffset, endOffsetExclusive));
        }
    }
    if (nonEmptySources.size() <= 1) {
        THashMap<ui64, NArrow::TColumnFilter> result;
        for (const auto& [portionId, _] : columnData) {
            result.emplace(portionId, NArrow::TColumnFilter::BuildAllowFilter());
        }
        if (nonEmptySources.size() == 1) {
            NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
            filter.Add(true, nonEmptySources.begin()->second.NumRows());
            result.insert_or_assign(nonEmptySources.begin()->first, std::move(filter));
            Context.GetCounters()->OnRowsMerged(0, 0, nonEmptySources.begin()->second.NumRows());
        }
        return result;
    }
    merger.DrainToControlPoint(filtersBuilder, end.GetIsLast());
    Context.GetCounters()->OnRowsMerged(filtersBuilder.GetRowsAdded(), filtersBuilder.GetRowsSkipped(), 0);
    return std::move(filtersBuilder).ExtractFilters();
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
