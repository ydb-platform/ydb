#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TSyncPointLimitControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;

    const ui32 Limit;
    ui32 FetchedCount = 0;

    class TSourceIterator {
    private:
        std::shared_ptr<IDataSource> Source;
        bool Reverse;
        int Delta = 0;
        i64 Start = 0;
        i64 Finish = 0;
        std::shared_ptr<NArrow::NMerger::TRWSortableBatchPosition> SortableRecord;
        std::shared_ptr<NArrow::TColumnFilter> Filter;
        std::shared_ptr<NArrow::TColumnFilter::TIterator> FilterIterator;
        bool IsValidFlag = true;

        bool ShiftWithFilter() const {
            AFL_VERIFY(IsValidFlag);
            while (!FilterIterator->GetCurrentAcceptance()) {
                if (!FilterIterator->Next(1)) {
                    AFL_VERIFY(!SortableRecord->NextPosition(Delta));
                    return false;
                } else {
                    AFL_VERIFY(SortableRecord->NextPosition(Delta));
                }
            }
            return true;
        }

        bool StartedWithLimit = false;

    public:
        const std::shared_ptr<IDataSource>& GetSource() const {
            AFL_VERIFY(Source);
            return Source;
        }

        bool StartWithLimit() {
            if (!StartedWithLimit) {
                Source->MarkAsStartedInLimit();
                StartedWithLimit = true;
                Source->ContinueCursor(Source);
                return true;
            } else {
                return false;
            }
        }

        TSourceIterator(const std::shared_ptr<IDataSource>& source)
            : Source(source)
            , Reverse(Source->GetContext()->GetReadMetadata()->IsDescSorted())
            , Delta(Reverse ? -1 : 1) {
            AFL_VERIFY(Source);
            auto arr = Source->GetStart().GetValue().GetArrays();
            auto batch =
                arrow::RecordBatch::Make(Source->GetSourceSchema()->GetIndexInfo().GetReplaceKey(), arr.front()->length(), std::move(arr));
            SortableRecord =
                std::make_shared<NArrow::NMerger::TRWSortableBatchPosition>(batch, Source->GetStart().GetValue().GetMonoPosition(), Reverse);
        }

        TSourceIterator(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& arrs,
            const std::shared_ptr<NArrow::TColumnFilter>& filter, const std::shared_ptr<IDataSource>& source)
            : Source(source)
            , Reverse(Source->GetContext()->GetReadMetadata()->IsDescSorted())
            , Delta(Reverse ? -1 : 1)
            , Start(Reverse ? (arrs.front()->GetRecordsCount() - 1) : 0)
            , Finish(Reverse ? 0 : (arrs.front()->GetRecordsCount() - 1))
            , Filter(filter ? filter : std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter())) {
            AFL_VERIFY(arrs.size());
            AFL_VERIFY(arrs.front()->GetRecordsCount());
            FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetIterator(Reverse, arrs.front()->GetRecordsCount()));
            auto prefixSchema = Source->GetSourceSchema()->GetIndexInfo().GetReplaceKeyPrefix(arrs.size());
            auto copyArrs = arrs;
            auto batch = std::make_shared<NArrow::TGeneralContainer>(prefixSchema->fields(), std::move(copyArrs));
            SortableRecord = std::make_shared<NArrow::NMerger::TRWSortableBatchPosition>(batch, Start, Reverse);
            IsValidFlag = ShiftWithFilter();
        }

        ui64 GetSourceId() const {
            AFL_VERIFY(Source);
            return Source->GetSourceId();
        }

        bool IsFilled() const {
            return !!Filter;
        }

        bool IsValid() const {
            return IsValidFlag;
        }

        bool Next() {
            AFL_VERIFY(IsValidFlag);
            AFL_VERIFY(!!SortableRecord);
            AFL_VERIFY(!!Filter);
            IsValidFlag = SortableRecord->NextPosition(Delta);
            AFL_VERIFY(FilterIterator->Next(1) == IsValidFlag);
            if (IsValidFlag) {
                IsValidFlag = ShiftWithFilter();
            }
            return IsValidFlag;
        }

        bool operator<(const TSourceIterator& item) const {
            const auto cmp = SortableRecord->ComparePartial(*item.SortableRecord);
            if (cmp == std::partial_ordering::equivalent) {
                return item.Source->GetSourceId() < Source->GetSourceId();
            }
            return cmp == std::partial_ordering::greater;
        }
    };

    THashMap<ui32, TSourceIterator> FilledIterators;
    std::vector<TSourceIterator> Iterators;

    virtual std::shared_ptr<TFetchingScript> BuildFetchingPlan(const std::shared_ptr<IDataSource>& source) const override {
        AFL_VERIFY(FetchedCount < Limit);
    }

    virtual bool OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) const override {
        if (FetchedCount >= Limit) {
            return true;
        }
        const auto& rk = *source->GetSourceSchema()->GetIndexInfo().GetReplaceKey();
        const auto& g = *source->GetStageResult().GetBatch();
        AFL_VERIFY(g.GetRecordsCount());
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrs;
        for (auto&& i : rk.fields()) {
            auto acc = g.GetAccessorByNameOptional(i->name());
            if (!acc) {
                break;
            }
            arrs.emplace_back(acc);
        }
        AFL_VERIFY(arrs.size());
        if (!PKPrefixSize) {
            PKPrefixSize = arrs.size();
        } else {
            AFL_VERIFY(*PKPrefixSize == arrs.size())("prefix", PKPrefixSize)("arr", arrs.size());
        }
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoOnSourceCheckLimitFillIterator")("source_id", source->GetSourceId())(
            "fetched", FetchedCount)("limit", Limit);
        AFL_VERIFY(FilledIterators.emplace(source->GetSourceId(),
            TSourceIterator(arrs, source->GetStageResult().GetNotAppliedFilter(), source)).second);
        AFL_VERIFY(Iterators.size());
        AFL_VERIFY(Iterators.front().GetSourceId() == source->GetSourceId());
        if (DrainToLimit()) {
            reader.GetScanner().MutableSourcesCollection().Clear();
        }
        return true;
    }

    bool DrainToLimit() {
        while (Iterators.size()) {
            if (!Iterators.front().IsFilled()) {
                auto it = FilledIterators.find(Iterators.front().GetSourceId());
                if (it == FilledIterators.end()) {
                    return false;
                }
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "RemoveFilledIterator")("source_id", Iterators.front().GetSourceId())(
                    "fetched", FetchedCount)("limit", Limit);
                std::pop_heap(Iterators.begin(), Iterators.end());
                AFL_VERIFY(it->second.IsFilled());
                Iterators.back() = std::move(it->second);
                AFL_VERIFY(Iterators.back().IsValid());
                std::push_heap(Iterators.begin(), Iterators.end());
                FilledIterators.erase(it);
            } else {
                std::pop_heap(Iterators.begin(), Iterators.end());
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "LimitIteratorNext")("source_id", Iterators.back().GetSourceId())(
                    "fetched", FetchedCount)("limit", Limit)("max", GetMaxInFlight())("in_flight_limit", InFlightLimit)(
                    "count", FetchingInFlightSources.size())("iterators", Iterators.size());
                if (!Iterators.back().Next()) {
                    Iterators.pop_back();
                } else {
                    std::push_heap(Iterators.begin(), Iterators.end());
                    if (++FetchedCount >= Limit) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

public:
    TSyncPointLimitControl(const ui32 limit, const ui32 pointIndex)
        : TBase(pointIndex, "SYNC_LIMIT")
        , Limit(limit) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
