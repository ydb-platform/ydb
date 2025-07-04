#pragma once

#include "events.h"
#include "splitter.h"

#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TInternalFilterConstructor: TMoveOnly {
private:
    class TRowRange {
    private:
        YDB_READONLY_DEF(ui64, Begin);
        YDB_READONLY_DEF(ui64, End);

    public:
        TRowRange(const ui64 begin, const ui64 end)
            : Begin(begin)
            , End(end) {
            AFL_VERIFY(end >= begin);
        }

        std::partial_ordering operator<=>(const TRowRange& other) const {
            return std::tie(Begin, End) <=> std::tie(other.Begin, other.End);
        }
        bool operator==(const TRowRange& other) const {
            return (*this <=> other) == std::partial_ordering::equivalent;
        }

        ui64 NumRows() const {
            return End - Begin;
        }
    };

private:
    TEvRequestFilter::TPtr OriginalRequest;
    const TColumnDataSplitter Intervals;

    std::map<TRowRange, NArrow::TColumnFilter> FiltersByRange;

private:
    bool IsReady() const {
        return !FiltersByRange.empty() && FiltersByRange.begin()->first.GetBegin() == 0 &&
               FiltersByRange.begin()->first.GetEnd() == OriginalRequest->Get()->GetRecordsCount();
    }

    void Complete() {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(IsReady());
        OriginalRequest->Get()->GetSubscriber()->OnFilterReady(std::move(FiltersByRange.begin()->second));
        OriginalRequest.Reset();
        AFL_VERIFY(IsDone());
    }

public:
    void AddFilter(const TDuplicateMapInfo& info, const NArrow::TColumnFilter& filterExt) {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(filterExt.GetRecordsCountVerified() == info.GetRowsCount())("filter", filterExt.GetRecordsCountVerified())(
                                                            "info", info.GetRowsCount());
        FiltersByRange.emplace(TRowRange(info.GetOffset(), info.GetOffset() + info.GetRowsCount()), filterExt);

        while (FiltersByRange.size() > 1 && FiltersByRange.begin()->first.GetEnd() >= std::next(FiltersByRange.begin())->first.GetBegin()) {
            auto l = FiltersByRange.begin();
            auto r = std::next(FiltersByRange.begin());
            AFL_VERIFY(l->first.GetEnd() == r->first.GetBegin());
            TRowRange range = TRowRange(l->first.GetBegin(), r->first.GetEnd());
            NArrow::TColumnFilter filter = l->second;
            filter.Append(r->second);
            FiltersByRange.erase(FiltersByRange.begin());
            FiltersByRange.erase(FiltersByRange.begin());
            AFL_VERIFY(filter.GetRecordsCountVerified() == range.NumRows())("filter", filter.GetRecordsCountVerified())("range", range.NumRows());
            FiltersByRange.emplace(range, std::move(filter));
        }

        if (IsReady()) {
            Complete();
        }
    }

    bool IsDone() const {
        return !OriginalRequest;
    }

    void Abort(const TString& error) {
        OriginalRequest->Get()->GetSubscriber()->OnFailure(error);
        OriginalRequest.Reset();
    }

    const TEvRequestFilter::TPtr& GetRequest() const {
        AFL_VERIFY(!!OriginalRequest);
        return OriginalRequest;
    }

    const TColumnDataSplitter& GetIntervals() const {
        return Intervals;
    }

    TInternalFilterConstructor(const TEvRequestFilter::TPtr& request, TColumnDataSplitter&& splitter);

    ~TInternalFilterConstructor() {
        AFL_VERIFY(IsDone());
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
