#pragma once

#include "events.h"
#include "splitter.h"

#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TInternalFilterConstructor: TMoveOnly {
private:
    TEvRequestFilter::TPtr OriginalRequest;
    const TColumnDataSplitter Intervals;
    const std::shared_ptr<NGroupedMemoryManager::TProcessGuard> ProcessGuard;
    const std::shared_ptr<NGroupedMemoryManager::TScopeGuard> ScopeGuard;
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard> GroupGuard;

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
        AFL_VERIFY(filterExt.GetRecordsCountVerified() == info.GetRows().NumRows())("filter", filterExt.GetRecordsCountVerified())(
                                                            "info", info.GetRows().NumRows());
        FiltersByRange.emplace(info.GetRows(), filterExt);

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
        AFL_VERIFY(IsDone())("state", DebugString());
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        sb << "Portion=" << OriginalRequest->Get()->GetSourceId() << ";";
        sb << "ReadyFilters=[";
        for (const auto& [range, _] : FiltersByRange) {
            sb << range.DebugString() << ";";
        }
        sb << "];";
        sb << "}";
        return sb;
    }

    ui64 GetMemoryProcessId() const {
        return ProcessGuard->GetProcessId();
    }
    ui64 GetMemoryScopeId() const {
        return ScopeGuard->GetProcessId();
    }
    ui64 GetMemoryGroupId() const {
        return GroupGuard->GetProcessId();
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
