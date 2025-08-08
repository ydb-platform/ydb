#pragma once

#include "events.h"
#include "splitter.h"

#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TInternalFilterConstructor: TMoveOnly {
private:
    const TEvRequestFilter::TPtr OriginalRequest;
    const TColumnDataSplitter Intervals;
    const std::shared_ptr<NGroupedMemoryManager::TProcessGuard> ProcessGuard;
    const std::shared_ptr<NGroupedMemoryManager::TScopeGuard> ScopeGuard;
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard> GroupGuard;
    bool Done = false;

    std::map<TRowRange, NArrow::TColumnFilter> FiltersByRange;

private:
    bool IsReady() const {
        return FiltersByRange.size() == 1 && FiltersByRange.begin()->first.GetBegin() == 0 &&
               FiltersByRange.begin()->first.GetEnd() == OriginalRequest->Get()->GetRecordsCount();
    }

    void Complete() {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(IsReady());
        OriginalRequest->Get()->GetSubscriber()->OnFilterReady(std::move(FiltersByRange.begin()->second));
        Done = true;
        AFL_VERIFY(IsDone());
    }

public:
    void AddFilter(const TDuplicateMapInfo& info, const NArrow::TColumnFilter& filterExt) {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(filterExt.GetRecordsCountVerified() == info.GetRows().NumRows())("filter", filterExt.GetRecordsCountVerified())(
                                                            "info", info.GetRows().NumRows());
        AFL_VERIFY(FiltersByRange.emplace(info.GetRows(), filterExt).second)("info", info.DebugString());
        AFL_VERIFY(info.GetRows().GetEnd() <= OriginalRequest->Get()->GetRecordsCount())("range", info.GetRows().DebugString())(
                                                "requested", OriginalRequest->Get()->GetRecordsCount());

        while (FiltersByRange.size() > 1 && FiltersByRange.begin()->first.GetEnd() >= std::next(FiltersByRange.begin())->first.GetBegin()) {
            auto l = FiltersByRange.begin();
            auto r = std::next(FiltersByRange.begin());
            AFL_VERIFY(l->first.GetEnd() == r->first.GetBegin())("l", l->first.DebugString())("r", r->first.DebugString())(
                                              "reason", "intersection_prohibited");
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
        return Done;
    }

    void Abort(const TString& error) {
        OriginalRequest->Get()->GetSubscriber()->OnFailure(error);
        Done = true;
    }

    const TEvRequestFilter::TPtr& GetRequest() const {
        return OriginalRequest;
    }

    const TColumnDataSplitter& GetIntervals() const {
        return Intervals;
    }

    TInternalFilterConstructor(const TEvRequestFilter::TPtr& request, TColumnDataSplitter&& splitter);

    ~TInternalFilterConstructor() {
        AFL_VERIFY(IsDone() || (OriginalRequest->Get()->GetAbortionFlag() && OriginalRequest->Get()->GetAbortionFlag()->Val()))(
                                                                             "state", DebugString());
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
