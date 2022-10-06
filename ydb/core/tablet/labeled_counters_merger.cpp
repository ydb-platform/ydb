#include "labeled_counters_merger.h"

namespace NKikimr {

namespace NLC = NKikimrLabeledCounters;

TMerger::TMerger(THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>& response,
                 TTabletLabeledCountersResponseContext& record)
    : Response(response)
    , ResponseContext(record)
{}

void TMerger::Merge(const NLC::TEvTabletLabeledCountersResponse& source) {
    TVector<ui32> namesMapper;
    namesMapper.reserve(source.CounterNamesSize());
    for (const TString& name : source.GetCounterNames()) {
        namesMapper.push_back(ResponseContext.GetNameId(name));
    }
    for (const NLC::TTabletLabeledCounters& srcCounters : source.GetLabeledCountersByGroup()) {
        NLC::TTabletLabeledCounters* trgCounters = GetCounters(srcCounters.GetGroup());
        for (const NLC::TTabletLabeledCounter& srcCounter : srcCounters.GetLabeledCounter()) {
            ui32 nameId = 0;
            if (srcCounter.HasName()) {
                nameId = ResponseContext.GetNameId(srcCounter.GetName());
            } else {
                nameId = namesMapper[srcCounter.GetNameId()];
            }
            NLC::TTabletLabeledCounter* trgCounter = GetCounter(trgCounters, nameId);
            MergeOne(srcCounter, *trgCounter);
        }
    }
}

NLC::TTabletLabeledCounters* TMerger::GetCounters(const TString& group) {
    auto it = IndexTabletLabeledCounters.find(group);
    if (it != IndexTabletLabeledCounters.end()) {
        return it->second;
    }
    NLC::TTabletLabeledCounters* counters =
            Response->Record.AddLabeledCountersByGroup();
    counters->SetGroup(group);
    counters->SetDelimiter("/"); // TODO: replace with |
    IndexTabletLabeledCounters.emplace(group, counters);
    return counters;
}

NLC::TTabletLabeledCounter* TMerger::GetCounter(NLC::TTabletLabeledCounters* counters, ui32 nameId) {
    auto key = std::make_pair(counters, nameId);
    auto it = IndexTabletLabeledCounter.find(key);
    if (it != IndexTabletLabeledCounter.end()) {
        return it->second;
    }
    NLC::TTabletLabeledCounter* counter = counters->AddLabeledCounter();
    counter->SetNameId(nameId);
    IndexTabletLabeledCounter.emplace(key, counter);
    return counter;
}

} // namespace NKikimr
