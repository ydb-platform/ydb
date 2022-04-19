#include "tablet_counters_aggregator.h"
#include "tablet_counters_app.h"
#include <library/cpp/actors/core/log.h>
#include <ydb/core/mon/mon.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/db_counters.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>

#include <util/generic/xrange.h>
#include <util/string/vector.h>
#include <util/string/split.h>

#ifdef _darwin_
#pragma GCC diagnostic ignored "-Wformat"
#endif

////////////////////////////////////////////
namespace NKikimr {

TActorId MakeTabletCountersAggregatorID(ui32 node, bool follower) {
    if (!follower) {
        char x[12] = {'t','a','b','l','c','o','u','n','t','a','g','g'};
        return TActorId(node, TStringBuf(x, 12));
    } else {
        char x[12] ={'s','l','a','v','c','o','u','n','t','a','g','g'};
        return TActorId(node, TStringBuf(x, 12));
    }
}

TStringBuf GetHistogramAggregateSimpleName(TStringBuf name) {
    TStringBuf buffer(name);
    if (buffer.SkipPrefix("HIST(")) {
        return buffer.Before(')');
    }
    return TStringBuf();
}

bool IsHistogramAggregateSimpleName(TStringBuf name) {
    return !GetHistogramAggregateSimpleName(name).empty();
}

////////////////////////////////////////////
namespace {

const ui32 WAKEUP_TIMEOUT_SECONDS = 4;


////////////////////////////////////////////
using TCountersVector = TVector<NMonitoring::TDynamicCounters::TCounterPtr>;

struct THistogramCounter {
    TVector<TTabletPercentileCounter::TRangeDef> Ranges;
    TCountersVector Values;
    NMonitoring::THistogramPtr Histogram;

    THistogramCounter(
        const TVector<TTabletPercentileCounter::TRangeDef>& ranges,
        TCountersVector&& values,
        NMonitoring::THistogramPtr histogram)
        : Ranges(ranges)
        , Values(std::move(values))
        , Histogram(histogram)
    {
        Y_VERIFY(!Ranges.empty() && Ranges.size() == Values.size());
    }

    void Clear() {
        for (const NMonitoring::TDynamicCounters::TCounterPtr& cnt : Values) {
            *cnt = 0;
        }

        Histogram->Reset();
    }

    void IncrementFor(ui64 value) {
        const size_t i = Max<ssize_t>(0, std::upper_bound(Ranges.begin(), Ranges.end(), value) - Ranges.begin() - 1);
        Values[i]->Inc();

        Histogram->Collect(value);
    }
};

using THistogramVector = TVector<THolder<THistogramCounter>>;

class TAggregatedSimpleCounters {
public:
    //
    TAggregatedSimpleCounters(NMonitoring::TDynamicCounterPtr counterGroup)
        : CounterGroup(counterGroup)
    {}

    void Reserve(size_t hint) {
        CountersByTabletID.reserve(hint);
        ChangedCounters.reserve(hint);
        MaxSimpleCounters.reserve(hint);
    }

    void AddSimpleCounter(const char* name, THolder<THistogramCounter> percentileAggregate = THolder<THistogramCounter>()) {
        auto fnAddCounter = [this](const char* name, TCountersVector& container) {
            auto counter = CounterGroup->GetCounter(name, false);
            container.push_back(counter);
        };

        CountersByTabletID.push_back(TCountersByTabletIDMap());
        ChangedCounters.push_back(true);
        TString maxName = Sprintf("MAX(%s)", name);
        TString sumName = Sprintf("SUM(%s)", name);

        fnAddCounter(maxName.data(), MaxSimpleCounters);
        fnAddCounter(sumName.data(), SumSimpleCounters);

        HistSimpleCounters.emplace_back(std::move(percentileAggregate));
    }

    ui64 GetSum(ui32 counterIndex) const {
        Y_VERIFY(counterIndex < SumSimpleCounters.size(),
            "inconsistent sum simple counters, %u >= %lu", counterIndex, SumSimpleCounters.size());
        return *SumSimpleCounters[counterIndex];
    }

    void SetSum(ui32 counterIndex, ui64 value) {
        Y_VERIFY(counterIndex < SumSimpleCounters.size(),
            "inconsistent sum simple counters, %u >= %lu", counterIndex, SumSimpleCounters.size());
        *SumSimpleCounters[counterIndex] = value;
    }

    ui64 GetMax(ui32 counterIndex) const {
        Y_VERIFY(counterIndex < MaxSimpleCounters.size(),
            "inconsistent max simple counters, %u >= %lu", counterIndex, MaxSimpleCounters.size());
        return *MaxSimpleCounters[counterIndex];
    }

    void SetMax(ui32 counterIndex, ui64 value) {
        Y_VERIFY(counterIndex < MaxSimpleCounters.size(),
            "inconsistent max simple counters, %u >= %lu", counterIndex, MaxSimpleCounters.size());
        *MaxSimpleCounters[counterIndex] = value;
    }

    void SetValue(ui64 tabletID, ui32 counterIndex, ui64 value, TTabletTypes::EType tabletType) {
        Y_VERIFY(counterIndex < CountersByTabletID.size(),
            "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
        TCountersByTabletIDMap::insert_ctx insertCtx;
        auto it = CountersByTabletID[counterIndex].find(tabletID, insertCtx);
        if (it != CountersByTabletID[counterIndex].end()) {
            if (it->second != value) {
                ChangedCounters[counterIndex] = true;
                it->second = value;
            }
        } else {
            CountersByTabletID[counterIndex].insert_direct(std::make_pair(tabletID, value), insertCtx);
            ChangedCounters[counterIndex] = true;
        }
    }

    void ForgetTablet(ui64 tabletId) {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            auto &counters = CountersByTabletID[idx];
            if (counters.erase(tabletId) != 0)
                ChangedCounters[idx] = true;
        }
    }

    void RecalcAll() {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            if (ChangedCounters[idx])
                Recalc(idx);
            ChangedCounters[idx] = false;
        }
    }

private:
    //
    NMonitoring::TDynamicCounterPtr CounterGroup;

    TCountersVector MaxSimpleCounters;
    TCountersVector SumSimpleCounters;
    THistogramVector HistSimpleCounters;
    using TCountersByTabletIDMap = THashMap<ui64, ui64>;

    TVector<TCountersByTabletIDMap> CountersByTabletID;
    TVector<bool> ChangedCounters;

private:
    void Recalc(ui32 idx) {
        auto &counters = CountersByTabletID[idx];
        THistogramCounter* histCounter = HistSimpleCounters[idx].Get();

        ui64 maxVal = 0;
        ui64 sumVal = 0;

        if (histCounter) {
            histCounter->Clear();
        }

        for (auto&& t : counters) {
            ui64 tValue = t.second;
            maxVal = Max(tValue, maxVal);
            sumVal += tValue;
            if (histCounter) {
                histCounter->IncrementFor(tValue);
            }
        }

        *MaxSimpleCounters[idx].Get() = maxVal;
        *SumSimpleCounters[idx].Get() = sumVal;
    }
};

class TAggregatedCumulativeCounters {
public:
    //
    TAggregatedCumulativeCounters(NMonitoring::TDynamicCounterPtr counterGroup)
        : CounterGroup(counterGroup)
    {}

    void Reserve(size_t hint) {
        CountersByTabletID.reserve(hint);
        ChangedCounters.reserve(hint);
        MaxCumulativeCounters.reserve(hint);
    }

    void AddCumulativeCounter(const char* name, THolder<THistogramCounter> percentileAggregate = THolder<THistogramCounter>()) {
        auto fnAddCounter = [this](const char* name, TCountersVector& container) {
            auto counter = CounterGroup->GetCounter(name, false);
            container.push_back(counter);
        };

        CountersByTabletID.push_back(TCountersByTabletIDMap());
        ChangedCounters.push_back(true);
        TString maxName = Sprintf("MAX(%s)", name);

        fnAddCounter(maxName.data(), MaxCumulativeCounters);

        HistCumulativeCounters.emplace_back(std::move(percentileAggregate));
    }

    ui64 GetMax(ui32 counterIndex) const {
        Y_VERIFY(counterIndex < MaxCumulativeCounters.size(),
            "inconsistent max cumulative counters, %u >= %lu", counterIndex, MaxCumulativeCounters.size());
        return *MaxCumulativeCounters[counterIndex];
    }

    void SetMax(ui32 counterIndex, ui64 value) {
        Y_VERIFY(counterIndex < MaxCumulativeCounters.size(),
            "inconsistent max cumulative counters, %u >= %lu", counterIndex, MaxCumulativeCounters.size());
        *MaxCumulativeCounters[counterIndex] = value;
    }

    void SetValue(ui64 tabletID, ui32 counterIndex, ui64 value, TTabletTypes::EType tabletType) {
        Y_VERIFY(counterIndex < CountersByTabletID.size(), "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
        TCountersByTabletIDMap::insert_ctx insertCtx;
        auto it = CountersByTabletID[counterIndex].find(tabletID, insertCtx);
        if (it != CountersByTabletID[counterIndex].end()) {
            if (it->second != value) {
                ChangedCounters[counterIndex] = true;
                it->second = value;
            }
        } else {
            CountersByTabletID[counterIndex].insert_direct(std::make_pair(tabletID, value), insertCtx);
            ChangedCounters[counterIndex] = true;
        }
    }

    void ForgetTablet(ui64 tabletId) {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            auto &counters = CountersByTabletID[idx];
            if (counters.erase(tabletId) != 0)
                ChangedCounters[idx] = true;
        }
    }

    void RecalcAll() {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            if (ChangedCounters[idx])
                Recalc(idx);
            ChangedCounters[idx] = false;
        }
    }

private:
    //
    NMonitoring::TDynamicCounterPtr CounterGroup;

    TCountersVector MaxCumulativeCounters;
    THistogramVector HistCumulativeCounters;
    using TCountersByTabletIDMap = THashMap<ui64, ui64>;

    TVector<TCountersByTabletIDMap> CountersByTabletID;
    TVector<bool> ChangedCounters;

private:
    void Recalc(ui32 idx) {
        auto &counters = CountersByTabletID[idx];
        THistogramCounter* histCounter = HistCumulativeCounters[idx].Get();

        ui64 maxVal = 0;

        if (histCounter) {
            histCounter->Clear();
        }

        for (auto&& t : counters) {
            ui64 tValue = t.second;
            maxVal = Max(tValue, maxVal);
            if (histCounter) {
                histCounter->IncrementFor(tValue);
            }
        }

        *MaxCumulativeCounters[idx].Get() = maxVal;
    }
};

class TAggregatedHistogramCounters {
public:
    //

    TAggregatedHistogramCounters(NMonitoring::TDynamicCounterPtr counterGroup)
        : CounterGroup(counterGroup)
    {}

    void Reserve(size_t hint) {
        PercentileCounters.reserve(hint);
        Histograms.reserve(hint);
        IsDerivative.reserve(hint);
        ShiftedBucketBounds.reserve(hint);
        CountersByTabletID.reserve(hint);
    }

    void AddCounter(
        const char* name,
        const NKikimr::TTabletPercentileCounter& percentileCounter,
        THashMap<TString, THolder<THistogramCounter>>& histogramAggregates)
    {
        // old style
        PercentileCounters.push_back(TCountersVector());
        auto& rangeCounters = PercentileCounters.back();

        TStringBuf counterName(name);
        TStringBuf simpleCounterName = GetHistogramAggregateSimpleName(counterName);
        bool histogramAggregate = !simpleCounterName.empty();
        bool isDerivative = !histogramAggregate && !percentileCounter.GetIntegral();
        IsDerivative.push_back(isDerivative);

        auto rangeCount = percentileCounter.GetRangeCount();
        Y_VERIFY_DEBUG(rangeCount > 0);

        for (ui32 r = 0; r < rangeCount; ++r) {
            const char* rangeName = percentileCounter.GetRangeName(r);
            auto subgroup = CounterGroup->GetSubgroup("range", rangeName);
            auto counter = subgroup->GetCounter(name, isDerivative);
            rangeCounters.push_back(counter);
        }

        // Note that:
        // 1. PercentileCounters always start from 0 range
        // 2. Ranges in PercentileCounters are left inclusive, i.e. for ranges 0, 1, 2 buckets will be
        // [0; 1), [1; 2), [2; +inf);
        // 3. In monitoring's histogram buckets are right inclusive and can be negative, i.e. for ranges 0, 1, 2
        // buckets will be: (-inf; 0], (0; 1], (1; 2], (2; +inf).
        // 4. Currently we shift PercentileCounters ranges so that original ranges 0, 1, 2 become 1, 2:
        // (-inf; 1], (1; 2], (2; +inf). This is because values in proto are lower bounds

        // new style
        NMonitoring::TBucketBounds bucketBounds;
        for (ui32 r = 1; r < rangeCount; ++r) {
            bucketBounds.push_back(percentileCounter.GetRangeBound(r));
        }

        // since we shift we need hack for hists with single bucket (though they are meaningless anyway),
        // hist will be (-inf; range0], (range0; +inf).
        if (bucketBounds.empty()) {
            bucketBounds.push_back(percentileCounter.GetRangeBound(0));
        }

        auto histogram = CounterGroup->GetHistogram(
            name, NMonitoring::ExplicitHistogram(bucketBounds), isDerivative);

        if (histogramAggregate) {
            // either simple or cumulative aggregate will handle this histogram,
            // it is a special case for hists name HIST(name), which have corresponding
            // simple or cumulative counter updated by tablet (tablet doesn't update hist itself,
            // hist is updated here by aggregated values)
            histogramAggregates.emplace(simpleCounterName, new THistogramCounter(
                percentileCounter.GetRanges(), std::move(rangeCounters), histogram));

            // we need this hack to access PercentileCounters by index easily skipping
            // hists we moved to simple/cumulative aggregates
            TCountersVector().swap(rangeCounters);
            ShiftedBucketBounds.emplace_back();
        } else {
            // note that this bound in histogram is implicit
            bucketBounds.push_back(Max<NMonitoring::TBucketBound>());
            ShiftedBucketBounds.emplace_back(std::move(bucketBounds));
        }

        // note that in case of histogramAggregate it will contain reference
        // on the histogram updated outside
        Histograms.push_back(histogram);

        CountersByTabletID.emplace_back(TCountersByTabletIDMap());
    }

    void SetValue(
        ui64 tabletID,
        ui32 counterIndex,
        const NKikimr::TTabletPercentileCounter& percentileCounter,
        const char* name,
        TTabletTypes::EType tabletType)
    {
        Y_VERIFY(counterIndex < CountersByTabletID.size(),
            "inconsistent counters for tablet type %s, counter %s",
            TTabletTypes::TypeToStr(tabletType),
            name);

        Y_VERIFY(counterIndex < PercentileCounters.size(),
            "inconsistent counters for tablet type %s, counter %s",
            TTabletTypes::TypeToStr(tabletType),
            name);

        auto& percentileRanges = PercentileCounters[counterIndex];

        // see comment in AddCounter() related to histogramAggregate
        if (percentileRanges.empty())
            return;

        // just sanity check, normally should not happen
        const auto rangeCount = percentileCounter.GetRangeCount();
        if (rangeCount == 0)
            return;

        Y_VERIFY(rangeCount <= percentileRanges.size(),
            "inconsistent counters for tablet type %s, counter %s",
            TTabletTypes::TypeToStr(tabletType),
            name);

        if (IsDerivative[counterIndex]) {
            AddValues(counterIndex, percentileCounter);
            return;
        }

        // integral histogram

        TValuesVec newValues;
        newValues.reserve(rangeCount);
        for (auto i: xrange(rangeCount))
            newValues.push_back(percentileCounter.GetRangeValue(i));

        TCountersByTabletIDMap::insert_ctx insertCtx;
        auto it = CountersByTabletID[counterIndex].find(tabletID, insertCtx);
        if (it != CountersByTabletID[counterIndex].end()) {
            auto& oldValues = it->second;
            if (newValues != oldValues) {
                SubValues(counterIndex, oldValues);
                AddValues(counterIndex, newValues);
            }
            oldValues.swap(newValues);
        } else {
            AddValues(counterIndex, newValues);
            CountersByTabletID[counterIndex].insert_direct(std::make_pair(tabletID, std::move(newValues)), insertCtx);
        }
    }

    void ForgetTablet(ui64 tabletId) {
        for (auto idx : xrange(CountersByTabletID.size())) {
            auto &tabletToCounters = CountersByTabletID[idx];
            auto it = tabletToCounters.find(tabletId);
            if (it == tabletToCounters.end())
                continue;

            auto values = std::move(it->second);
            tabletToCounters.erase(it);

            if (IsDerivative[idx])
                continue;

            SubValues(idx, values);
        }
    }

    NMonitoring::THistogramPtr GetHistogram(size_t i) {
        Y_VERIFY(i < Histograms.size());
        return Histograms[i];
    }

private:
    using TValuesVec = TVector<ui64>;

    void SubValues(size_t counterIndex, const TValuesVec& values) {
        auto& percentileRanges = PercentileCounters[counterIndex];
        auto& histogram = Histograms[counterIndex];
        auto snapshot = histogram->Snapshot();
        histogram->Reset();
        for (auto i: xrange(values.size())) {
            Y_VERIFY_DEBUG(static_cast<ui64>(*percentileRanges[i]) >= values[i]);
            *percentileRanges[i] -= values[i];

            ui64 oldValue = snapshot->Value(i);
            ui64 negValue = 0UL - values[i];
            ui64 newValue = oldValue + negValue;
            histogram->Collect(ShiftedBucketBounds[counterIndex][i], newValue);
        }
    }

    void AddValues(size_t counterIndex, const TValuesVec& values) {
        auto& percentileRanges = PercentileCounters[counterIndex];
        auto& histogram = Histograms[counterIndex];
        for (auto i: xrange(values.size())) {
            *percentileRanges[i] += values[i];
            histogram->Collect(ShiftedBucketBounds[counterIndex][i], values[i]);
        }
    }

    void AddValues(size_t counterIndex, const NKikimr::TTabletPercentileCounter& percentileCounter) {
        auto& percentileRanges = PercentileCounters[counterIndex];
        auto& histogram = Histograms[counterIndex];
        for (auto i: xrange(percentileCounter.GetRangeCount())) {
            auto value = percentileCounter.GetRangeValue(i);
            *percentileRanges[i] += value;
            histogram->Collect(ShiftedBucketBounds[counterIndex][i], value);
        }
    }

private:
    NMonitoring::TDynamicCounterPtr CounterGroup;

    // monitoring counters holders, updated only during recalculation
    TVector<TCountersVector> PercentileCounters;    // old style (ranges)
    TVector<NMonitoring::THistogramPtr> Histograms; // new style (bins)
    TVector<bool> IsDerivative;

    // per percentile counter bounds. Note the shift: index0 is range1,
    // hence array size is 1 less than original ranges count
    TVector<NMonitoring::TBucketBounds> ShiftedBucketBounds;

    // tabletId -> values
    using TCountersByTabletIDMap = THashMap<ui64, TValuesVec>;

    // counter values (not "real" monitoring counters)
    TVector<TCountersByTabletIDMap> CountersByTabletID; // each index is map from tablet to counter value
};

struct TTabletLabeledCountersResponseContext {
    NKikimrTabletCountersAggregator::TEvTabletLabeledCountersResponse& Response;
    THashMap<TStringBuf, ui32> NamesToId;

    TTabletLabeledCountersResponseContext(NKikimrTabletCountersAggregator::TEvTabletLabeledCountersResponse& response)
        : Response(response)
    {}

    ui32 GetNameId(TStringBuf name) {
        auto it = NamesToId.find(name);
        if (it != NamesToId.end()) {
            return it->second;
        }
        Response.AddCounterNames(TString(name));
        ui32 id = Response.CounterNamesSize() - 1;
        NamesToId[name] = id;
        return id;
    }
};

class TAggregatedLabeledCounters {
public:
    //
    TAggregatedLabeledCounters(ui32 count, const ui8* aggrFunc, const char * const * names, const ui8* types, const TString& groupNames)
        : AggrFunc(aggrFunc)
        , Names(names)
        , GroupNames(groupNames)
        , Types(types)
        , AggrCounters(count, 0)
        , Ids(count, 0)
        , Changed(false)
        , CountersByTabletID(count)
    {
    }

    void SetValue(ui64 tabletID, ui32 counterIndex, ui64 value, ui64 id) {

        CountersByTabletID[counterIndex][tabletID] = std::make_pair(value, id);
        Changed = true;
    }

    bool ForgetTablet(ui64 tabletId) {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            auto &counters = CountersByTabletID[idx];
            counters.erase(tabletId);
        }
        Changed = true;
        return CountersByTabletID.size() == 0 || CountersByTabletID[0].size() == 0;
    }

    ui32 Size() const {
        return AggrCounters.size();
    }

    ui64 GetValue(ui32 index) const {
        return AggrCounters[index];
    }

    ui64 GetId(ui32 index) const {
        return Ids[index];
    }

    void FillGetRequestV1(NKikimrTabletCountersAggregator::TTabletLabeledCounters& labeledCounters, const TString& group, ui32 start, ui32 end) const {
        if (Changed) {
            for (ui32 idx : xrange(CountersByTabletID.size())) {
                Recalc(idx);
            }
            Changed = false;
        }
        Y_VERIFY(end >= start);
        Y_VERIFY(end <= Size());
        labeledCounters.SetGroupNames(GroupNames);
        labeledCounters.SetGroup(group);
        labeledCounters.SetDelimiter("/"); //TODO: change here to "|"
        for (ui32 i = start; i < end; ++i) {
            auto& labeledCounter = *labeledCounters.AddLabeledCounter();
            labeledCounter.SetValue(GetValue(i));
            labeledCounter.SetId(GetId(i));
            labeledCounter.SetName(Names[i]);
            labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(AggrFunc[i]));
            labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(Types[i]));
        }
    }

    void FillGetRequestV2(TTabletLabeledCountersResponseContext& context, const TString& group) const {
        if (Changed) {
            for (ui32 idx : xrange(CountersByTabletID.size())) {
                Recalc(idx);
            }
            Changed = false;
        }
        auto& labeledCounters = *context.Response.AddLabeledCountersByGroup();
        labeledCounters.SetGroup(group);
        labeledCounters.SetDelimiter("/"); //TODO: change here to "|"
        for (ui32 i = 0; i < Size(); ++i) {
            auto& labeledCounter = *labeledCounters.AddLabeledCounter();
            labeledCounter.SetValue(GetValue(i));
            labeledCounter.SetNameId(context.GetNameId(Names[i]));
            labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(AggrFunc[i]));
            labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(Types[i]));
        }
    }


private:
    //
    NMonitoring::TDynamicCounterPtr CounterGroup;
    const ui8* AggrFunc;
    const char* const * Names;
    TString GroupNames;
    const ui8* Types;

    mutable TVector<ui64> AggrCounters;
    mutable TVector<ui64> Ids;
    mutable bool Changed;

    using TCountersByTabletIDMap = THashMap<ui64, std::pair<ui64, ui64>>; //second pair is for counter and id
    TVector<TCountersByTabletIDMap> CountersByTabletID;

private:
    void Recalc(ui32 idx) const {
        Y_VERIFY(idx < Ids.size());
        auto &counters = CountersByTabletID[idx];
        ui8 aggrFunc = AggrFunc[idx];
        std::pair<ui64, ui64> aggrVal{0,0};
        ui64 cntCount = counters.size();

        Y_VERIFY(cntCount > 0);
        if (aggrFunc == TTabletLabeledCountersBase::EAF_MIN)
            aggrVal = counters.begin()->second;

        for (auto&& t : counters) {
            const std::pair<ui64, ui64>& tValue = t.second;
            switch (aggrFunc) {
                case TTabletLabeledCountersBase::EAF_MIN:
                    aggrVal = Min(tValue, aggrVal);
                    break;
                case TTabletLabeledCountersBase::EAF_MAX:
                    aggrVal = Max(tValue, aggrVal);
                    break;
                case TTabletLabeledCountersBase::EAF_SUM:
                    aggrVal.first += tValue.first;
                    break;
                default:
                    Y_FAIL("bad aggrFunc value");
            };
        }
        AggrCounters[idx] = aggrVal.first;
        Ids[idx] = aggrVal.second;
    }
};



}

////////////////////////////////////////////
class TTabletMon {
public:
    //
    TTabletMon(NMonitoring::TDynamicCounterPtr counters, bool isFollower, TActorId dbWatcherActorId)
        : Counters(GetServiceCounters(counters, isFollower ? "followers" : "tablets"))
        , AllTypes(Counters.Get(), "type", "all")
        , IsFollower(isFollower)
        , DbWatcherActorId(dbWatcherActorId)
    {
        if (!IsFollower) {
            YdbCounters = MakeIntrusive<TYdbTabletCounters>(GetServiceCounters(counters, "ydb"));
        }
    }

    void Apply(ui64 tabletID, TTabletTypes::EType tabletType, TPathId tenantPathId,
        const TTabletCountersBase* executorCounters, const TTabletCountersBase* appCounters,
        const TActorContext& ctx)
    {
        AllTypes.Apply(tabletID, executorCounters, nullptr, tabletType);
        //
        auto* typeCounters = GetOrAddCountersByTabletType(tabletType, CountersByTabletType, Counters);
        if (typeCounters) {
            typeCounters->Apply(tabletID, executorCounters, appCounters, tabletType);
        }
        //
        if (!IsFollower && AppData(ctx)->FeatureFlags.GetEnableDbCounters() && tenantPathId) {
            auto dbCounters = GetDbCounters(tenantPathId, ctx);
            if (dbCounters) {
                auto* limitedAppCounters = GetOrAddLimitedAppCounters(tabletType);
                dbCounters->Apply(tabletID, executorCounters, appCounters, tabletType, limitedAppCounters);
            }
        }

        //
        auto& quietStats = QuietTabletCounters[tabletID];

        if (executorCounters) {
            if (quietStats.first == nullptr)
                quietStats.first = new TTabletCountersBase();
            quietStats.first->Populate(*executorCounters);
        }

        if (appCounters) {
            if (quietStats.second == nullptr)
                quietStats.second = new TTabletCountersBase();
            quietStats.second->Populate(*appCounters);
        }
    }

    void ApplyLabeledCounters(ui64 tabletID, TTabletTypes::EType tabletType, const TTabletLabeledCountersBase* labeledCounters) {

        auto iterTabletType = LabeledCountersByTabletTypeAndGroup.find(std::make_pair(tabletType, labeledCounters->GetGroup()));

        if (labeledCounters->GetDrop() ) {
            if (iterTabletType != LabeledCountersByTabletTypeAndGroup.end()) {
                LabeledCountersByTabletTypeAndGroup.erase(iterTabletType);
            }
            return;
        }


        if (iterTabletType == LabeledCountersByTabletTypeAndGroup.end()) {
            TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
            TString groupNames;
            TVector<TString> rr;
            StringSplitter(labeledCounters->GetGroup()).Split('/').SkipEmpty().Collect(&rr); // TODO: change here to "|"
            for (ui32 i = 0; i < rr.size(); ++i) {
                if (i > 0)
                    groupNames += '/';
                groupNames += labeledCounters->GetGroupName(i);
            }
            iterTabletType = LabeledCountersByTabletTypeAndGroup.emplace(
                                            std::make_pair(tabletType, labeledCounters->GetGroup()),
                                            new TAggregatedLabeledCounters(labeledCounters->GetCounters().Size(), labeledCounters->GetAggrFuncs(),
                                                    labeledCounters->GetNames(), labeledCounters->GetTypes(), groupNames)
                        ).first;

        }

        for (ui32 i = 0, e = labeledCounters->GetCounters().Size(); i < e; ++i) {
            const ui64& value = labeledCounters->GetCounters()[i].Get();
            const ui64& id = labeledCounters->GetIds()[i].Get();
            iterTabletType->second->SetValue(tabletID, i, value, id);
        }
    }

    void ForgetTablet(ui64 tabletID, TTabletTypes::EType tabletType, TPathId tenantPathId) {
        AllTypes.Forget(tabletID);
        // and now erase from every other path
        auto iterTabletType = CountersByTabletType.find(tabletType);
        if (iterTabletType != CountersByTabletType.end()) {
            iterTabletType->second->Forget(tabletID);
        }
        // from db counters
        if (auto itPath = CountersByPathId.find(tenantPathId); itPath != CountersByPathId.end()) {
            itPath->second->Forget(tabletID, tabletType);
        }
        //and from all labeledCounters that could have this tablet
        auto iterTabletTypeAndGroup = LabeledCountersByTabletTypeAndGroup.lower_bound(std::make_pair(tabletType, TString()));
        for (; iterTabletTypeAndGroup != LabeledCountersByTabletTypeAndGroup.end() && iterTabletTypeAndGroup->first.first == tabletType; ) {
            bool empty = iterTabletTypeAndGroup->second->ForgetTablet(tabletID);
            if (empty) {
                iterTabletTypeAndGroup = LabeledCountersByTabletTypeAndGroup.erase(iterTabletTypeAndGroup);
            } else {
                ++iterTabletTypeAndGroup;
            }
        }

        QuietTabletCounters.erase(tabletID);

        TString tabletIdStr = Sprintf("%" PRIu64, tabletID);
        Counters->RemoveSubgroup("tabletid", tabletIdStr.data());
    }

    void Query(const NKikimrTabletCountersAggregator::TEvTabletCountersRequest& request, NKikimrTabletCountersAggregator::TEvTabletCountersResponse& response) {
        TVector<ui64> tabletIDs(request.GetTabletIds().begin(), request.GetTabletIds().end());
        if (tabletIDs.empty()) {
            for (const auto& pr : QuietTabletCounters) {
                auto& countersInfo = *response.AddCountersInfo();
                countersInfo.SetTabletId(pr.first);
                if (pr.second.first) { // executor counters
                    auto& executorCounters = *countersInfo.MutableExecutorCounters();
                    const auto& simple = pr.second.first->Simple();
                    for (ui32 i = 0; i < simple.Size(); ++i) {
                        executorCounters.AddSimpleCounters(simple[i].Get());
                    }
                    const auto& cumulative = pr.second.first->Cumulative();
                    for (ui32 i = 0; i < cumulative.Size(); ++i) {
                        executorCounters.AddCumulativeCounters(cumulative[i].Get());
                    }
                }
                if (pr.second.second) { // app counters
                    auto& appCounters = *countersInfo.MutableAppCounters();
                    const auto& simple = pr.second.second->Simple();
                    for (ui32 i = 0; i < simple.Size(); ++i) {
                        appCounters.AddSimpleCounters(simple[i].Get());
                    }
                    const auto& cumulative = pr.second.second->Cumulative();
                    for (ui32 i = 0; i < cumulative.Size(); ++i) {
                        appCounters.AddCumulativeCounters(cumulative[i].Get());
                    }
                }
            }
        } else {
            for (ui64 tabletID : tabletIDs) {
                auto it = QuietTabletCounters.find(tabletID);
                if (it != QuietTabletCounters.end()) {
                    auto& countersInfo = *response.AddCountersInfo();
                    countersInfo.SetTabletId(it->first);
                    if (it->second.first) { // executor counters
                        auto& executorCounters = *countersInfo.MutableExecutorCounters();
                        const auto& simple = it->second.first->Simple();
                        for (ui32 i = 0; i < simple.Size(); ++i) {
                            executorCounters.AddSimpleCounters(simple[i].Get());
                        }
                        const auto& cumulative = it->second.first->Cumulative();
                        for (ui32 i = 0; i < cumulative.Size(); ++i) {
                            executorCounters.AddCumulativeCounters(cumulative[i].Get());
                        }
                    }
                    if (it->second.second) { // app counters
                        auto& appCounters = *countersInfo.MutableAppCounters();
                        const auto& simple = it->second.second->Simple();
                        for (ui32 i = 0; i < simple.Size(); ++i) {
                            appCounters.AddSimpleCounters(simple[i].Get());
                        }
                        const auto& cumulative = it->second.second->Cumulative();
                        for (ui32 i = 0; i < cumulative.Size(); ++i) {
                            appCounters.AddCumulativeCounters(cumulative[i].Get());
                        }
                    }
                }
            }
        }
    }

    void QueryLabeledCounters(const NKikimrTabletCountersAggregator::TEvTabletLabeledCountersRequest& request, NKikimrTabletCountersAggregator::TEvTabletLabeledCountersResponse& response, const TActorContext& ctx) {

        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "got request v" << request.GetVersion());

        TString group = request.HasGroup() ? request.GetGroup() : "";
        TTabletTypes::EType tabletType = request.GetTabletType();
        ui32 cc = 0;

        if (request.GetVersion() == 1) {
            auto iter = LabeledCountersByTabletTypeAndGroup.lower_bound(std::make_pair(tabletType, group));
            for (; iter != LabeledCountersByTabletTypeAndGroup.end() && iter->first.first == tabletType &&
                    (group.empty() || iter->first.second == group); ++iter) {

                ui32 s = 0, e = iter->second->Size();
                if (request.HasLabeledCounterId()) {
                    s = request.GetLabeledCounterId();
                    e = s + 1;
                }
                if (s >= iter->second->Size())
                    continue;
                auto& labeledCountersByGroup = *response.AddLabeledCountersByGroup();

                iter->second->FillGetRequestV1(labeledCountersByGroup, iter->first.second, s, e);
                ++cc;
            }
        } else if (request.GetVersion() >= 2) {
            TTabletLabeledCountersResponseContext context(response);
            auto iter = LabeledCountersByTabletTypeAndGroup.lower_bound({tabletType, TString()});
            for (; iter != LabeledCountersByTabletTypeAndGroup.end()
                   && iter->first.first == tabletType; ++iter) {
                if (group.empty() || IsMatchesWildcards(iter->first.second, group)) {
                    iter->second->FillGetRequestV2(context, iter->first.second);
                }
                ++cc;
            }
        }
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "request processed, " << cc << " groups processed");
    }

    void RecalcAll() {
        AllTypes.RecalcAll();
        for (auto& c : CountersByTabletType) {
            c.second->RecalcAll();
        }

        if (YdbCounters) {
            YdbCounters->Initialize(Counters, CountersByTabletType);
            YdbCounters->Transform();
        }
    }

    void RemoveTabletsByPathId(TPathId pathId) {
        CountersByPathId.erase(pathId);
    }

private:
    // subgroups
    class TTabletCountersForTabletType {
    public:
        //
        TTabletCountersForTabletType(NMonitoring::TDynamicCounters* owner, const char* category, const char* name)
            : TabletCountersSection(owner->GetSubgroup(category, name))
            , TabletExecutorCountersSection(TabletCountersSection->GetSubgroup("category", "executor"))
            , TabletAppCountersSection(TabletCountersSection->GetSubgroup("category", "app"))
            , TabletExecutorCounters(TabletExecutorCountersSection)
            , TabletAppCounters(TabletAppCountersSection)
        {}

        void Apply(ui64 tabletID,
            const TTabletCountersBase* executorCounters,
            const TTabletCountersBase* appCounters,
            TTabletTypes::EType tabletType,
            const TTabletCountersBase* limitedAppCounters = {})
        {
            Y_VERIFY(executorCounters);

            if (executorCounters) {
                if (!TabletExecutorCounters.IsInitialized) {
                    TabletExecutorCounters.Initialize(executorCounters);
                }
                TabletExecutorCounters.Apply(tabletID, executorCounters, tabletType);
            }

            if (appCounters) {
                if (!TabletAppCounters.IsInitialized) {
                    TabletAppCounters.Initialize(limitedAppCounters ? limitedAppCounters : appCounters);
                }
                TabletAppCounters.Apply(tabletID, appCounters, tabletType);
            }
        }

        void Forget(ui64 tabletId) {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.Forget(tabletId);
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.Forget(tabletId);
            }
        }

        void RecalcAll() {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.RecalcAll();
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.RecalcAll();
            }
        }

        // db counters

        bool IsInitialized() const {
            return TabletExecutorCounters.IsInitialized;
        }

        void Initialize(const TTabletCountersBase* executorCounters, const TTabletCountersBase* appCounters) {
            Y_VERIFY(executorCounters);

            if (!TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.Initialize(executorCounters);
            }

            if (appCounters && !TabletAppCounters.IsInitialized) {
                TabletAppCounters.Initialize(appCounters);
            }
        }

        void ToProto(NKikimrSysView::TDbTabletCounters& tabletCounters) {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.RecalcAll();
                TabletExecutorCounters.ToProto(*tabletCounters.MutableExecutorCounters(),
                    *tabletCounters.MutableMaxExecutorCounters());
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.RecalcAll();
                TabletAppCounters.ToProto(*tabletCounters.MutableAppCounters(),
                    *tabletCounters.MutableMaxAppCounters());
            }
        }

        void FromProto(NKikimrSysView::TDbTabletCounters& tabletCounters) {
            if (TabletExecutorCounters.IsInitialized) {
                TabletExecutorCounters.FromProto(*tabletCounters.MutableExecutorCounters(),
                    *tabletCounters.MutableMaxExecutorCounters());
            }
            if (TabletAppCounters.IsInitialized) {
                TabletAppCounters.FromProto(*tabletCounters.MutableAppCounters(),
                    *tabletCounters.MutableMaxAppCounters());
            }
        }

    private:
        //
        class TSolomonCounters {
        public:
            //
            bool IsInitialized;

            TSolomonCounters(NMonitoring::TDynamicCounterPtr counterGroup)
                : IsInitialized(false)
                , AggregatedSimpleCounters(counterGroup)
                , AggregatedCumulativeCounters(counterGroup)
                , AggregatedHistogramCounters(counterGroup)
                , CounterGroup(counterGroup)
            {}

            void Initialize(const TTabletCountersBase* counters) {
                Y_VERIFY(!IsInitialized);

                if (counters) {
                    THashMap<TString, THolder<THistogramCounter>> histogramAggregates;

                    // percentile counters
                    FullSizePercentile = counters->Percentile().Size();
                    AggregatedHistogramCounters.Reserve(FullSizePercentile);
                    for (ui32 i = 0; i < FullSizePercentile; ++i) {
                        if (!counters->PercentileCounterName(i)) {
                            DeprecatedPercentile.insert(i);
                            continue;
                        }

                        auto& percentileCounter = counters->Percentile()[i];
                        const char* percentileCounterName = counters->PercentileCounterName(i);
                        AggregatedHistogramCounters.AddCounter(
                            percentileCounterName,
                            percentileCounter,
                            histogramAggregates);
                    }

                    // simple counters
                    FullSizeSimple = counters->Simple().Size();
                    AggregatedSimpleCounters.Reserve(FullSizeSimple);
                    for (ui32 i = 0; i < FullSizeSimple; ++i) {
                        const char* name = counters->SimpleCounterName(i);
                        if (!name) {
                            DeprecatedSimple.insert(i);
                            continue;
                        }
                        auto itHistogramAggregate = histogramAggregates.find(name);
                        if (itHistogramAggregate != histogramAggregates.end()) {
                            AggregatedSimpleCounters.AddSimpleCounter(name, std::move(itHistogramAggregate->second));
                        } else {
                            AggregatedSimpleCounters.AddSimpleCounter(name);
                        }
                    }

                    // cumulative counters
                    FullSizeCumulative = counters->Cumulative().Size();
                    AggregatedCumulativeCounters.Reserve(FullSizeSimple);
                    for (ui32 i = 0; i < FullSizeCumulative; ++i) {
                        const char* name = counters->CumulativeCounterName(i);
                        if (!name) {
                            DeprecatedCumulative.insert(i);
                            continue;
                        }
                        auto itHistogramAggregate = histogramAggregates.find(name);
                        if (itHistogramAggregate != histogramAggregates.end()) {
                            AggregatedCumulativeCounters.AddCumulativeCounter(name, std::move(itHistogramAggregate->second));
                        } else {
                            AggregatedCumulativeCounters.AddCumulativeCounter(name);
                        }
                        auto counter = CounterGroup->GetCounter(name, true);
                        CumulativeCounters.push_back(counter);
                    }
                }

                //
                IsInitialized = true;
            }

            void Apply(ui64 tabletID, const TTabletCountersBase* counters, TTabletTypes::EType tabletType) {
                Y_VERIFY(counters);

                TInstant now = TInstant::Now();
                auto it = LastAggregateUpdateTime.find(tabletID);
                TDuration diff;
                if (it != LastAggregateUpdateTime.end()) {
                    diff = now - it->second;
                    it->second = now;
                } else {
                    LastAggregateUpdateTime.emplace(tabletID, now);
                }

                // simple counters
                ui32 nextSimpleOffset = 0;
                for (ui32 i = 0; i < FullSizeSimple; ++i) {
                    if (!counters->SimpleCounterName(i)) {
                        continue;
                    }
                    const ui32 offset = nextSimpleOffset++;
                    const ui64 value = counters->Simple()[i].Get();
                    AggregatedSimpleCounters.SetValue(tabletID, offset, value, tabletType);
                }

                // cumulative counters
                ui32 nextCumulativeOffset = 0;
                for (ui32 i = 0; i < FullSizeCumulative; ++i) {
                    if (!counters->CumulativeCounterName(i)) {
                        continue;
                    }
                    const ui32 offset = nextCumulativeOffset++;
                    const ui64 valueDiff = counters->Cumulative()[i].Get();
                    if (diff) {
                        const ui64 diffValue = valueDiff * 1000000 / diff.MicroSeconds(); // differentiate value to per second rate
                        AggregatedCumulativeCounters.SetValue(tabletID, offset, diffValue, tabletType);
                    }
                    Y_VERIFY(offset < CumulativeCounters.size(), "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
                    *CumulativeCounters[offset] += valueDiff;
                }

                // percentile counters
                ui32 nextPercentileOffset = 0;
                for (ui32 i = 0; i < FullSizePercentile; ++i) {
                    if (!counters->PercentileCounterName(i)) {
                        continue;
                    }

                    const ui32 offset = nextPercentileOffset++;
                    AggregatedHistogramCounters.SetValue(
                        tabletID,
                        offset,
                        counters->Percentile()[i],
                        counters->PercentileCounterName(i),
                        tabletType);
                }
            }

            void Forget(ui64 tabletId) {
                Y_VERIFY(IsInitialized);

                AggregatedSimpleCounters.ForgetTablet(tabletId);
                AggregatedCumulativeCounters.ForgetTablet(tabletId);
                AggregatedHistogramCounters.ForgetTablet(tabletId);
                LastAggregateUpdateTime.erase(tabletId);
            }

            void RecalcAll() {
                AggregatedSimpleCounters.RecalcAll();
                AggregatedCumulativeCounters.RecalcAll();
            }

            template <bool IsSaving>
            void Convert(NKikimrSysView::TDbCounters& sumCounters,
                NKikimrSysView::TDbCounters& maxCounters)
            {
                // simple counters
                auto* simpleSum = sumCounters.MutableSimple();
                auto* simpleMax = maxCounters.MutableSimple();
                simpleSum->Resize(FullSizeSimple, 0);
                simpleMax->Resize(FullSizeSimple, 0);
                ui32 nextSimpleOffset = 0;
                for (ui32 i = 0; i < FullSizeSimple; ++i) {
                    if (DeprecatedSimple.find(i) != DeprecatedSimple.end()) {
                        if constexpr (IsSaving) {
                            (*simpleSum)[i] = 0;
                            (*simpleMax)[i] = 0;
                        }
                        continue;
                    }
                    const ui32 offset = nextSimpleOffset++;
                    if constexpr (IsSaving) {
                        (*simpleSum)[i] = AggregatedSimpleCounters.GetSum(offset);
                        (*simpleMax)[i] = AggregatedSimpleCounters.GetMax(offset);
                    } else {
                        AggregatedSimpleCounters.SetSum(offset, (*simpleSum)[i]);
                        AggregatedSimpleCounters.SetMax(offset, (*simpleMax)[i]);
                    }
                }
                // cumulative counters
                auto* cumulativeSum = sumCounters.MutableCumulative();
                auto* cumulativeMax = maxCounters.MutableCumulative();
                cumulativeSum->Resize(FullSizeCumulative, 0);
                cumulativeMax->Resize(FullSizeCumulative, 0);
                ui32 nextCumulativeOffset = 0;
                for (ui32 i = 0; i < FullSizeCumulative; ++i) {
                    if (DeprecatedCumulative.find(i) != DeprecatedCumulative.end()) {
                        if constexpr (IsSaving) {
                            (*cumulativeSum)[i] = 0;
                            (*cumulativeMax)[i] = 0;
                        }
                        continue;
                    }
                    const ui32 offset = nextCumulativeOffset++;
                    Y_VERIFY(offset < CumulativeCounters.size(),
                        "inconsistent cumulative counters %u >= %lu", offset, CumulativeCounters.size());
                    if constexpr (IsSaving) {
                        (*cumulativeSum)[i] = *CumulativeCounters[offset];
                        (*cumulativeMax)[i] = AggregatedCumulativeCounters.GetMax(offset);
                    } else {
                        *CumulativeCounters[offset] = (*cumulativeSum)[i];
                        AggregatedCumulativeCounters.SetMax(offset, (*cumulativeMax)[i]);
                    }
                }
                // percentile counters
                auto* histogramSum = sumCounters.MutableHistogram();
                if (sumCounters.HistogramSize() < FullSizePercentile) {
                    auto missing = FullSizePercentile - sumCounters.HistogramSize();
                    for (; missing > 0; --missing) {
                        sumCounters.AddHistogram();
                    }
                }
                ui32 nextPercentileOffset = 0;
                for (ui32 i = 0; i < FullSizePercentile; ++i) {
                    if (DeprecatedPercentile.find(i) != DeprecatedPercentile.end()) {
                        continue;
                    }
                    auto* buckets = (*histogramSum)[i].MutableBuckets();
                    const ui32 offset = nextPercentileOffset++;
                    auto histogram = AggregatedHistogramCounters.GetHistogram(offset);
                    auto snapshot = histogram->Snapshot();
                    auto count = snapshot->Count();
                    buckets->Resize(count, 0);
                    if constexpr (!IsSaving) {
                        histogram->Reset();
                    }
                    for (ui32 r = 0; r < count; ++r) {
                        if constexpr (IsSaving) {
                            (*buckets)[r] = snapshot->Value(r);
                        } else {
                            histogram->Collect(snapshot->UpperBound(r), (*buckets)[r]);
                        }
                    }
                }
            }

            void ToProto(NKikimrSysView::TDbCounters& sumCounters,
                NKikimrSysView::TDbCounters& maxCounters)
            {
                Convert<true>(sumCounters, maxCounters);
            }

            void FromProto(NKikimrSysView::TDbCounters& sumCounters,
                NKikimrSysView::TDbCounters& maxCounters)
            {
                Convert<false>(sumCounters, maxCounters);
            }

        private:
            ui32 FullSizeSimple = 0;
            THashSet<ui32> DeprecatedSimple;
            ui32 FullSizeCumulative = 0;
            THashSet<ui32> DeprecatedCumulative;
            ui32 FullSizePercentile = 0;
            THashSet<ui32> DeprecatedPercentile;
            //
            TAggregatedSimpleCounters AggregatedSimpleCounters;

            TCountersVector CumulativeCounters;
            TAggregatedCumulativeCounters AggregatedCumulativeCounters;

            TAggregatedHistogramCounters AggregatedHistogramCounters;

            THashMap<ui64, TInstant> LastAggregateUpdateTime;

            NMonitoring::TDynamicCounterPtr CounterGroup;
        };

        //
        NMonitoring::TDynamicCounterPtr TabletCountersSection;

        NMonitoring::TDynamicCounterPtr TabletExecutorCountersSection;
        NMonitoring::TDynamicCounterPtr TabletAppCountersSection;

        TSolomonCounters TabletExecutorCounters;
        TSolomonCounters TabletAppCounters;
    };

    typedef TMap<TTabletTypes::EType, TAutoPtr<TTabletCountersForTabletType> > TCountersByTabletType;

    static TTabletCountersForTabletType* FindCountersByTabletType(
        TTabletTypes::EType tabletType,
        TCountersByTabletType& countersByTabletType)
    {
        auto iterTabletType = countersByTabletType.find(tabletType);
        if (iterTabletType != countersByTabletType.end()) {
            return iterTabletType->second.Get();
        }
        return {};
    }

    static TTabletCountersForTabletType* GetOrAddCountersByTabletType(
        TTabletTypes::EType tabletType,
        TCountersByTabletType& countersByTabletType,
        NMonitoring::TDynamicCounterPtr counters)
    {
        auto* typeCounters = FindCountersByTabletType(tabletType, countersByTabletType);
        if (!typeCounters) {
            TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
            typeCounters = new TTabletCountersForTabletType(
                counters.Get(), "type", tabletTypeStr.data());
            countersByTabletType.emplace(tabletType, typeCounters);
        }
        return typeCounters;
    }

    const TTabletCountersBase* GetOrAddLimitedAppCounters(TTabletTypes::EType tabletType) {
        if (auto it = LimitedAppCounters.find(tabletType); it != LimitedAppCounters.end()) {
            return it->second.Get();
        }
        auto appCounters = CreateAppCountersByTabletType(tabletType);
        return LimitedAppCounters.emplace(tabletType, std::move(appCounters)).first->second.Get();
    }

    class TYdbTabletCounters : public TThrRefBase {
        using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;
        using THistogramPtr = NMonitoring::THistogramPtr;

    private:
        TCounterPtr WriteRowCount;
        TCounterPtr WriteBytes;
        TCounterPtr ReadRowCount;
        TCounterPtr ReadBytes;
        TCounterPtr EraseRowCount;
        TCounterPtr EraseBytes;
        TCounterPtr BulkUpsertRowCount;
        TCounterPtr BulkUpsertBytes;
        TCounterPtr ScanRowCount;
        TCounterPtr ScanBytes;
        TCounterPtr DatashardRowCount;
        TCounterPtr DatashardSizeBytes;
        TCounterPtr ResourcesStorageUsedBytes;
        TCounterPtr ResourcesStorageLimitBytes;
        TCounterPtr ResourcesStreamUsedShards;
        TCounterPtr ResourcesStreamLimitShards;
        //TCounterPtr ResourcesStreamUsedShardsPercents;
        TCounterPtr ResourcesStreamReservedThroughput;
        TCounterPtr ResourcesStreamReservedStorage;
        TCounterPtr ResourcesStreamReservedStorageLimit;

        THistogramPtr ShardCpuUtilization;

        TCounterPtr RowUpdates;
        TCounterPtr RowUpdateBytes;
        TCounterPtr RowReads;
        TCounterPtr RangeReadRows;
        TCounterPtr RowReadBytes;
        TCounterPtr RangeReadBytes;
        TCounterPtr RowErases;
        TCounterPtr RowEraseBytes;
        TCounterPtr UploadRows;
        TCounterPtr UploadRowsBytes;
        TCounterPtr ScannedRows;
        TCounterPtr ScannedBytes;
        TCounterPtr DbUniqueRowsTotal;
        TCounterPtr DbUniqueDataBytes;
        THistogramPtr ConsumedCpuHistogram;

        TCounterPtr DiskSpaceTablesTotalBytes;
        TCounterPtr DiskSpaceSoftQuotaBytes;

        TCounterPtr StreamShardsCount;
        TCounterPtr StreamShardsQuota;
        TCounterPtr StreamReservedThroughput;
        TCounterPtr StreamReservedStorage;
        TCounterPtr StreamReservedStorageLimit;


    public:
        explicit TYdbTabletCounters(const NMonitoring::TDynamicCounterPtr& ydbGroup) {
            WriteRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.write.rows", true);
            WriteBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.write.bytes", true);
            ReadRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.read.rows", true);
            ReadBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.read.bytes", true);
            EraseRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.erase.rows", true);
            EraseBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.erase.bytes", true);
            BulkUpsertRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.bulk_upsert.rows", true);
            BulkUpsertBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.bulk_upsert.bytes", true);
            ScanRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.scan.rows", true);
            ScanBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.scan.bytes", true);

            DatashardRowCount = ydbGroup->GetNamedCounter("name",
                "table.datashard.row_count", false);
            DatashardSizeBytes = ydbGroup->GetNamedCounter("name",
                "table.datashard.size_bytes", false);

            ResourcesStorageUsedBytes = ydbGroup->GetNamedCounter("name",
                "resources.storage.used_bytes", false);
            ResourcesStorageLimitBytes = ydbGroup->GetNamedCounter("name",
                "resources.storage.limit_bytes", false);

            ResourcesStreamUsedShards = ydbGroup->GetNamedCounter("name",
                "resources.stream.used_shards", false);
            ResourcesStreamLimitShards = ydbGroup->GetNamedCounter("name",
                "resources.stream.limit_shards", false);

            //ResourcesStreamUsedShardsPercents = ydbGroup->GetNamedCounter("name",
            //    "resources.stream.used_shards_percents", false);

            ResourcesStreamReservedThroughput = ydbGroup->GetNamedCounter("name",
                "resources.stream.throughput.limit_bytes_per_second", false);

            ResourcesStreamReservedStorage = ydbGroup->GetNamedCounter("name",
                "resources.stream.storage.reserved_bytes", false);

            ResourcesStreamReservedStorageLimit = ydbGroup->GetNamedCounter("name",
                "resources.stream.storage.limit_bytes", false);

            ShardCpuUtilization = ydbGroup->GetNamedHistogram("name",
                "table.datashard.used_core_percents", NMonitoring::LinearHistogram(12, 0, 10), false);
        };

        void Initialize(
            NMonitoring::TDynamicCounterPtr counters,
            TCountersByTabletType& countersByTabletType)
        {
            auto datashard = FindCountersByTabletType(
                TTabletTypes::FLAT_DATASHARD, countersByTabletType);

            if (datashard && !RowUpdates) {
                auto datashardGroup = counters->GetSubgroup("type", "DataShard");
                auto appGroup = datashardGroup->GetSubgroup("category", "app");

                RowUpdates = appGroup->GetCounter("DataShard/EngineHostRowUpdates");
                RowUpdateBytes = appGroup->GetCounter("DataShard/EngineHostRowUpdateBytes");
                RowReads = appGroup->GetCounter("DataShard/EngineHostRowReads");
                RangeReadRows = appGroup->GetCounter("DataShard/EngineHostRangeReadRows");
                RowReadBytes = appGroup->GetCounter("DataShard/EngineHostRowReadBytes");
                RangeReadBytes = appGroup->GetCounter("DataShard/EngineHostRangeReadBytes");
                RowErases = appGroup->GetCounter("DataShard/EngineHostRowErases");
                RowEraseBytes = appGroup->GetCounter("DataShard/EngineHostRowEraseBytes");
                UploadRows = appGroup->GetCounter("DataShard/UploadRows");
                UploadRowsBytes = appGroup->GetCounter("DataShard/UploadRowsBytes");
                ScannedRows = appGroup->GetCounter("DataShard/ScannedRows");
                ScannedBytes = appGroup->GetCounter("DataShard/ScannedBytes");

                auto execGroup = datashardGroup->GetSubgroup("category", "executor");

                DbUniqueRowsTotal = execGroup->GetCounter("SUM(DbUniqueRowsTotal)");
                DbUniqueDataBytes = execGroup->GetCounter("SUM(DbUniqueDataBytes)");
                ConsumedCpuHistogram = execGroup->FindHistogram("HIST(ConsumedCPU)");
            }

            auto schemeshard = FindCountersByTabletType(
                TTabletTypes::FLAT_SCHEMESHARD, countersByTabletType);

            if (schemeshard && !DiskSpaceTablesTotalBytes) {
                auto schemeshardGroup = counters->GetSubgroup("type", "SchemeShard");
                auto appGroup = schemeshardGroup->GetSubgroup("category", "app");

                DiskSpaceTablesTotalBytes = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceTablesTotalBytes)");
                DiskSpaceSoftQuotaBytes = appGroup->GetCounter("SUM(SchemeShard/DiskSpaceSoftQuotaBytes)");

                StreamShardsCount = appGroup->GetCounter("SUM(SchemeShard/StreamShardsCount)");
                StreamShardsQuota = appGroup->GetCounter("SUM(SchemeShard/StreamShardsQuota)");
                StreamReservedThroughput = appGroup->GetCounter("SUM(SchemeShard/StreamReservedThroughput)");
                StreamReservedStorage = appGroup->GetCounter("SUM(SchemeShard/StreamReservedStorage)");
                StreamReservedStorageLimit = appGroup->GetCounter("SUM(SchemeShard/StreamReservedStorageQuota)");

            }
        }

        void Transform() {
            if (RowUpdates) {
                WriteRowCount->Set(RowUpdates->Val());
                WriteBytes->Set(RowUpdateBytes->Val());
                ReadRowCount->Set(RowReads->Val() + RangeReadRows->Val());
                ReadBytes->Set(RowReadBytes->Val() + RangeReadBytes->Val());
                EraseRowCount->Set(RowErases->Val());
                EraseBytes->Set(RowEraseBytes->Val());
                BulkUpsertRowCount->Set(UploadRows->Val());
                BulkUpsertBytes->Set(UploadRowsBytes->Val());
                ScanRowCount->Set(ScannedRows->Val());
                ScanBytes->Set(ScannedBytes->Val());
                DatashardRowCount->Set(DbUniqueRowsTotal->Val());
                DatashardSizeBytes->Set(DbUniqueDataBytes->Val());

                if (ConsumedCpuHistogram) {
                    TransferBuckets(ShardCpuUtilization, ConsumedCpuHistogram);
                }
            }

            if (DiskSpaceTablesTotalBytes) {
                ResourcesStorageUsedBytes->Set(DiskSpaceTablesTotalBytes->Val());
                ResourcesStorageLimitBytes->Set(DiskSpaceSoftQuotaBytes->Val());

                auto quota = StreamShardsQuota->Val();
                ResourcesStreamUsedShards->Set(StreamShardsCount->Val());
                ResourcesStreamLimitShards->Set(quota);
                /*if (quota > 0) {
                    ResourcesStreamUsedShardsPercents->Set(StreamShardsCount->Val() * 100.0 / (quota + 0.0));
                } else {
                    ResourcesStreamUsedShardsPercents->Set(0.0);
                }*/

                ResourcesStreamReservedThroughput->Set(StreamReservedThroughput->Val());
                ResourcesStreamReservedStorage->Set(StreamReservedStorage->Val());
                ResourcesStreamReservedStorageLimit->Set(StreamReservedStorageLimit->Val());
            }
        }

        void TransferBuckets(THistogramPtr dst, THistogramPtr src) {
            auto srcSnapshot = src->Snapshot();
            auto srcCount = srcSnapshot->Count();
            auto dstSnapshot = dst->Snapshot();
            auto dstCount = dstSnapshot->Count();

            dst->Reset();
            for (ui32 b = 0; b < std::min(srcCount, dstCount); ++b) {
                dst->Collect(dstSnapshot->UpperBound(b), srcSnapshot->Value(b));
            }
        }
    };

    using TYdbTabletCountersPtr = TIntrusivePtr<TYdbTabletCounters>;

public:
    class TTabletCountersForDb : public NSysView::IDbCounters {
    public:
        TTabletCountersForDb()
            : SolomonCounters(new NMonitoring::TDynamicCounters)
            , AllTypes(SolomonCounters.Get(), "type", "all")
        {}

        TTabletCountersForDb(NMonitoring::TDynamicCounterPtr externalGroup,
            NMonitoring::TDynamicCounterPtr internalGroup,
            THolder<TTabletCountersBase> executorCounters)
            : SolomonCounters(internalGroup->GetSubgroup("group", "tablets"))
            , ExecutorCounters(std::move(executorCounters))
            , AllTypes(SolomonCounters.Get(), "type", "all")
        {
            YdbCounters = MakeIntrusive<TYdbTabletCounters>(externalGroup);
        }

        void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
            auto* proto = counters.FindOrAddTabletCounters(TTabletTypes::Unknown);
            AllTypes.ToProto(*proto);

            for (auto& [type, tabletCounters] : CountersByTabletType) {
                auto* proto = counters.FindOrAddTabletCounters(type);
                tabletCounters->ToProto(*proto);
            }
        }

        void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) override {
            for (auto& proto : *counters.Proto().MutableTabletCounters()) {
                auto type = proto.GetType();
                TTabletCountersForTabletType* tabletCounters = {};
                if (type == TTabletTypes::Unknown) {
                    tabletCounters = &AllTypes;
                } else {
                    tabletCounters = GetOrAddCountersByTabletType(type, CountersByTabletType, SolomonCounters);
                }
                if (tabletCounters) {
                    if (!tabletCounters->IsInitialized()) {
                        Y_VERIFY(ExecutorCounters.Get());
                        auto appCounters = CreateAppCountersByTabletType(type);
                        tabletCounters->Initialize(ExecutorCounters.Get(), appCounters.Get());
                    }
                    tabletCounters->FromProto(proto);
                }
            }
            if (YdbCounters) {
                YdbCounters->Initialize(SolomonCounters, CountersByTabletType);
                YdbCounters->Transform();
            }
        }

        void Apply(ui64 tabletId, const TTabletCountersBase* executorCounters,
            const TTabletCountersBase* appCounters, TTabletTypes::EType type,
            const TTabletCountersBase* limitedAppCounters)
        {
            AllTypes.Apply(tabletId, executorCounters, nullptr, type);
            auto* tabletCounters = GetOrAddCountersByTabletType(type, CountersByTabletType, SolomonCounters);
            if (tabletCounters) {
                tabletCounters->Apply(tabletId, executorCounters, appCounters, type, limitedAppCounters);
            }
        }

        void Forget(ui64 tabletId, TTabletTypes::EType type) {
            if (auto it = CountersByTabletType.find(type); it != CountersByTabletType.end()) {
                it->second->Forget(tabletId);
            }
        }

    private:
        NMonitoring::TDynamicCounterPtr SolomonCounters;
        THolder<TTabletCountersBase> ExecutorCounters;

        TTabletCountersForTabletType AllTypes;
        TCountersByTabletType CountersByTabletType;

        TYdbTabletCountersPtr YdbCounters;
    };

    class TTabletsDbWatcherCallback : public NKikimr::NSysView::TDbWatcherCallback {
        TActorSystem* ActorSystem = {};

    public:
        explicit TTabletsDbWatcherCallback(TActorSystem* actorSystem)
            : ActorSystem(actorSystem)
        {}

        void OnDatabaseRemoved(const TString&, TPathId pathId) override {
            auto evRemove = MakeHolder<TEvTabletCounters::TEvRemoveDatabase>(pathId);
            auto aggregator = MakeTabletCountersAggregatorID(ActorSystem->NodeId, false);
            ActorSystem->Send(aggregator, evRemove.Release());
        }
    };

private:
    TIntrusivePtr<TTabletCountersForDb> GetDbCounters(TPathId pathId, const TActorContext& ctx) {
        auto it = CountersByPathId.find(pathId);
        if (it != CountersByPathId.end()) {
            return it->second;
        }

        auto dbCounters = MakeIntrusive<TTabletMon::TTabletCountersForDb>();
        CountersByPathId[pathId] = dbCounters;

        auto evRegister = MakeHolder<NSysView::TEvSysView::TEvRegisterDbCounters>(
            NKikimrSysView::TABLETS, pathId, dbCounters);
        ctx.Send(NSysView::MakeSysViewServiceID(ctx.SelfID.NodeId()), evRegister.Release());

        if (DbWatcherActorId) {
            auto evWatch = MakeHolder<NSysView::TEvSysView::TEvWatchDatabase>(pathId);
            ctx.Send(DbWatcherActorId, evWatch.Release());
        }

        return dbCounters;
    }

private:
    //
    NMonitoring::TDynamicCounterPtr Counters;
    TTabletCountersForTabletType AllTypes;
    bool IsFollower = false;

    typedef THashMap<TPathId, TIntrusivePtr<TTabletCountersForDb>> TCountersByPathId;
    typedef TMap<TTabletTypes::EType, THolder<TTabletCountersBase>> TAppCountersByTabletType;

    typedef TMap<std::pair<TTabletTypes::EType, TString>, TAutoPtr<TAggregatedLabeledCounters> > TLabeledCountersByTabletTypeAndGroup;

    TCountersByTabletType CountersByTabletType;
    TCountersByPathId CountersByPathId;
    TActorId DbWatcherActorId;
    TAppCountersByTabletType LimitedAppCounters; // without txs

    TYdbTabletCountersPtr YdbCounters;

    TLabeledCountersByTabletTypeAndGroup LabeledCountersByTabletTypeAndGroup;
    THashMap<ui64, std::pair<TAutoPtr<TTabletCountersBase>, TAutoPtr<TTabletCountersBase>>> QuietTabletCounters;
};


TIntrusivePtr<NSysView::IDbCounters> CreateTabletDbCounters(
    NMonitoring::TDynamicCounterPtr externalGroup,
    NMonitoring::TDynamicCounterPtr internalGroup,
    THolder<TTabletCountersBase> executorCounters)
{
    return MakeIntrusive<TTabletMon::TTabletCountersForDb>(
        externalGroup, internalGroup, std::move(executorCounters));
}

////////////////////////////////////////////
/// The TTabletCountersAggregatorActor class
////////////////////////////////////////////
class TTabletCountersAggregatorActor : public TActorBootstrapped<TTabletCountersAggregatorActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COUNTERS_AGGREGATOR;
    }

    //
    TTabletCountersAggregatorActor(bool follower);
    virtual ~TTabletCountersAggregatorActor();

    //
    void Bootstrap(const TActorContext &ctx);

    //
    STFUNC(StateWork);

private:
    //
    void HandleWork(TEvTabletCounters::TEvTabletAddCounters::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletCountersForgetTablet::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletCountersRequest::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletAddLabeledCounters::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletLabeledCountersRequest::TPtr &ev, const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx);//from cluster aggregator
    void HandleWork(NMon::TEvHttpInfo::TPtr& ev, const TActorContext &ctx);
    void HandleWakeup(const TActorContext &ctx);
    void HandleWork(TEvTabletCounters::TEvRemoveDatabase::TPtr& ev);

    //
    TAutoPtr<TTabletMon> TabletMon;
    TActorId DbWatcherActorId;
    THashMap<TActorId, std::pair<TActorId, TAutoPtr<NMon::TEvHttpInfo>>> HttpRequestHandlers;
    THashSet<ui32> TabletTypeOfReceivedLabeledCounters;
    bool Follower;
};

////////////////////////////////////////////
/// The TTabletCountersAggregatorActor class
////////////////////////////////////////////
TTabletCountersAggregatorActor::TTabletCountersAggregatorActor(bool follower)
    : Follower(follower)
{}

////////////////////////////////////////////
TTabletCountersAggregatorActor::~TTabletCountersAggregatorActor()
{}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);

    TAppData* appData = AppData(ctx);
    Y_VERIFY(!TabletMon);

    if (AppData(ctx)->FeatureFlags.GetEnableDbCounters() && !Follower) {
        auto callback = MakeIntrusive<TTabletMon::TTabletsDbWatcherCallback>(ctx.ActorSystem());
        DbWatcherActorId = ctx.Register(NSysView::CreateDbWatcherActor(callback));
    }

    TabletMon = new TTabletMon(appData->Counters, Follower, DbWatcherActorId);
    auto mon = appData->Mon;
    if (mon) {
        if (!Follower)
            mon->RegisterActorPage(nullptr, "labeledcounters", "Labeled Counters", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId(), false);
        else
            mon->RegisterActorPage(nullptr, "followercounters", "Follower Counters", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId(), false);
    }

    ctx.Schedule(TDuration::Seconds(WAKEUP_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
}


////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletAddCounters::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    TEvTabletCounters::TEvTabletAddCounters* msg = ev->Get();
    TabletMon->Apply(msg->TabletID, msg->TabletType, msg->TenantPathId, msg->ExecutorCounters.Get(), msg->AppCounters.Get(), ctx);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletAddLabeledCounters::TPtr &ev, const TActorContext &ctx) {
    TEvTabletCounters::TEvTabletAddLabeledCounters* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "got labeledCounters from tablet " << msg->TabletID);
    TabletMon->ApplyLabeledCounters(msg->TabletID, msg->TabletType, msg->LabeledCounters.Get());

    TabletTypeOfReceivedLabeledCounters.insert(msg->TabletType);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletCountersForgetTablet::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    TEvTabletCounters::TEvTabletCountersForgetTablet* msg = ev->Get();
    TabletMon->ForgetTablet(msg->TabletID, msg->TabletType, msg->TenantPathId);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletCountersRequest::TPtr &ev, const TActorContext &ctx) {
    TEvTabletCounters::TEvTabletCountersRequest* msg = ev->Get();
    TAutoPtr<TEvTabletCounters::TEvTabletCountersResponse> resp = new TEvTabletCounters::TEvTabletCountersResponse();
    TabletMon->Query(msg->Record, resp->Record);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletLabeledCountersRequest::TPtr &ev, const TActorContext &ctx) {
    TEvTabletCounters::TEvTabletLabeledCountersRequest* msg = ev->Get();
    TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersResponse> resp = new TEvTabletCounters::TEvTabletLabeledCountersResponse();
    TabletMon->QueryLabeledCounters(msg->Record, resp->Record, ctx);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
    auto& response = ev->Get()->Record;

    auto it = HttpRequestHandlers.find(ev->Sender);
    if (it == HttpRequestHandlers.end())
        return;
    TString html;
    TStringOutput oss(html);
    NMonitoring::TDynamicCounters counters;
    const auto& params = it->second.second->Request.GetParams();
    TString reqTabletType = params.Get("type");

    auto mainGroup = counters.GetSubgroup("user_counters", reqTabletType);

    bool parsePQTopic = params.Has("parse_pq");
    ui32 version = 0;
    if (parsePQTopic) {
        TryFromString(params.Get("parse_pq"), version);
    }

    for (ui32 i = 0; i < response.LabeledCountersByGroupSize(); ++i) {
        const auto ucByGroup = response.GetLabeledCountersByGroup(i);
        TVector<TString> groups;
        TVector<TString> groupNames;
        Y_VERIFY(ucByGroup.GetDelimiter() == "/");
        StringSplitter(ucByGroup.GetGroup()).Split('/').SkipEmpty().Collect(&groups);

        if (parsePQTopic) {
            bool skip = false;
            for (ui32 j = 0; j < groups.size(); ++j) {
                if (groups[j] == "total") {
                    skip = true;
                    break;
                }
            }
            if (skip)
                continue;
        }

        StringSplitter(ucByGroup.GetGroupNames()).Split('/').SkipEmpty().Collect(&groupNames);
        Y_VERIFY(groups.size() == groupNames.size(), "%s and %s", ucByGroup.GetGroup().c_str(), ucByGroup.GetGroupNames().c_str());
        auto group = mainGroup;
        for (ui32 j = 0; j < groups.size(); ++j) {
            if (parsePQTopic) {
                groupNames[j] = TString(1, toupper(groupNames[j][0])) + groupNames[j].substr(1);
                if (groupNames[j] == "Topic") {
                    if (NPersQueue::CorrectName(groups[j])) {
                        TString dc = to_title(NPersQueue::GetDC(groups[j]));
                        TString producer = NPersQueue::GetProducer(groups[j]);
                        TString topic = NPersQueue::GetRealTopic(groups[j]);
                        group = group->GetSubgroup("OriginDC", dc);
                        if (version > 1) {
                            topic = NPersQueue::GetTopicPath(groups[j]);
                            producer = NPersQueue::GetAccount(groups[j]);
                            group = group->GetSubgroup("Account", producer);
                            group = group->GetSubgroup("TopicPath", topic);
                        } else {
                            group = group->GetSubgroup("Producer", producer);
                            group = group->GetSubgroup("Topic", topic);
                        }
                    } else {
                        if (version > 1) {
                            group = group->GetSubgroup("OriginDC", "unknown");
                            group = group->GetSubgroup("Account", "unknown");
                            group = group->GetSubgroup("TopicPath", groups[j]);
                        } else {
                            group = group->GetSubgroup("OriginDC", "unknown");
                            group = group->GetSubgroup("Producer", "unknown");
                            group = group->GetSubgroup("Topic", groups[j]);
                        }
                    }
                    continue;
                }
                if (groupNames[j] == "Client") {
                    group = group->GetSubgroup("ConsumerPath", NPersQueue::ConvertOldConsumerName(groups[j], ctx));
                    continue;
                }
            }
            group = group->GetSubgroup(groupNames[j], groups[j]);
        }
        for (ui32 j = 0; j < ucByGroup.LabeledCounterSize(); ++j) {
            const auto& uc = ucByGroup.GetLabeledCounter(j);
            bool deriv = (uc.GetType() == TLabeledCounterOptions::CT_DERIV);
            TString counterName = uc.GetName();
            if (parsePQTopic && counterName.StartsWith("PQ/")) {
                counterName = counterName.substr(3);
            }
            *group->GetCounter(counterName, deriv).Get() = uc.GetValue();
        }
    }

    bool json = params.Has("json");
    bool spack = params.Has("spack");
    if (json) {
        oss << NMonitoring::HTTPOKJSON;
        auto encoder = NMonitoring::CreateEncoder(&oss, NMonitoring::EFormat::JSON);
        counters.Accept(TString(), TString(), *encoder);
    } else if (spack) {
        oss.Write(NMonitoring::HTTPOKSPACK);
        auto encoder = NMonitoring::CreateEncoder(&oss, NMonitoring::EFormat::SPACK);
        counters.Accept(TString(), TString(), *encoder);
    } else {
        counters.OutputHtml(oss);
    }
    oss.Flush();
    ctx.Send(it->second.first, new NMon::TEvHttpInfoRes(html, 0, (json || spack) ? NMon::TEvHttpInfoRes::Custom : NMon::TEvHttpInfoRes::Html));
    HttpRequestHandlers.erase(it);
}


////////////////////////////////////////////
void TTabletCountersAggregatorActor::HandleWork(TEvTabletCounters::TEvRemoveDatabase::TPtr& ev) {

    TabletMon->RemoveTabletsByPathId(ev->Get()->PathId);
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWork(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {

    TString reqTabletType = ev->Get()->Request.GetParams().Get("type");
    ui32 workers = 0;
    TryFromString(ev->Get()->Request.GetParams().Get("workers"), workers);
    for (ui32 tabletType = 0; tabletType < TTabletTypes::USER_TYPE_START; ++tabletType) {
        if (!NKikimrTabletBase::TTabletTypes::EType_IsValid(tabletType))
            continue;
        TString tabletTypeStr = TTabletTypes::TypeToStr((TTabletTypes::EType)tabletType);
        if (tabletTypeStr == reqTabletType) {
            TActorId handler = CreateClusterLabeledCountersAggregator(ctx.SelfID, (TTabletTypes::EType)tabletType, ctx, 1, "", workers);
            HttpRequestHandlers.insert(std::make_pair(handler, std::make_pair(ev->Sender, ev->Release())));
            return;
        }
    }
    //reaching this point means that this is unknow tablet type, response with nothing
    TString html;
    for (const auto& tabletType: TabletTypeOfReceivedLabeledCounters) {
        TString tabletTypeStr = TTabletTypes::TypeToStr((TTabletTypes::EType)tabletType);
        html += "<a href=\"?type=" + tabletTypeStr + "\">" + tabletTypeStr + " labeled counters</a><br>";
        html += "<a href=\"?type=" + tabletTypeStr + "&json=1\">" + tabletTypeStr + " labeled counters as json</a><br>";
        html += "<a href=\"?type=" + tabletTypeStr + "&spack=1\">" + tabletTypeStr + " labeled counters as spack</a><br>";
    }
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
}

////////////////////////////////////////////
void
TTabletCountersAggregatorActor::HandleWakeup(const TActorContext &ctx) {

    TabletMon->RecalcAll();
    ctx.Schedule(TDuration::Seconds(WAKEUP_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
}

////////////////////////////////////////////
/// public state functions
////////////////////////////////////////////
STFUNC(TTabletCountersAggregatorActor::StateWork) {
    Y_UNUSED(ctx);
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletCounters::TEvTabletAddCounters, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletCountersForgetTablet, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletCountersRequest, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletAddLabeledCounters, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletLabeledCountersRequest, HandleWork);
        HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, HandleWork); //from cluster aggregator, for http requests
        hFunc(TEvTabletCounters::TEvRemoveDatabase, HandleWork);
        HFunc(NMon::TEvHttpInfo, HandleWork);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);

        // HFunc(TEvents::TEvPoisonPill, Handle); // we do not need PoisonPill for the actor
    }
}

////////////////////////////////////////////

static ui32 AGGREGATOR_TIMEOUT_SECONDS = 60;

IActor*
CreateTabletCountersAggregator(bool follower) {
    return new TTabletCountersAggregatorActor(follower);
}

void TabletCountersForgetTablet(ui64 tabletId, TTabletTypes::EType tabletType, TPathId tenantPathId, bool follower, TActorIdentity identity) {
    const TActorId countersAggregator = MakeTabletCountersAggregatorID(identity.NodeId(), follower);
    identity.Send(countersAggregator, new TEvTabletCounters::TEvTabletCountersForgetTablet(tabletId, tabletType, tenantPathId));
}

///////////////////////////////////////////

TString ReplaceDelimiter(const TString& groups) {
    TString res;
    for (const auto& c : groups) {
        if (c == '|') {
            res += '/';
        } else {
            res += c;
        }
    }
    return res;
}

void PreProcessResponse(TEvTabletCounters::TEvTabletLabeledCountersResponse* response) {
    auto& record = response->Record;
    for (auto & lc : (*record.MutableLabeledCountersByGroup())) {
        if (lc.GetDelimiter() == "/") continue;
        if (lc.GetDelimiter() == "|") { //convert
            TString group = ReplaceDelimiter(lc.GetGroup());
            lc.SetGroup(group);
            if (lc.HasGroupNames()) {
                TString groupNames = ReplaceDelimiter(lc.GetGroupNames());
                lc.SetGroupNames(groupNames);
            }
        }
        lc.SetDelimiter("/");
    }
}


class TClusterLabeledCountersAggregatorActorV1 : public TActorBootstrapped<TClusterLabeledCountersAggregatorActorV1> {
    using TBase = TActorBootstrapped<TClusterLabeledCountersAggregatorActorV1>;
    TActorId Initiator;
    TTabletTypes::EType TabletType;
    ui32 NodesRequested;
    ui32 NodesReceived;
    TVector<ui32> Nodes;
    THashMap<ui32, TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersResponse>> PerNodeResponse;
    ui32 NumWorkers;
    ui32 WorkerId;


public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COUNTERS_AGGREGATOR;
    }

    //
    TClusterLabeledCountersAggregatorActorV1(const TActorId& parentActor, const TTabletTypes::EType tabletType, ui32 numWorkers = 0, ui32 workerId = 0)
        : Initiator(parentActor)
        , TabletType(tabletType)
        , NodesRequested(0)
        , NodesReceived(0)
        , NumWorkers(numWorkers)
        , WorkerId(workerId) //if NumWorkers is zero, then WorkerId is treat as desired number of workers
    {
    }

    void SendRequest(ui32 nodeId, const TActorContext &ctx) {
        TActorId aggregatorServiceId = MakeTabletCountersAggregatorID(nodeId);
        TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersRequest> request(new TEvTabletCounters::TEvTabletLabeledCountersRequest());
        request->Record.SetTabletType(TabletType);
        request->Record.SetVersion(1);
        ctx.Send(aggregatorServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        Nodes.emplace_back(nodeId);
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor request to node " << nodeId << " " << ctx.SelfID);
        ++NodesRequested;
    }

    void Die(const TActorContext& ctx) override {
        for (const ui32 node : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe());
        }
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (NumWorkers > 0) {
            const TActorId nameserviceId = GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            TBase::Become(&TThis::StateRequestedBrowse);
            ctx.Schedule(TDuration::Seconds(AGGREGATOR_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V1 Initiator " << Initiator << " self " << ctx.SelfID << " worker " << WorkerId);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V1 " << ctx.SelfID);
            for (ui32 i = 0; i < WorkerId; ++i) {
                ctx.Register(new TClusterLabeledCountersAggregatorActorV1(ctx.SelfID, TabletType, WorkerId, i));
            }
            NodesRequested = WorkerId;
            TBase::Become(&TThis::StateRequested);
        }
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        Y_VERIFY(!nodesInfo->Nodes.empty());
        Nodes.reserve(nodesInfo->Nodes.size());
        ui32 i = 0;
        for (const auto& ni : nodesInfo->Nodes) {
            ++i;
            if (i % NumWorkers == WorkerId) {
                SendRequest(ni.NodeId, ctx);
            }
        }
        if (NodesRequested > 0) {
            TBase::Become(&TThis::StateRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor undelivered node " << nodeId << " " << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor disconnected node " << nodeId << " " << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void HandleResponse(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got response node " << nodeId << " " << ctx.SelfID);
        PreProcessResponse(ev->Get());
        PerNodeResponse[nodeId] = ev->Release();
        NodeResponseReceived(ctx);
    }

    void NodeResponseReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived >= NodesRequested) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got TIMEOUT");
        ReplyAndDie(ctx);
    }


    void ReplyAndDie(const TActorContext& ctx) {
        TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersResponse> response(new TEvTabletCounters::TEvTabletLabeledCountersResponse);

        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator all answers recieved - replying " << ctx.SelfID);

        TVector<ui8> types;
        TVector<ui8> aggrFuncs;
        TVector<const char*> names;
        ui32 metaInfoCount = 0;
        THashMap<TString, TAutoPtr<TTabletLabeledCountersBase>> groupsToLabeledCounter;
        THashMap<TString, std::pair<ui32,ui32>> startPos;
        THashMap<TString, TString> groupToNames;
        for (auto& resp : PerNodeResponse) {
            if (!resp.second)
                continue;
            for (ui32 i = 0; i < resp.second->Record.LabeledCountersByGroupSize(); ++i) {
                const auto& labeledCounterByGroup = resp.second->Record.GetLabeledCountersByGroup(i);
                const TString& group = labeledCounterByGroup.GetGroup();
                if (startPos.find(group) != startPos.end())
                    continue;
                ui32 count = labeledCounterByGroup.LabeledCounterSize();
                for (ui32 j = 0; j < count; ++j) {
                    const auto& labeledCounter = labeledCounterByGroup.GetLabeledCounter(j);
                    names.push_back(labeledCounter.GetName().c_str());
                    aggrFuncs.push_back(labeledCounter.GetAggregateFunc());
                    types.push_back(labeledCounter.GetType());
                }
                startPos[group] = std::make_pair(metaInfoCount, count);
                groupToNames[group] = labeledCounterByGroup.GetGroupNames();
                metaInfoCount += count;
            }
        }
        for (auto& resp : PerNodeResponse) {
            if (!resp.second)
                continue;
            //TODO: labeledCounterByGroup must have as key group + groupToNames[group] - in case of aggregation (changing group to total/total/total)
            // keys may be equal for different metrics types
            for (ui32 i = 0; i < resp.second->Record.LabeledCountersByGroupSize(); ++i) {
                const auto& labeledCounterByGroup = resp.second->Record.GetLabeledCountersByGroup(i);
                const TString& originalGroup = labeledCounterByGroup.GetGroup();
                ui32 count = Min<ui32>(startPos[originalGroup].second, labeledCounterByGroup.LabeledCounterSize());
                ui32 sp = startPos[originalGroup].first;
                TAutoPtr<TTabletLabeledCountersBase> labeledCounters(new TTabletLabeledCountersBase(count, names.begin() + sp,
                                                                            types.begin() + sp, aggrFuncs.begin() + sp,
                                                                            originalGroup, nullptr, 0));
                for (ui32 j = 0; j < count; ++j) {
                    const auto& labeledCounter = labeledCounterByGroup.GetLabeledCounter(j);
                    labeledCounters->GetCounters()[j] = labeledCounter.GetValue();
                    labeledCounters->GetIds()[j] = labeledCounter.GetId();
                }

                TVector<TString> aggrGroups;
                TVector<TString> groupParts, groupParts2;
                StringSplitter(originalGroup).Split('/').SkipEmpty().Collect(&groupParts);
                Y_VERIFY(groupParts.size() > 0);
                groupParts2 = groupParts;
                ui32 changePos = groupParts.size();
                TString group = originalGroup;
                do { //repors for user/topic  user/total; total;total
                    auto& aggr = groupsToLabeledCounter[group];
                    groupToNames[group] = groupToNames[originalGroup];
                    if (aggr == nullptr) {
                        aggr = new TTabletLabeledCountersBase(*labeledCounters);
                    } else {
                        aggr->AggregateWith(*labeledCounters);
                    }
                    if (changePos > 0) {
                        --changePos;
                        groupParts[changePos] = "total";
                        group = "";
                        for (auto& g: groupParts) {
                            if (!group.empty()) group += '/';
                            group += g;
                        }
                    } else
                        break;
                } while (true);
                for (changePos = 0; changePos + 1 < groupParts.size(); ++changePos) {
                    groupParts2[changePos] = "total";
                    group = "";
                    for (auto& g: groupParts2) {
                        if (!group.empty()) group += '/';
                        group += g;
                    }
                    auto& aggr = groupsToLabeledCounter[group];
                    groupToNames[group] = groupToNames[originalGroup];
                    if (aggr == nullptr) {
                        aggr = new TTabletLabeledCountersBase(*labeledCounters);
                    } else {
                        aggr->AggregateWith(*labeledCounters);
                    }
                }

            }
            response->Record.AddNodes(resp.first);
        }
        ui32 numGroups = 0, numCounters = 0;
        for (auto& g : groupsToLabeledCounter) {
            auto& labeledCounterByGroup = *response->Record.AddLabeledCountersByGroup();
            labeledCounterByGroup.SetGroup(g.first);
            labeledCounterByGroup.SetGroupNames(groupToNames[g.first]);
            labeledCounterByGroup.SetDelimiter("/"); //TODO: change to "|"
            ++numGroups;
            for (ui32 i = 0; i < g.second->GetCounters().Size(); ++i) {
                if (g.second->GetTypes()[i] == TLabeledCounterOptions::CT_TIMELAG && g.second->GetCounters()[i].Get() == Max<i64>()) { //this means no data, do not report it
                    continue;
                }
                ++numCounters;
                auto& labeledCounter = *labeledCounterByGroup.AddLabeledCounter();
                switch(g.second->GetTypes()[i]) {
                    case TLabeledCounterOptions::CT_SIMPLE:
                    case TLabeledCounterOptions::CT_DERIV:
                        labeledCounter.SetValue(g.second->GetCounters()[i].Get());
                        break;
                    case TLabeledCounterOptions::CT_TIMELAG: {
                        ui64 milliSeconds = TAppData::TimeProvider->Now().MilliSeconds();
                        labeledCounter.SetValue(milliSeconds > g.second->GetCounters()[i].Get() ? milliSeconds - g.second->GetCounters()[i].Get() : 0);
                        break;
                        }
                    default:
                        Y_FAIL("unknown type");
                }
                labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(g.second->GetAggrFuncs()[i]));
                labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(g.second->GetTypes()[i]));
                labeledCounter.SetId(g.second->GetIds()[i].Get());
                labeledCounter.SetName(g.second->GetCounterName(i));
            }
        }
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator request processed  - got " << numGroups << " groups and " << numCounters << " counters " << ctx.SelfID << " Initiator " << Initiator);
        ui64 cookie = NumWorkers ? WorkerId : 0;
        ctx.Send(Initiator, response.Release(), 0, cookie);
        TBase::Die(ctx);
    }
};

class TClusterLabeledCountersAggregatorActorV2 : public TActorBootstrapped<TClusterLabeledCountersAggregatorActorV2> {
    using TBase = TActorBootstrapped<TClusterLabeledCountersAggregatorActorV2>;
    TActorId Initiator;
    TTabletTypes::EType TabletType;
    ui32 NodesRequested;
    ui32 NodesReceived;
    TVector<ui32> Nodes;
    THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> Response;
    TTabletLabeledCountersResponseContext ResponseContext;
    THashMap<ui32, THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>> PerNodeResponse;
    THashMap<TString, NKikimrTabletCountersAggregator::TTabletLabeledCounters*> IndexTabletLabeledCounters;
    THashMap<std::pair<NKikimrTabletCountersAggregator::TTabletLabeledCounters*, ui32>, NKikimrTabletCountersAggregator::TTabletLabeledCounter*> IndexTabletLabeledCounter;
    TString Group;
    ui32 NumWorkers;
    ui32 WorkerId;
    bool NewFormat;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_COUNTERS_AGGREGATOR;
    }

    //
    TClusterLabeledCountersAggregatorActorV2(const TActorId& parentActor, const TTabletTypes::EType tabletType, const TString& group, ui32 numWorkers = 0, ui32 workerId = 0, bool newFormat = false)
        : Initiator(parentActor)
        , TabletType(tabletType)
        , NodesRequested(0)
        , NodesReceived(0)
        , Response(new TEvTabletCounters::TEvTabletLabeledCountersResponse())
        , ResponseContext(Response->Record)
        , Group(group)
        , NumWorkers(numWorkers)
        , WorkerId(workerId)
        , NewFormat(newFormat)
    {}

    void SendRequest(ui32 nodeId, const TActorContext &ctx) {
        TActorId aggregatorServiceId = MakeTabletCountersAggregatorID(nodeId);
        TAutoPtr<TEvTabletCounters::TEvTabletLabeledCountersRequest> request(new TEvTabletCounters::TEvTabletLabeledCountersRequest());
        request->Record.SetVersion(2);
        request->Record.SetTabletType(TabletType);
        if (!Group.empty()) {
            request->Record.SetGroup(Group);

        }
        ctx.Send(aggregatorServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        Nodes.emplace_back(nodeId);
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor request to node " << nodeId << " " << ctx.SelfID);
        ++NodesRequested;
    }

    NKikimrTabletCountersAggregator::TTabletLabeledCounters* GetCounters(const TString& group) {
        auto it = IndexTabletLabeledCounters.find(group);
        if (it != IndexTabletLabeledCounters.end()) {
            return it->second;
        }
        NKikimrTabletCountersAggregator::TTabletLabeledCounters* counters = Response->Record.AddLabeledCountersByGroup();
        counters->SetGroup(group);
        counters->SetDelimiter("/"); //TODO:change to "|"
        IndexTabletLabeledCounters.emplace(group, counters);
        return counters;
    }

    NKikimrTabletCountersAggregator::TTabletLabeledCounter* GetCounter(NKikimrTabletCountersAggregator::TTabletLabeledCounters* counters, ui32 nameId) {
        auto key = std::make_pair(counters, nameId);
        auto it = IndexTabletLabeledCounter.find(key);
        if (it != IndexTabletLabeledCounter.end()) {
            return it->second;
        }
        NKikimrTabletCountersAggregator::TTabletLabeledCounter* counter = counters->AddLabeledCounter();
        counter->SetNameId(nameId);
        IndexTabletLabeledCounter.emplace(key, counter);
        return counter;
    }

    void Merge(NKikimrTabletCountersAggregator::TTabletLabeledCounter& target, const NKikimrTabletCountersAggregator::TTabletLabeledCounter& source) {
        ui64 value(source.GetValue());
        TLabeledCounterOptions::ECounterType type(source.GetType());
        NKikimr::TLabeledCounterOptions::EAggregateFunc func(source.GetAggregateFunc());
        if (type == TLabeledCounterOptions::CT_TIMELAG) {
            type = TLabeledCounterOptions::CT_SIMPLE;
            auto now = TInstant::Now().MilliSeconds();
            value =  now > value ? now - value : 0;
            switch (func) {
            case NKikimr::TLabeledCounterOptions::EAF_MIN:
                func = NKikimr::TLabeledCounterOptions::EAF_MAX;
                break;
            case NKikimr::TLabeledCounterOptions::EAF_MAX:
                func = NKikimr::TLabeledCounterOptions::EAF_MIN;
                break;
            default:
                break;
            }
        }
        if (target.HasValue()) {
            switch (func) {
            case NKikimr::TLabeledCounterOptions::EAF_MIN:
                target.SetValue(std::min(target.GetValue(), value));
                break;
            case NKikimr::TLabeledCounterOptions::EAF_MAX:
                target.SetValue(std::max(target.GetValue(), value));
                break;
            case NKikimr::TLabeledCounterOptions::EAF_SUM:
                target.SetValue(target.GetValue() + value);
                break;
            }
        } else {
            target.SetValue(value);
            target.SetType(type);
            target.SetAggregateFunc(func);
        }
    }

    void Merge(const NKikimrTabletCountersAggregator::TEvTabletLabeledCountersResponse& source) {
        TVector<ui32> namesMapper;
        namesMapper.reserve(source.CounterNamesSize());
        for (const TString& name : source.GetCounterNames()) {
            namesMapper.push_back(ResponseContext.GetNameId(name));
        }
        for (const NKikimrTabletCountersAggregator::TTabletLabeledCounters& srcCounters : source.GetLabeledCountersByGroup()) {
            NKikimrTabletCountersAggregator::TTabletLabeledCounters* trgCounters = GetCounters(srcCounters.GetGroup());
            for (const NKikimrTabletCountersAggregator::TTabletLabeledCounter& srcCounter : srcCounters.GetLabeledCounter()) {
                ui32 nameId = 0;
                if (srcCounter.HasName()) {
                    nameId = ResponseContext.GetNameId(srcCounter.GetName());
                } else {
                    nameId = namesMapper[srcCounter.GetNameId()];
                }
                NKikimrTabletCountersAggregator::TTabletLabeledCounter* trgCounter = GetCounter(trgCounters, nameId);
                Merge(*trgCounter, srcCounter);
            }
        }
    }

    void Die(const TActorContext& ctx) override {
        for (const ui32 node : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(node), new TEvents::TEvUnsubscribe());
        }
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (NumWorkers > 0) {
            const TActorId nameserviceId = GetNameserviceActorId();
            ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            TBase::Become(&TThis::StateRequestedBrowse);
            ctx.Schedule(TDuration::Seconds(AGGREGATOR_TIMEOUT_SECONDS), new TEvents::TEvWakeup());
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V2 Initiator " << Initiator << " self " << ctx.SelfID << " worker " << WorkerId);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator new request V2 " << ctx.SelfID);
            for (ui32 i = 0; i < WorkerId; ++i) {
                ctx.Register(new TClusterLabeledCountersAggregatorActorV2(ctx.SelfID, TabletType, Group, WorkerId, i));
            }
            NodesRequested = WorkerId;
            TBase::Become(&TThis::StateRequested);
        }
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        Y_VERIFY(!nodesInfo->Nodes.empty());
        Nodes.reserve(nodesInfo->Nodes.size());
        ui32 i = 0;
        for (const auto& ni : nodesInfo->Nodes) {
            ++i;
            if (i % NumWorkers == WorkerId) {
                SendRequest(ni.NodeId, ctx);
            }
        }
        if (NodesRequested > 0) {
            TBase::Become(&TThis::StateRequested);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor undelivered node " << nodeId << " "  << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor disconnected node " << nodeId << " " << ctx.SelfID);
        if (PerNodeResponse.emplace(nodeId, nullptr).second) {
            NodeResponseReceived(ctx);
        }
    }

    void HandleResponse(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got response node " << nodeId << " " << ctx.SelfID);
        PreProcessResponse(ev->Get());
        auto it = PerNodeResponse.emplace(nodeId, ev->Release().Release());
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor merged response node " << nodeId << " " << ctx.SelfID);
        if (it.second) {
            Merge(it.first->second->Record);
            NodeResponseReceived(ctx);
        }
    }

    void NodeResponseReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived >= NodesRequested) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator actor got TIMEOUT");
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TABLET_AGGREGATOR, "aggregator request processed " << ctx.SelfID << " Initiator " << Initiator);
        ui64 cookie = NumWorkers ? WorkerId : 0;
        if (NewFormat) {
            for (auto& counters : *Response->Record.MutableLabeledCountersByGroup()) {
                TVector<TString> groups;
                StringSplitter(counters.GetGroup()).SplitByString(counters.GetDelimiter()).SkipEmpty().Collect(&groups);
                TStringBuf ff;
                TString topic = "";
                TString dc = "";
                TString res;
                if (groups.size() == 1) { //topic case
                    ff = groups[0];
                } else if (groups.size() == 3) { //client important topic
                    res = NPersQueue::ConvertOldConsumerName(groups[0], ctx) + "|" + groups[1] + "|";
                    ff = groups[2];
                } else {
                    continue;
                }
                if (ff.empty())
                    continue;
                TStringBuf tmp(ff.NextTok('.'));
                if (tmp != "rt3")
                    continue;
                dc = TString(ff.NextTok("--"));
                if (dc.empty())
                    continue;
                if (ff.empty())
                    continue;
                topic = NPersQueue::ConvertOldTopicName(TString(ff));
                res += topic + "|" + dc;
                counters.SetGroup(res);
                counters.SetDelimiter("|");
            }
        }
        ctx.Send(Initiator, Response.Release(), 0, cookie);
        TBase::Die(ctx);
    }
};

IActor* CreateClusterLabeledCountersAggregatorActor(const TActorId& parentActor, TTabletTypes::EType tabletType, ui32 version, const TString& group, const ui32 totalWorkersCount) {
    switch (version) {
    case 1:
        return new TClusterLabeledCountersAggregatorActorV1(parentActor, tabletType, totalWorkersCount == 0 ? 1 : 0, totalWorkersCount);
    case 2:
        return new TClusterLabeledCountersAggregatorActorV2(parentActor, tabletType, group, totalWorkersCount == 0 ? 1 : 0, totalWorkersCount);
    case 3: //new format
        return new TClusterLabeledCountersAggregatorActorV2(parentActor, tabletType, group, totalWorkersCount == 0 ? 1 : 0, totalWorkersCount, true);
    }
    return nullptr;
}


TActorId CreateClusterLabeledCountersAggregator(const TActorId& parentActor, TTabletTypes::EType tabletType, const TActorContext& ctx, ui32 version, const TString& group, const ui32 totalWorkersCount) {
    return ctx.Register(CreateClusterLabeledCountersAggregatorActor(parentActor, tabletType, version, group, totalWorkersCount), TMailboxType::HTSwap, AppData(ctx)->BatchPoolId);
}




}
