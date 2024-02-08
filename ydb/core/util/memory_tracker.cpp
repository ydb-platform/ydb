#include "memory_tracker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/memory_tracker.h>
#include <library/cpp/html/escape/escape.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/xrange.h>

using namespace NMonitoring;
using namespace NActors::NMemory::NPrivate;

namespace NKikimr {
namespace NMemory {

class TMemoryTrackerActor : public TActorBootstrapped<TMemoryTrackerActor> {
public:
    TMemoryTrackerActor(TDuration updateInterval, TDynamicCounterPtr appCounters)
        : UpdateInterval(updateInterval)
        , AppCounters(appCounters)
    {
        auto utilsCounters = GetServiceCounters(AppCounters, "utils");
        Counters = utilsCounters->GetSubgroup("component", "memory_tracker");
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MEMORY_TRACKER;
    }

    void Bootstrap(const TActorContext&) {
#if defined(ENABLE_MEMORY_TRACKING)
        TMemoryTracker::Instance()->Initialize();
        RegisterCounters(Counters);

        TMon* mon = AppData()->Mon;
        if (mon) {
            auto *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "memory_tracker", "Memory tracker",
                false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        Collect();
#endif
        Become(&TMemoryTrackerActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvWakeup::EventType, Wakeup)
        hFunc(NMon::TEvHttpInfo, HttpInfo)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

private:
    void Wakeup() {
        Collect();
    }

    void Collect() {
        Schedule(UpdateInterval, new TEvents::TEvWakeup());

        std::vector<TMetric> metrics;
        TMemoryTracker::Instance()->GatherMetrics(metrics);

        auto count = metrics.size();
        if (Static.size() != count) {
            return;
        }

        const auto& sensors = TMemoryTracker::Instance()->GetSensors();

        for (auto i : xrange(count)) {
            auto& metric = Static[i];
            metric.Current = metrics[i];
            metric.Peak.CalculatePeak(metric.Current);

            if (sensors.find(i) != sensors.end()) {
                *metric.CurrentMemCounter = metric.Current.GetMemory();
                *metric.PeakMemCounter = metric.Peak.GetMemory();
                *metric.CurrentCountCounter = metric.Current.GetCount();
                *metric.PeakCountCounter = metric.Peak.GetCount();
            }
        }

        GatherDynamicMetrics();
    }

    void RegisterCounters(::NMonitoring::TDynamicCounterPtr counters) {
        auto componentGroup = counters->GetSubgroup("component", "memory_tracker");

        auto count = TMemoryTracker::Instance()->GetCount();
        Static.resize(count);

        const auto& sensors = TMemoryTracker::Instance()->GetSensors();
        for (auto index : sensors) {
            auto name = TMemoryTracker::Instance()->GetName(index);
            auto& metric = Static[index];
            auto group = componentGroup->GetSubgroup("label", name);
            metric.CurrentMemCounter = group->GetCounter("MT/Memory");
            metric.PeakMemCounter = group->GetCounter("MT/PeakMemory");
            metric.CurrentCountCounter = group->GetCounter("MT/Count");
            metric.PeakCountCounter = group->GetCounter("MT/PeakCount");
        }
    }

    void UpdateDynamicCounters() {
        auto utils = GetServiceCounters(AppCounters, "utils");

        if (Dynamic.find(AnonRssSize) == Dynamic.end()) {
            Dynamic.emplace(AnonRssSize, utils->GetCounter("Process/AnonRssSize"));
        }

        if (Dynamic.find(LFMmapped) == Dynamic.end()) {
            auto lfAllocGroup = utils->FindSubgroup("component", "lfalloc");
            if (lfAllocGroup) {
                Dynamic.emplace(LFMmapped, lfAllocGroup->GetCounter("ActiveBytesMmapped"));
                Dynamic.emplace(LFSmall, lfAllocGroup->GetCounter("ActiveBytesSmall"));
                Dynamic.emplace(LFLarge, lfAllocGroup->GetCounter("ActiveBytesLarge"));
                Dynamic.emplace(LFSystem, lfAllocGroup->GetCounter("ActiveBytesSystem"));
            }
        }

        if (Dynamic.find(MkqlGlobalPoolTotalBytes) == Dynamic.end()) {
            auto mkqlAllocGroup = utils->FindSubgroup("subsystem", "mkqlalloc");
            if (mkqlAllocGroup) {
                Dynamic.emplace(MkqlGlobalPoolTotalBytes, mkqlAllocGroup->GetCounter("GlobalPoolTotalBytes"));

                auto getNamedPagePoolCounter = [&mkqlAllocGroup](const char* name) {
                    return mkqlAllocGroup->GetCounter(TString(name) + "/TotalBytesAllocated");
                };
                Dynamic.emplace(MkqlKqp, getNamedPagePoolCounter("kqp"));
                Dynamic.emplace(MkqlCompile, getNamedPagePoolCounter("compile"));
                Dynamic.emplace(MkqlDatashard, getNamedPagePoolCounter("datashard"));
                Dynamic.emplace(MkqlLocalTx, getNamedPagePoolCounter("local_tx"));
                Dynamic.emplace(MkqlTxProxy, getNamedPagePoolCounter("tx_proxy"));
            }
        }
    }

    void GatherDynamicMetrics() {
        UpdateDynamicCounters();

        for (auto& [_, metric] : Dynamic) {
            if (metric.Counter) {
                metric.Current.SetMemory(metric.Counter->Val());
                metric.Peak.CalculatePeak(metric.Current);
            }
        }
    }

    static TString PrettyNumber(ssize_t val) {
        if (val == 0) {
            return "0";
        }
        TStringStream str;
        if (val < 0) {
            str << "- ";
        }
        ui64 value = val < 0 ? -val : val;
        TSmallVec<ui64> parts;
        while (value > 0) {
            auto head = value / 1000;
            parts.push_back(value - head * 1000);
            value = head;
        }
        str << parts.back();
        parts.pop_back();
        while (!parts.empty()) {
            str << Sprintf(" %03" PRIu64, parts.back());
            parts.pop_back();
        }
        return str.Str();
    }

    void HttpInfo(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;

        HTML(str) {
#if defined(ENABLE_MEMORY_TRACKING)
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Metric"; }
                        TABLEH() { str << "Memory"; }
                        TABLEH() { str << "Peak memory"; }
                    }
                }
                TABLEBODY() {
                    auto outputDynamic = [&str, &__stream] (const char* name, const TDynamicMetric& metric) {
                        TABLED() { str << name; }
                        TAG_CLASS_STYLE(TTableD, "", "text-align:right") {
                            str << PrettyNumber(metric.Current.GetMemory()); }
                        TAG_CLASS_STYLE(TTableD, "", "text-align:right") {
                            str << PrettyNumber(metric.Peak.GetMemory()); }
                    };

                    if (Dynamic.find(AnonRssSize) != Dynamic.end()) {
                        TABLER() { outputDynamic("AnonRssSize", Dynamic[AnonRssSize]); }
                    }
                    if (Dynamic.find(LFMmapped) != Dynamic.end()) {
                        TABLER() {
                            TABLED() { str << "LFAlloc"; }
                        }
                        TABLER() { outputDynamic("Mmapped", Dynamic[LFMmapped]); }
                        TABLER() { outputDynamic("Small", Dynamic[LFSmall]); }
                        TABLER() { outputDynamic("Large", Dynamic[LFLarge]); }
                        TABLER() { outputDynamic("System", Dynamic[LFSystem]); }
                    }
                    if (Dynamic.find(MkqlGlobalPoolTotalBytes) != Dynamic.end()) {
                        TABLER() {
                            TABLED() { str << "MKQLAlloc"; }
                        }
                        TABLER() { outputDynamic("GlobalPoolTotalBytes", Dynamic[MkqlGlobalPoolTotalBytes]); }
                        TABLER() { outputDynamic("Kqp", Dynamic[MkqlKqp]); }
                        TABLER() { outputDynamic("Compile", Dynamic[MkqlCompile]); }
                        TABLER() { outputDynamic("Datashard", Dynamic[MkqlDatashard]); }
                        TABLER() { outputDynamic("LocalTx", Dynamic[MkqlLocalTx]); }
                        TABLER() { outputDynamic("TxProxy", Dynamic[MkqlTxProxy]); }
                    }
                }
            }
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Metric"; }
                        TABLEH() { str << "Count"; }
                        TABLEH() { str << "Peak count"; }
                        TABLEH() { str << "Memory"; }
                        TABLEH() { str << "Peak memory"; }
                    }
                }
                TABLEBODY() {
                    const auto& indices = TMemoryTracker::Instance()->GetMetricIndices();
                    for (const auto& [name, index] : indices) {
                        TABLER() {
                            auto& metric = Static[index];
                            TABLED() { str << NHtml::EscapeText(name); }
                            TAG_CLASS_STYLE(TTableD, "", "text-align:right") {
                                str << PrettyNumber(metric.Current.GetCount()); }
                            TAG_CLASS_STYLE(TTableD, "", "text-align:right") {
                                str << PrettyNumber(metric.Peak.GetCount()); }
                            TAG_CLASS_STYLE(TTableD, "", "text-align:right") {
                                str << PrettyNumber(metric.Current.GetMemory()); }
                            TAG_CLASS_STYLE(TTableD, "", "text-align:right") {
                                str << PrettyNumber(metric.Peak.GetMemory()); }
                        }
                    }
                }
            }
#endif
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    const TDuration UpdateInterval;

    enum EDynamicMetric {
        AnonRssSize,
        LFMmapped,
        LFSmall,
        LFLarge,
        LFSystem,
        MkqlGlobalPoolTotalBytes,
        MkqlKqp,
        MkqlCompile,
        MkqlDatashard,
        MkqlLocalTx,
        MkqlTxProxy
    };

    TDynamicCounterPtr AppCounters;
    TDynamicCounterPtr Counters;

    struct TStaticMetric {
        TMetric Current;
        TMetric Peak;
        TDynamicCounters::TCounterPtr CurrentMemCounter;
        TDynamicCounters::TCounterPtr PeakMemCounter;
        TDynamicCounters::TCounterPtr CurrentCountCounter;
        TDynamicCounters::TCounterPtr PeakCountCounter;
    };

    struct TDynamicMetric {
        TMetric Current;
        TMetric Peak;
        TDynamicCounters::TCounterPtr Counter;

        TDynamicMetric(TDynamicCounters::TCounterPtr counter = {})
            : Counter(counter)
        {}
    };

    std::vector<TStaticMetric> Static;
    std::unordered_map<EDynamicMetric, TDynamicMetric> Dynamic;
};

NActors::IActor* CreateMemoryTrackerActor(TDuration updateInterval, TDynamicCounterPtr counters) {
    return new TMemoryTrackerActor(updateInterval, counters);
}

}
}
