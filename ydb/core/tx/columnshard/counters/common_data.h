#pragma once
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard {

class TLoadTimeSignals: public TCommonCountersOwner {
public:
    class TLoadTimer: public TNonCopyable {
    private:
        const TLoadTimeSignals& Signals;
        TDuration* const Total;
        const TMonotonic Start = TMonotonic::Now();
        bool Failed = false;

    public:
        TLoadTimer(const TLoadTimeSignals& signals, TDuration* total)
            : Signals(signals)
            , Total(total)
        {
        }

        void AddLoadingFail();

        ~TLoadTimer();
    };

    class TSignalsRegistry {
    private:
        TRWMutex Mutex;
        THashMap<TString, TLoadTimeSignals> Signals;

        TLoadTimeSignals GetSignalImpl(const TString& name) {
            TReadGuard rg(Mutex);
            auto it = Signals.find(name);
            if (it == Signals.end()) {
                rg.Release();
                TWriteGuard wg(Mutex);
                it = Signals.emplace(name, TLoadTimeSignals(name)).first;
                return it->second;
            } else {
                return it->second;
            }
        }

    public:
        static TLoadTimeSignals GetSignal(const TString& name) {
            return Singleton<TSignalsRegistry>()->GetSignalImpl(name);
        }
    };

private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr LoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr FailedLoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr LoadingFailCounter;
    TString Type;

    TLoadTimeSignals(const TString& type)
        : TBase("Startup")
        , Type(type)
    {
        LoadingTimeCounter = TBase::GetValue("Startup/" + type + "/LoadingTime");
        FailedLoadingTimeCounter = TBase::GetValue("Startup/" + type + "/FailedLoadingTime");
        LoadingFailCounter = TBase::GetValue("Startup/" + type + "/LoadingFailCount");
    }

public:
    TLoadTimer StartGuard(TDuration* total = nullptr) const {
        return TLoadTimer(*this, total);
    }

private:
    void AddLoadingTime(ui64 microSeconds) const {
        LoadingTimeCounter->Add(microSeconds);
    }

    void AddFailedLoadingTime(ui64 microSeconds) const {
        FailedLoadingTimeCounter->Add(microSeconds);
    }

    void AddLoadingFail() const {
        LoadingFailCounter->Add(1);
    }
};

struct TTablesLoadDurations {
    TDuration Tables;
    TDuration SchemaPresets;
    TDuration TableVersions;
    TDuration SchemaPresetVersions;
};

class TTableLoadTimeCounters {
public:
    NColumnShard::TLoadTimeSignals TableLoadTimeCounters;
    NColumnShard::TLoadTimeSignals SchemaPresetLoadTimeCounters;
    NColumnShard::TLoadTimeSignals TableVersionsLoadTimeCounters;
    NColumnShard::TLoadTimeSignals SchemaPresetVersionsLoadTimeCounters;

public:
    TTableLoadTimeCounters()
        : TableLoadTimeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("Tables"))
        , SchemaPresetLoadTimeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("SchemaPreset"))
        , TableVersionsLoadTimeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("TableVersions"))
        , SchemaPresetVersionsLoadTimeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("SchemaPresetVersions"))
    {
    }
};

}   // namespace NKikimr::NColumnShard
