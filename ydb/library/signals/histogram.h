#pragma once
#include "owner.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <algorithm>
#include <map>
#include <mutex>
#include <set>
#include <vector>

namespace NKikimr::NColumnShard {

// Buckets are registered on the first Add, so an unused histogram is not exported.
class TIncrementalHistogram {
private:
    class TBuckets: public TCommonCountersOwner {
    private:
        using TBase = TCommonCountersOwner;
        std::vector<i64> Borders;
        // One name per border plus the last one for (border,+Inf)
        std::vector<TString> Names;
        std::once_flag Registered;
        std::vector<NMonitoring::TDynamicCounters::TCounterPtr> Counters;

    public:
        TBuckets(const TString& moduleId, const TString& metricId, const TString& category,
            const std::vector<std::pair<i64, TString>>& borders, TIntrusivePtr<NMonitoring::TDynamicCounters> baseSignals);

        ui32 GetSize() const {
            return Names.size();
        }

        ui32 GetIndex(const i64 value) const {
            return std::lower_bound(Borders.begin(), Borders.end(), value) - Borders.begin();
        }

        const NMonitoring::TDynamicCounters::TCounterPtr& GetCounter(const ui32 index) {
            std::call_once(Registered, [this] {
                for (auto&& name : Names) {
                    Counters.emplace_back(TBase::GetValue(name));
                }
            });
            return Counters[index];
        }
    };

    std::shared_ptr<TBuckets> Buckets;

public:
    class TGuard {
    private:
        std::shared_ptr<TBuckets> Buckets;
        std::vector<i64> Values;

    public:
        explicit TGuard(std::shared_ptr<TBuckets> buckets)
            : Buckets(std::move(buckets)) {
        }

        ~TGuard() {
            for (ui32 i = 0; i < Values.size(); ++i) {
                if (Values[i]) {
                    Buckets->GetCounter(i)->Sub(Values[i]);
                }
            }
        }

        void Add(const i64 value, const i64 count) {
            if (!count) {
                return;
            }
            if (Values.empty()) {
                Values.resize(Buckets->GetSize());
            }
            const ui32 index = Buckets->GetIndex(value);
            Buckets->GetCounter(index)->Add(count);
            Values[index] += count;
        }

        void Sub(const i64 value, const i64 count) {
            if (!count) {
                return;
            }
            const ui32 index = Buckets->GetIndex(value);
            Y_ABORT_UNLESS(index < Values.size() && Values[index] >= count);
            Buckets->GetCounter(index)->Sub(count);
            Values[index] -= count;
        }
    };

    std::shared_ptr<TGuard> BuildGuard() const {
        return std::make_shared<TGuard>(Buckets);
    }

    TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::set<i64>& values,
        TIntrusivePtr<NMonitoring::TDynamicCounters> baseSignals = nullptr);

    TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::map<i64, TString>& values);
};

class TDeriviativeHistogram: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::map<i64, NMonitoring::TDynamicCounters::TCounterPtr> Counters;
    NMonitoring::TDynamicCounters::TCounterPtr PlusInf;

    NMonitoring::TDynamicCounters::TCounterPtr GetQuantile(const i64 value) const {
        auto it = Counters.lower_bound(value);
        if (it == Counters.end()) {
            return PlusInf;
        } else {
            return it->second;
        }
    }
public:

    void Collect(const i64 volume, const ui32 count = 1) const {
        GetQuantile(volume)->Add(count);

    }

    TDeriviativeHistogram(const TString& moduleId, const TString& signalName, const TString& category, const std::set<i64>& values);

    TDeriviativeHistogram(const TString& moduleId, const TString& signalName, const TString& category, const std::map<i64, TString>& values);

};


}