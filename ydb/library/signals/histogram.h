#pragma once
#include "owner.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <set>
#include <map>

namespace NKikimr::NColumnShard {

class TIncrementalHistogram: public TCommonCountersOwner {
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

    class TGuard {
    private:
        class TLineGuard {
        private:
            NMonitoring::TDynamicCounters::TCounterPtr Counter;
            i64 Value = 0;
        public:
            TLineGuard(NMonitoring::TDynamicCounters::TCounterPtr counter)
                : Counter(counter) {

            }

            ~TLineGuard() {
                Sub(Value);
            }

            void Add(const i64 value) {
                Counter->Add(value);
                Value += value;
            }

            void Sub(const i64 value) {
                Counter->Sub(value);
                Value -= value;
                Y_ABORT_UNLESS(Value >= 0);
            }
        };

        std::map<i64, TLineGuard> Counters;
        TLineGuard PlusInf;

        TLineGuard& GetLineGuard(const i64 value) {
            auto it = Counters.lower_bound(value);
            if (it == Counters.end()) {
                return PlusInf;
            } else {
                return it->second;
            }
        }
    public:
        TGuard(const TIncrementalHistogram& owner)
            : PlusInf(owner.PlusInf) {
            for (auto&& i : owner.Counters) {
                Counters.emplace(i.first, TLineGuard(i.second));
            }
        }
        void Add(const i64 value, const i64 count) {
            GetLineGuard(value).Add(count);
        }

        void Sub(const i64 value, const i64 count) {
            GetLineGuard(value).Sub(count);
        }
    };

    std::shared_ptr<TGuard> BuildGuard() const {
        return std::make_shared<TGuard>(*this);
    }

    TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::set<i64>& values);

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

struct THistorgamBorders {
    static inline const std::map<i64, TString> BlobSizeBorders = {{0, "0"}, {512 * 1024, "512kb"}, {1024 * 1024, "1Mb"},
        {2 * 1024 * 1024, "2Mb"}, {4 * 1024 * 1024, "4Mb"},
        {5 * 1024 * 1024, "5Mb"}, {6 * 1024 * 1024, "6Mb"},
        {7 * 1024 * 1024, "7Mb"}, {8 * 1024 * 1024, "8Mb"}};

    static inline const std::map<i64, TString> PortionSizeBorders = [] {
        std::map<i64, TString> map;
        map[0] = "0";
        ui64 base = 1024;
        for (auto i = 0; i < 20; i++, base *= 2) {
            if (base >= 1024 * 1024) {
                map[base] = ToString(base / 1024 * 1024) + "Mb";
            }
            else {
                map[base] = ToString(base /  1024) + "Kb";
            }
        }
        return map;
    }();

    static inline const std::set<i64> PortionRecordBorders = {0, 2500, 5000, 7500, 9000, 10000, 20000, 40000, 80000, 160000, 320000, 640000, 1024000};
};


}