#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NUnifiedAgent {
    class TDynamicCountersWrapper: public TAtomicRefCount<TDynamicCountersWrapper> {
    public:
        explicit TDynamicCountersWrapper(TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
            : Counters_(std::move(counters))
        {
        }

        NMonitoring::TDynamicCounters::TCounterPtr MakeCounterWithLabels(
                const std::vector<std::pair<TString, TString>>& labels,
                const char* name,
                bool derivative
                ) {
            auto ptr = Unwrap();
            for (const auto& p : labels) {
                ptr = ptr->GetSubgroup(p.first, p.second);
            }
            return ptr->GetCounter(name, derivative);
        }

        NMonitoring::THistogramPtr MakeHistogramWithLabels(
                const std::vector<std::pair<TString, TString>>& labels,
                const char* name,
                NMonitoring::IHistogramCollectorPtr hist
                ) {
            auto ptr = Unwrap();
            for (const auto& p : labels) {
                ptr = ptr->GetSubgroup(p.first, p.second);
            }
            return ptr->GetHistogram(name, std::move(hist));
        }

        virtual ~TDynamicCountersWrapper() = default;

        const TIntrusivePtr<NMonitoring::TDynamicCounters>& Unwrap() const {
            return Counters_;
        }

    protected:
        NMonitoring::TDeprecatedCounter& GetCounter(const TString& value, bool derivative) {
            return *Counters_->GetCounter(value, derivative);
        }

    private:
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    };

    class TUpdatableCounters: public TDynamicCountersWrapper {
    public:
        using TDynamicCountersWrapper::TDynamicCountersWrapper;

        virtual void Update() = 0;
    };
}
