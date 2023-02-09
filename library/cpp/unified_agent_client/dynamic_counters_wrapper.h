#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NUnifiedAgent {
    class TDynamicCountersWrapper: public TAtomicRefCount<TDynamicCountersWrapper> {
    public:
        explicit TDynamicCountersWrapper(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
            : Counters(counters)
        {
        }

        virtual ~TDynamicCountersWrapper() = default;

        const TIntrusivePtr<NMonitoring::TDynamicCounters>& Unwrap() const {
            return Counters;
        }

    protected:
        NMonitoring::TDeprecatedCounter& GetCounter(const TString& value, bool derivative) {
            return *Counters->GetCounter(value, derivative);
        }

    private:
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    };

    class TUpdatableCounters: public TDynamicCountersWrapper {
    public:
        using TDynamicCountersWrapper::TDynamicCountersWrapper;

        virtual void Update() = 0;
    };
}
