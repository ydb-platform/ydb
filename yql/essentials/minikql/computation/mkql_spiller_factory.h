#pragma once

#include "mkql_spiller.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql::NDq {
struct TSpillingTaskCounters;
} // namespace NYql::NDq

namespace NKikimr::NMiniKQL {

class ISpillerFactory: private TNonCopyable {
public:
    virtual ISpiller::TPtr CreateSpiller() = 0;

    virtual void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& spillingTaskCounters) = 0;

    virtual void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback reportAlloc, ISpiller::TMemoryReportCallback reportFree) = 0;

    // Returns the monitoring counters root for sort/spilling metrics.
    // May return nullptr if monitoring is not configured.
    virtual ::NMonitoring::TDynamicCounterPtr GetCounters() const { return nullptr; }

    virtual ~ISpillerFactory() {
    }
};

} // namespace NKikimr::NMiniKQL
