#pragma once
#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>

namespace NKikimr {

class TSharedQuota : public TThrRefBase {
public:
    TSharedQuota(::NMonitoring::TDynamicCounters::TCounterPtr usedCounter,
                 ::NMonitoring::TDynamicCounters::TCounterPtr sizeCounter,
                 ui64 quota = 0)
        : Quota(quota)
        , FreeQuota(Quota)
        , UsedCounter(usedCounter)
        , SizeCounter(sizeCounter)
    {
        Y_ABORT_UNLESS(UsedCounter);
        Y_ABORT_UNLESS(SizeCounter);
    }

    bool TryCaptureQuota(ui64 quota)
    {
        i64 updatedQuota;
        i64 freeQuota;
        do {
            freeQuota = AtomicLoad(&FreeQuota);
            if (freeQuota < static_cast<i64>(quota))
                return false;
            updatedQuota = freeQuota - quota;
        } while (!AtomicCas(&FreeQuota, updatedQuota, freeQuota));
        *UsedCounter += quota;
        return true;
    }

    void ReleaseQuota(ui64 quota)
    {
        AtomicAdd(FreeQuota, quota);
        *UsedCounter -= quota;
    }

    ui64 GetFreeQuota() const
    {
        return AtomicLoad(&FreeQuota);
    }

    ui64 GetQuota() const
    {
        return AtomicLoad(&Quota);
    }

    ui64 ChangeQuota(i64 delta)
    {
        ui64 updatedQuota;
        ui64 quota;
        do {
            quota = AtomicLoad(&Quota);
            Y_ABORT_UNLESS(delta > 0 || quota >= static_cast<ui64>(-delta));
            updatedQuota = quota + delta;
        } while (!AtomicCas(&Quota, updatedQuota, quota));
        AtomicAdd(FreeQuota, delta);
        *SizeCounter += delta;
        return updatedQuota;
    }

private:
    volatile ui64 Quota;
    volatile i64 FreeQuota;
    ::NMonitoring::TDynamicCounters::TCounterPtr UsedCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr SizeCounter;
};

using TSharedQuotaPtr = TIntrusivePtr<TSharedQuota>;

} // namespace NKikimr
