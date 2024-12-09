#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TResourceTracker
{
public:
    //! Enables collecting background statistics and pushing them to profiler.
    static void Enable();

    static double GetUserCpu();
    static double GetSystemCpu();
    static double GetCpuWait();

    static i64 GetTotalMemoryLimit();
    static i64 GetAnonymousMemoryLimit();

    //! If this factor is set, additional metrics will be reported:
    //! user, system, total cpu multiplied by given factor.
    //! E.g. |system_vcpu = system_cpu * vcpu_factor|.
    static void SetVCpuFactor(double factor);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
