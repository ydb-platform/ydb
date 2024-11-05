#pragma once

#include "error.h"
#include "error_helpers.h"

#include <yt/yt/library/profiling/sensor.h>

#ifdef _unix_
    #include <string.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProcessExitProfiler
{
public:
    TProcessExitProfiler(
        const NProfiling::TProfiler& parent,
        const TString& prefix);

    void OnProcessExit(
        const TError& error,
        std::optional<TDuration> delay = {});

private:
    const NProfiling::TProfiler Profiler_;

    NProfiling::TEventTimer ExitDelayTimer_;
    NProfiling::TCounter ExitOKCounter_;
    NProfiling::TCounter ExitUnknownCounter_;
    THashMap<int, NProfiling::TCounter> NonZeroExitCounters_;
    THashMap<int, NProfiling::TCounter> SignalExitCounters_;

    NProfiling::TCounter& GetOrCreateNonZeroExitCounter(int exitCode);

    NProfiling::TCounter MakeExitCodeCounter(int exitCode);

    NProfiling::TCounter& GetOrCreateSignalExitCounter(int signal);

    NProfiling::TCounter MakeSignalExitCounter(int signal);

    static TString GetSignalName(int signal);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
