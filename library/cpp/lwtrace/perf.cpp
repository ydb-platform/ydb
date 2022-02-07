#include "perf.h"

#include "probes.h"

#include <util/system/datetime.h>
#include <util/system/hp_timer.h>

namespace NLWTrace {
    LWTRACE_USING(LWTRACE_INTERNAL_PROVIDER);

    TCpuTracker::TCpuTracker()
        : MinReportPeriod(NHPTimer::GetCyclesPerSecond())
    {
        ResetStats();
    }

    void TCpuTracker::Enter() {
        LastTs = GetCycleCount();
    }

    void TCpuTracker::Exit(const TProbe* probe) {
        ui64 exitTs = GetCycleCount();
        if (exitTs < LastTs) {
            return; // probably TSC was reset
        }
        ui64 cycles = exitTs - LastTs;
        LastTs = exitTs;

        AddStats(probe, cycles);
    }

    void TCpuTracker::AddStats(const TProbe* probe, ui64 cycles) {
        if (MaxCycles < cycles) {
            MaxProbe = probe;
            MaxCycles = cycles;
        }
        if (MinCycles > cycles) {
            MinCycles = cycles;
        }
        ProbeCycles += cycles;
        Count++;

        if (LastTs - LastReportTs > MinReportPeriod) {
            Report();
        }
    }

    void TCpuTracker::ResetStats() {
        MaxCycles = 0;
        MaxProbe = nullptr;
        MinCycles = ui64(-1);
        ProbeCycles = 0;
        Count = 0;
    }

    void TCpuTracker::Report() {
        if (!Reporting) {
            Reporting = true;
            ReportNotReentrant();
            Reporting = false;
        }
    }

    void TCpuTracker::ReportNotReentrant() {
        if (LastReportTs && Count > 0 && LastTs > LastReportTs) {
            ui64 reportPeriod = LastTs - LastReportTs;
            double share = double(ProbeCycles) / reportPeriod;
            double minMs = MilliSeconds(MinCycles);
            double maxMs = MilliSeconds(MaxCycles);
            double avgMs = MilliSeconds(ProbeCycles) / Count;
            LastReportTs = LastTs;
            ResetStats();
            LWPROBE(PerfReport, share, minMs, maxMs, avgMs);
        } else {
            LastReportTs = LastTs;
            ResetStats();
        }
    }

    double TCpuTracker::MilliSeconds(ui64 cycles) {
        return NHPTimer::GetSeconds(cycles) * 1000.0;
    }

}
