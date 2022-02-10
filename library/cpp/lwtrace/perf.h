#pragma once

#include <util/system/types.h>
#include <util/thread/singleton.h>

namespace NLWTrace {
    struct TProbe;

    class TCpuTracker {
    private:
        const ui64 MinReportPeriod;

        // State
        bool Reporting = false;
        ui64 LastTs = 0;
        ui64 LastReportTs = 0;

        // Statistics
        ui64 MaxCycles;
        const TProbe* MaxProbe;
        ui64 MinCycles;
        ui64 ProbeCycles;
        ui64 Count;

    public:
        TCpuTracker();
        void Enter();
        void Exit(const TProbe* Probe);
        static TCpuTracker* TlsInstance() {
            struct TCpuTrackerkHolder {
                TCpuTracker Tracker;
            };
            return &FastTlsSingletonWithPriority<TCpuTrackerkHolder, 4>()->Tracker;
        }

    private:
        void AddStats(const TProbe* probe, ui64 cycles);
        void ResetStats();
        void Report();
        void ReportNotReentrant();
        static double MilliSeconds(ui64 cycles);
    };

    class TScopedThreadCpuTracker {
    private:
        const TProbe* Probe;
        TCpuTracker* Tracker;

    public:
        template <class T>
        explicit TScopedThreadCpuTracker(const T& probe)
            : Probe(&probe.Probe)
            , Tracker(TCpuTracker::TlsInstance())
        {
            Tracker->Enter();
        }

        ~TScopedThreadCpuTracker() {
            Tracker->Exit(Probe);
        }
    };

}
