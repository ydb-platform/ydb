#pragma once

#include "defs.h"
#include "actor.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NMonitoring {
    class TMetricRegistry;
}

namespace NActors {
    struct TProcStat {
        ui64 Rss;
        ui64 VolCtxSwtch;
        ui64 NonvolCtxSwtch;

        int Pid;
        char State;
        int Ppid;
        int Pgrp;
        int Session;
        int TtyNr;
        int TPgid;
        unsigned Flags;
        unsigned long MinFlt;
        unsigned long CMinFlt;
        unsigned long MajFlt;
        unsigned long CMajFlt;
        unsigned long Utime;
        unsigned long Stime;
        long CUtime;
        long CStime;
        long Priority;
        long Nice;
        long NumThreads;
        long ItRealValue;
        // StartTime is measured from system boot
        unsigned long long StartTime;
        unsigned long Vsize;
        long RssPages;
        unsigned long RssLim;
        ui64 FileRss;
        ui64 AnonRss;
        ui64 CGroupMemLim;
        ui64 MemTotal;
        ui64 MemAvailable;

        TDuration Uptime;
        TDuration SystemUptime;
        // ...

        TProcStat() {
            Zero(*this);
            Y_UNUSED(PageSize);
        }

        bool Fill(pid_t pid);

    private:
        long PageSize = 0;

        long ObtainPageSize();
    };

    IActor* CreateProcStatCollector(ui32 intervalSec, NMonitoring::TDynamicCounterPtr counters);
    IActor* CreateProcStatCollector(TDuration interval, NMonitoring::TMetricRegistry& registry);
    IActor* CreateProcStatCollector(TDuration interval, std::weak_ptr<NMonitoring::TMetricRegistry> registry);
}
