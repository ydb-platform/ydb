#include "actorsystem.h"
#include "actor_bootstrapped.h"
#include "hfunc.h"
#include "process_stats.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

#include <util/datetime/uptime.h>
#include <util/system/defaults.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/string/vector.h>
#include <util/string/split.h>

#ifndef _win_
#include <sys/user.h>
#endif

namespace NActors {
#ifdef _linux_

    namespace {
        template <typename TVal>
        static bool ExtractVal(const TString& str, const TString& name, TVal& res) {
            if (!str.StartsWith(name))
                return false;
            size_t pos = name.size();
            while (pos < str.size() && (str[pos] == ' ' || str[pos] == '\t')) {
                pos++;
            }
            res = atol(str.data() + pos);
            return true;
        }

        float TicksPerMillisec() {
#ifdef _SC_CLK_TCK
            return sysconf(_SC_CLK_TCK) / 1000.0;
#else
            return 1.f;
#endif
        }

        void ConvertFromKb(ui64& value) {
            value *= 1024;
        }
    }

    bool TProcStat::Fill(pid_t pid) {
        try {
            TString strPid(ToString(pid));
            TString line;
            TVector<TString> fields;

            TFileInput proc("/proc/" + strPid + "/status");
            while (proc.ReadLine(line)) {
                if (ExtractVal(line, "VmRSS:", Rss))
                    continue;
                if (ExtractVal(line, "voluntary_ctxt_switches:", VolCtxSwtch))
                    continue;
                if (ExtractVal(line, "nonvoluntary_ctxt_switches:", NonvolCtxSwtch))
                    continue;
            }
            ConvertFromKb(Rss);

            float tickPerMillisec = TicksPerMillisec();

            TFileInput procStat("/proc/" + strPid + "/stat");
            if (procStat.ReadLine(line)) {
                sscanf(line.data(),
                       "%d %*s %c %d %d %d %d %d %u %lu %lu "
                       "%lu %lu %lu %lu %ld %ld %ld %ld %ld "
                       "%ld %llu %lu %ld %lu",
                       &Pid, &State, &Ppid, &Pgrp, &Session, &TtyNr, &TPgid, &Flags, &MinFlt, &CMinFlt,
                       &MajFlt, &CMajFlt, &Utime, &Stime, &CUtime, &CStime, &Priority, &Nice, &NumThreads,
                       &ItRealValue, &StartTime, &Vsize, &RssPages, &RssLim);
                Utime /= tickPerMillisec;
                Stime /= tickPerMillisec;
                CUtime /= tickPerMillisec;
                CStime /= tickPerMillisec;
                SystemUptime = ::Uptime();
                Uptime = SystemUptime - TDuration::MilliSeconds(StartTime / TicksPerMillisec());
            }

            TFileInput statm("/proc/" + strPid + "/statm");
            if (statm.ReadLine(line)) {
                TVector<TString> fields;
                StringSplitter(line).Split(' ').SkipEmpty().Collect(&fields);
                if (fields.size() >= 7) {
                    ui64 resident = FromString<ui64>(fields[1]);
                    ui64 shared = FromString<ui64>(fields[2]);
                    if (PageSize == 0) {
                        PageSize = ObtainPageSize();
                    }
                    FileRss = shared * PageSize;
                    AnonRss = (resident - shared) * PageSize;
                }
            }

            TFileInput cgroup("/proc/" + strPid + "/cgroup");
            TString memoryCGroup;
            while (cgroup.ReadLine(line)) {
                StringSplitter(line).Split(':').Collect(&fields);
                if (fields.size() > 2 && fields[1] == "memory") {
                    memoryCGroup = fields[2];
                    break;
                }
            }

            TString cgroupFileName = "/sys/fs/cgroup/memory" + memoryCGroup + "/memory.limit_in_bytes";
            if (!NFs::Exists(cgroupFileName)) {
                // fallback for mk8s
                cgroupFileName = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
            }
            TFileInput limit(cgroupFileName);
            if (limit.ReadLine(line) > 0) {
                CGroupMemLim = FromString<ui64>(line);
                if (CGroupMemLim > (1ULL << 40)) {
                    CGroupMemLim = 0;
                }
            }

            TFileInput memInfo("/proc/meminfo");
            while (memInfo.ReadLine(line)) {
                if (ExtractVal(line, "MemTotal:", MemTotal))
                    continue;
                if (ExtractVal(line, "MemAvailable:", MemAvailable))
                    continue;
            }
            ConvertFromKb(MemTotal);
            ConvertFromKb(MemAvailable);
        } catch (...) {
            return false;
        }
        return true;
    }

    long TProcStat::ObtainPageSize() {
        long sz = sysconf(_SC_PAGESIZE);
        return sz;
    }

#else

    bool TProcStat::Fill(pid_t pid) {
        Y_UNUSED(pid);
        return false;
    }

    long TProcStat::ObtainPageSize() {
        return 0;
    }

#endif

namespace {
    // Periodically collects process stats and exposes them as mon counters
    template <typename TDerived>
    class TProcStatCollectingActor: public TActorBootstrapped<TProcStatCollectingActor<TDerived>> {
    public:
        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::EActivityType::ACTORLIB_STATS;
        }

        TProcStatCollectingActor(TDuration interval)
            : Interval(interval)
        {
        }

        void Bootstrap(const TActorContext& ctx) {
            TryUpdateCounters();
            ctx.Schedule(Interval, new TEvents::TEvWakeup());
            static_cast<TDerived*>(this)->Become(&TDerived::StateWork);
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                CFunc(TEvents::TSystem::Wakeup, Wakeup);
            }
        }

    private:
        void Wakeup(const TActorContext& ctx) {
            TryUpdateCounters();
            ctx.Schedule(Interval, new TEvents::TEvWakeup());
        }

        void TryUpdateCounters() {
            if (ProcStat.Fill(getpid())) {
                static_cast<TDerived*>(this)->UpdateCounters(ProcStat);
            }
        }

    private:
        const TDuration Interval;
        TProcStat ProcStat;
    };

    // Periodically collects process stats and exposes them as mon counters
    class TDynamicCounterCollector: public TProcStatCollectingActor<TDynamicCounterCollector> {
        using TBase = TProcStatCollectingActor<TDynamicCounterCollector>;
    public:
        TDynamicCounterCollector(
            ui32 intervalSeconds,
            NMonitoring::TDynamicCounterPtr counters)
            : TBase{TDuration::Seconds(intervalSeconds)}
        {
            ProcStatGroup = counters->GetSubgroup("counters", "utils");

            VmSize = ProcStatGroup->GetCounter("Process/VmSize", false);
            AnonRssSize = ProcStatGroup->GetCounter("Process/AnonRssSize", false);
            FileRssSize = ProcStatGroup->GetCounter("Process/FileRssSize", false);
            CGroupMemLimit = ProcStatGroup->GetCounter("Process/CGroupMemLimit", false);
            UserTime = ProcStatGroup->GetCounter("Process/UserTime", true);
            SysTime = ProcStatGroup->GetCounter("Process/SystemTime", true);
            MinorPageFaults = ProcStatGroup->GetCounter("Process/MinorPageFaults", true);
            MajorPageFaults = ProcStatGroup->GetCounter("Process/MajorPageFaults", true);
            UptimeSeconds = ProcStatGroup->GetCounter("Process/UptimeSeconds", false);
            NumThreads = ProcStatGroup->GetCounter("Process/NumThreads", false);
            SystemUptimeSeconds = ProcStatGroup->GetCounter("System/UptimeSeconds", false);
        }

        void UpdateCounters(const TProcStat& procStat) {
            *VmSize = procStat.Vsize;
            *AnonRssSize = procStat.AnonRss;
            *FileRssSize = procStat.FileRss;
            if (procStat.CGroupMemLim) {
                *CGroupMemLimit = procStat.CGroupMemLim;
            }
            *UserTime = procStat.Utime;
            *SysTime = procStat.Stime;
            *MinorPageFaults = procStat.MinFlt;
            *MajorPageFaults = procStat.MajFlt;
            *UptimeSeconds = procStat.Uptime.Seconds();
            *NumThreads = procStat.NumThreads;
            *SystemUptimeSeconds = procStat.Uptime.Seconds();
        }

    private:
        NMonitoring::TDynamicCounterPtr ProcStatGroup;
        NMonitoring::TDynamicCounters::TCounterPtr VmSize;
        NMonitoring::TDynamicCounters::TCounterPtr AnonRssSize;
        NMonitoring::TDynamicCounters::TCounterPtr FileRssSize;
        NMonitoring::TDynamicCounters::TCounterPtr CGroupMemLimit;
        NMonitoring::TDynamicCounters::TCounterPtr UserTime;
        NMonitoring::TDynamicCounters::TCounterPtr SysTime;
        NMonitoring::TDynamicCounters::TCounterPtr MinorPageFaults;
        NMonitoring::TDynamicCounters::TCounterPtr MajorPageFaults;
        NMonitoring::TDynamicCounters::TCounterPtr UptimeSeconds;
        NMonitoring::TDynamicCounters::TCounterPtr NumThreads;
        NMonitoring::TDynamicCounters::TCounterPtr SystemUptimeSeconds;
    };

    class TRegistryCollector: public TProcStatCollectingActor<TRegistryCollector> {
        using TBase = TProcStatCollectingActor<TRegistryCollector>;
    public:
        TRegistryCollector(TDuration interval, NMonitoring::TMetricRegistry& registry)
            : TBase{interval}
        {
            VmSize = registry.IntGauge({{"sensor", "process.VmSize"}});
            AnonRssSize = registry.IntGauge({{"sensor", "process.AnonRssSize"}});
            FileRssSize = registry.IntGauge({{"sensor", "process.FileRssSize"}});
            CGroupMemLimit = registry.IntGauge({{"sensor", "process.CGroupMemLimit"}});
            UptimeSeconds = registry.IntGauge({{"sensor", "process.UptimeSeconds"}});
            NumThreads = registry.IntGauge({{"sensor", "process.NumThreads"}});
            SystemUptimeSeconds = registry.IntGauge({{"sensor", "system.UptimeSeconds"}});

            UserTime = registry.Rate({{"sensor", "process.UserTime"}});
            SysTime = registry.Rate({{"sensor", "process.SystemTime"}});
            MinorPageFaults = registry.Rate({{"sensor", "process.MinorPageFaults"}});
            MajorPageFaults = registry.Rate({{"sensor", "process.MajorPageFaults"}});
        }

        void UpdateCounters(const TProcStat& procStat) {
            VmSize->Set(procStat.Vsize);
            AnonRssSize->Set(procStat.AnonRss);
            FileRssSize->Set(procStat.FileRss);
            CGroupMemLimit->Set(procStat.CGroupMemLim);
            UptimeSeconds->Set(procStat.Uptime.Seconds());
            NumThreads->Set(procStat.NumThreads);
            SystemUptimeSeconds->Set(procStat.SystemUptime.Seconds());

            // it is ok here to reset and add metric value, because mutation
            // is performed in single threaded context

            UserTime->Reset();
            UserTime->Add(procStat.Utime);

            SysTime->Reset();
            SysTime->Add(procStat.Stime);

            MinorPageFaults->Reset();
            MinorPageFaults->Add(procStat.MinFlt);

            MajorPageFaults->Reset();
            MajorPageFaults->Add(procStat.MajFlt);
        }

    private:
        NMonitoring::TIntGauge* VmSize;
        NMonitoring::TIntGauge* AnonRssSize;
        NMonitoring::TIntGauge* FileRssSize;
        NMonitoring::TIntGauge* CGroupMemLimit;
        NMonitoring::TRate* UserTime;
        NMonitoring::TRate* SysTime;
        NMonitoring::TRate* MinorPageFaults;
        NMonitoring::TRate* MajorPageFaults;
        NMonitoring::TIntGauge* UptimeSeconds;
        NMonitoring::TIntGauge* NumThreads;
        NMonitoring::TIntGauge* SystemUptimeSeconds;
    };

    class TRegistryCollectorShared: public TProcStatCollectingActor<TRegistryCollectorShared> {
        using TBase = TProcStatCollectingActor<TRegistryCollectorShared>;
    public:
        TRegistryCollectorShared(TDuration interval, std::weak_ptr<NMonitoring::TMetricRegistry> registry)
            : TBase{interval}
            , Registry(std::move(registry))
        {
        }

        void UpdateCounters(const TProcStat& procStat) {
            std::shared_ptr<NMonitoring::TMetricRegistry> registry = Registry.lock();
            if (registry) {
                registry->IntGauge({{"sensor", "process.VmSize"}})->Set(procStat.Vsize);
                registry->IntGauge({{"sensor", "process.AnonRssSize"}})->Set(procStat.AnonRss);
                registry->IntGauge({{"sensor", "process.FileRssSize"}})->Set(procStat.FileRss);
                registry->IntGauge({{"sensor", "process.CGroupMemLimit"}})->Set(procStat.CGroupMemLim);
                registry->IntGauge({{"sensor", "process.UptimeSeconds"}})->Set(procStat.Uptime.Seconds());
                registry->IntGauge({{"sensor", "process.NumThreads"}})->Set(procStat.NumThreads);
                registry->IntGauge({{"sensor", "system.UptimeSeconds"}})->Set(procStat.SystemUptime.Seconds());

                // it is ok here to reset and add metric value, because mutation
                // is performed in single threaded context

                NMonitoring::TRate* userTime = registry->Rate({{"sensor", "process.UserTime"}});
                NMonitoring::TRate* sysTime = registry->Rate({{"sensor", "process.SystemTime"}});
                NMonitoring::TRate* minorPageFaults = registry->Rate({{"sensor", "process.MinorPageFaults"}});
                NMonitoring::TRate* majorPageFaults = registry->Rate({{"sensor", "process.MajorPageFaults"}});

                userTime->Reset();
                userTime->Add(procStat.Utime);

                sysTime->Reset();
                sysTime->Add(procStat.Stime);

                minorPageFaults->Reset();
                minorPageFaults->Add(procStat.MinFlt);

                majorPageFaults->Reset();
                majorPageFaults->Add(procStat.MajFlt);
            }
        }

    private:
        std::weak_ptr<NMonitoring::TMetricRegistry> Registry;
    };
} // namespace

    IActor* CreateProcStatCollector(ui32 intervalSec, NMonitoring::TDynamicCounterPtr counters) {
        return new TDynamicCounterCollector(intervalSec, counters);
    }

    IActor* CreateProcStatCollector(TDuration interval, NMonitoring::TMetricRegistry& registry) {
        return new TRegistryCollector(interval, registry);
    }

    IActor* CreateProcStatCollector(TDuration interval, std::weak_ptr<NMonitoring::TMetricRegistry> registry) {
        return new TRegistryCollectorShared(interval, std::move(registry));
    }
}
