#pragma once

#include "cgroup_memory.h"
#include "../defs.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <memory>
#include <optional>

namespace NActors {

    using TCGroupV1Limit = TCGroupMemoryLimit;

    struct TCGroupV1CpuStats {
        std::optional<TString> CpuCGroupPath;
        std::optional<TString> CpuAcctCGroupPath;

        // cpuacct.usage is reported in nanoseconds. cpuacct.stat values are
        // reported in USER_HZ ticks and are deliberately kept unconverted.
        ui64 UsageNsec = 0;
        ui64 UserTicks = 0;
        ui64 SystemTicks = 0;

        std::optional<TCGroupV1Limit> QuotaUsec;
        std::optional<ui64> PeriodUsec;
        std::optional<ui64> BurstUsec;
        std::optional<ui64> Shares;
        ui64 NrPeriods = 0;
        ui64 NrThrottled = 0;
        ui64 ThrottledNsec = 0;
    };

    struct TCGroupV1MemoryStats {
        TString CGroupPath;
        ui64 CurrentBytes = 0;
        std::optional<ui64> PeakBytes;
        std::optional<TCGroupV1Limit> MaxBytes;
        std::optional<TCGroupV1Limit> HighBytes;
        std::optional<ui64> SwapCurrentBytes;
        std::optional<TCGroupV1Limit> SwapMaxBytes;

        ui64 AnonBytes = 0;
        ui64 FileBytes = 0;
        ui64 KernelBytes = 0;

        ui64 MaxEvents = 0;
        ui64 OomKillEvents = 0;
        bool UnderOom = false;
    };

    struct TCGroupV1IoDeviceStats {
        TString Device;
        ui64 ReadBytes = 0;
        ui64 WriteBytes = 0;
        ui64 ReadOperations = 0;
        ui64 WriteOperations = 0;
        ui64 DiscardBytes = 0;
        ui64 DiscardOperations = 0;
    };

    struct TCGroupV1IoStats {
        TString CGroupPath;
        TVector<TCGroupV1IoDeviceStats> Devices;
        ui64 ReadBytes = 0;
        ui64 WriteBytes = 0;
        ui64 ReadOperations = 0;
        ui64 WriteOperations = 0;
        ui64 DiscardBytes = 0;
        ui64 DiscardOperations = 0;
    };

    struct TCGroupV1PidsStats {
        TString CGroupPath;
        ui64 Current = 0;
        std::optional<TCGroupV1Limit> Max;
        ui64 MaxEvents = 0;
    };

    struct TCGroupV1Stats {
        std::optional<TCGroupV1CpuStats> Cpu;
        std::optional<TCGroupV1MemoryStats> Memory;
        std::optional<TCGroupV1IoStats> Io;
        std::optional<TCGroupV1PidsStats> Pids;
    };

    using TCGroupV1StatsPtr = std::shared_ptr<const TCGroupV1Stats>;

    struct TCGroupV1StatsConfig {
        // The reader actor is registered in this pool. Use an IO executor pool.
        ui32 ExecutorPoolId = 0;

        // Requests made before this period expires receive the same immutable
        // snapshot. Zero disables caching.
        TDuration RefreshPeriod = TDuration::Seconds(1);

        // Controller mount points. cpu and cpuacct are commonly co-mounted;
        // callers may set both roots to that mount point.
        TString CpuCGroupRoot = "/sys/fs/cgroup/cpu,cpuacct";
        TString CpuAcctCGroupRoot = "/sys/fs/cgroup/cpu,cpuacct";
        TString MemoryCGroupRoot = "/sys/fs/cgroup/memory";
        TString BlkIoCGroupRoot = "/sys/fs/cgroup/blkio";
        TString PidsCGroupRoot = "/sys/fs/cgroup/pids";
        TString ProcSelfCGroup = "/proc/self/cgroup";
    };

    class TCGroupV1StatsSubSystem
        : public ICGroupMemoryStatsProvider
    {
    public:
        explicit TCGroupV1StatsSubSystem(TCGroupV1StatsConfig config);

        // The actor system must be running. Sends TEvCGroupV1Stats to the
        // local recipient actor and preserves cookie in IEventHandle::Cookie.
        // File IO is performed by the reader actor in Config.ExecutorPoolId. A
        // null Stats pointer means the process does not belong to a recognized
        // v1 controller or the read failed.
        void ReadStats(const TActorId& recipient, ui64 cookie = 0) const;

        // Version-neutral memory adapter consumed by the OOM controller.
        void ReadMemoryStats(const TActorId& recipient, ui64 cookie = 0) const override;

        void OnAfterStart(TActorSystem& actorSystem) override;
        void OnBeforeStop(TActorSystem& actorSystem) override;
        void OnAfterStop(TActorSystem& actorSystem) override;

    private:
        const TCGroupV1StatsConfig Config;

        mutable TMutex Mutex;
        TActorSystem* ActorSystem = nullptr;
        TActorId ReaderActorId;
        bool Stopping = false;
    };

    std::unique_ptr<TCGroupV1StatsSubSystem> MakeCGroupV1StatsSubSystem(TCGroupV1StatsConfig config);

    const TCGroupV1StatsSubSystem& GetCGroupV1StatsSubSystem(const TActorSystem& actorSystem);
    const TCGroupV1StatsSubSystem& GetCGroupV1StatsSubSystem();

} // namespace NActors
