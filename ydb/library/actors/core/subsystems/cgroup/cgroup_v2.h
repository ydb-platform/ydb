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

    using TCGroupV2Limit = TCGroupMemoryLimit;

    struct TCGroupV2CpuStats {
        ui64 UsageUsec = 0;
        ui64 UserUsec = 0;
        ui64 SystemUsec = 0;
        ui64 NrPeriods = 0;
        ui64 NrThrottled = 0;
        ui64 ThrottledUsec = 0;
        ui64 NrBursts = 0;
        ui64 BurstUsec = 0;
        std::optional<TCGroupV2Limit> QuotaUsec;
        std::optional<ui64> PeriodUsec;
    };

    struct TCGroupV2MemoryStats {
        ui64 CurrentBytes = 0;
        std::optional<ui64> PeakBytes;
        std::optional<TCGroupV2Limit> MaxBytes;
        std::optional<TCGroupV2Limit> HighBytes;
        std::optional<ui64> SwapCurrentBytes;
        std::optional<TCGroupV2Limit> SwapMaxBytes;

        ui64 AnonBytes = 0;
        ui64 FileBytes = 0;
        ui64 KernelBytes = 0;

        ui64 LowEvents = 0;
        ui64 HighEvents = 0;
        ui64 MaxEvents = 0;
        ui64 OomEvents = 0;
        ui64 OomKillEvents = 0;
        ui64 OomGroupKillEvents = 0;
        ui64 SockThrottledEvents = 0;
    };

    struct TCGroupV2IoDeviceStats {
        TString Device;
        ui64 ReadBytes = 0;
        ui64 WriteBytes = 0;
        ui64 ReadOperations = 0;
        ui64 WriteOperations = 0;
        ui64 DiscardBytes = 0;
        ui64 DiscardOperations = 0;
    };

    struct TCGroupV2IoStats {
        TVector<TCGroupV2IoDeviceStats> Devices;
        ui64 ReadBytes = 0;
        ui64 WriteBytes = 0;
        ui64 ReadOperations = 0;
        ui64 WriteOperations = 0;
        ui64 DiscardBytes = 0;
        ui64 DiscardOperations = 0;
    };

    struct TCGroupV2PidsStats {
        ui64 Current = 0;
        std::optional<ui64> Peak;
        std::optional<TCGroupV2Limit> Max;
        ui64 MaxEvents = 0;
    };

    struct TCGroupV2PressureLine {
        double Avg10 = 0;
        double Avg60 = 0;
        double Avg300 = 0;
        ui64 TotalUsec = 0;
    };

    struct TCGroupV2PressureStats {
        std::optional<TCGroupV2PressureLine> Some;
        std::optional<TCGroupV2PressureLine> Full;
    };

    struct TCGroupV2Stats {
        TString CGroupPath;
        std::optional<TCGroupV2CpuStats> Cpu;
        std::optional<TCGroupV2MemoryStats> Memory;
        std::optional<TCGroupV2IoStats> Io;
        std::optional<TCGroupV2PidsStats> Pids;
        std::optional<TCGroupV2PressureStats> CpuPressure;
        std::optional<TCGroupV2PressureStats> MemoryPressure;
        std::optional<TCGroupV2PressureStats> IoPressure;
    };

    using TCGroupV2StatsPtr = std::shared_ptr<const TCGroupV2Stats>;

    struct TCGroupV2StatsConfig {
        // The reader actor is registered in this pool. Use an IO executor pool.
        ui32 ExecutorPoolId = 0;

        // Requests made before this period expires receive the same immutable
        // snapshot. Zero disables caching.
        TDuration RefreshPeriod = TDuration::Seconds(1);

        TString CGroupRoot = "/sys/fs/cgroup";
        TString ProcSelfCGroup = "/proc/self/cgroup";
    };

    class TCGroupV2StatsSubSystem
        : public ICGroupMemoryStatsProvider
    {
    public:
        explicit TCGroupV2StatsSubSystem(TCGroupV2StatsConfig config);

        // The actor system must be running. Sends TEvCGroupV2Stats to the
        // local recipient actor and preserves cookie in IEventHandle::Cookie.
        // File IO is performed by the reader actor in Config.ExecutorPoolId. A
        // null Stats pointer means the process does not belong to a cgroup v2
        // hierarchy or the read failed.
        void ReadStats(const TActorId& recipient, ui64 cookie = 0) const;

        void ReadMemoryStats(const TActorId& recipient, ui64 cookie = 0) const override;

        void OnAfterStart(TActorSystem& actorSystem) override;
        void OnBeforeStop(TActorSystem& actorSystem) override;
        void OnAfterStop(TActorSystem& actorSystem) override;

    private:
        const TCGroupV2StatsConfig Config;

        mutable TMutex Mutex;
        TActorSystem* ActorSystem = nullptr;
        TActorId ReaderActorId;
        bool Stopping = false;
    };

    std::unique_ptr<TCGroupV2StatsSubSystem> MakeCGroupV2StatsSubSystem(TCGroupV2StatsConfig config);

    const TCGroupV2StatsSubSystem& GetCGroupV2StatsSubSystem(const TActorSystem& actorSystem);
    const TCGroupV2StatsSubSystem& GetCGroupV2StatsSubSystem();

} // namespace NActors
