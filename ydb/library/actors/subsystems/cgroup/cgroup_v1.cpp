#include "cgroup_v1.h"

#include "cgroup_events.h"
#include "cgroup_helpers.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NActors {

    namespace {

        constexpr ui64 V1UnlimitedThreshold = 1ULL << 60;

        using NCGroupDetail::MakeCGroupDirectory;
        using NCGroupDetail::ParseKeyValueFile;
        using NCGroupDetail::TryReadFile;
        using NCGroupDetail::TryReadUInt;

        std::optional<i64> TryReadInt(const TFsPath& path) {
            auto content = TryReadFile(path);
            if (!content) {
                return std::nullopt;
            }

            i64 value = 0;
            if (!TryFromString(StripString(*content), value)) {
                return std::nullopt;
            }
            return value;
        }

        std::optional<TCGroupV1Limit> MakeMemoryLimit(std::optional<ui64> value) {
            if (!value) {
                return std::nullopt;
            }
            if (*value >= V1UnlimitedThreshold) {
                return TCGroupV1Limit{.Unlimited = true};
            }
            return TCGroupV1Limit{.Value = *value};
        }

        std::optional<TCGroupV1Limit> TryReadMaxLimit(const TFsPath& path) {
            auto content = TryReadFile(path);
            if (!content) {
                return std::nullopt;
            }

            const TString value = StripString(*content);
            if (value == "max") {
                return TCGroupV1Limit{.Unlimited = true};
            }

            ui64 parsed = 0;
            if (!TryFromString(value, parsed)) {
                return std::nullopt;
            }
            return TCGroupV1Limit{.Value = parsed};
        }

        struct TCGroupV1ControllerPaths {
            std::optional<TString> Cpu;
            std::optional<TString> CpuAcct;
            std::optional<TString> Memory;
            std::optional<TString> BlkIo;
            std::optional<TString> Pids;

            bool Empty() const {
                return !Cpu && !CpuAcct && !Memory && !BlkIo && !Pids;
            }
        };

        TString NormalizeCGroupPath(TString path) {
            if (path.empty()) {
                return "/";
            }
            if (path.front() != '/') {
                path.prepend('/');
            }
            return path;
        }

        std::optional<TCGroupV1ControllerPaths> FindControllerPaths(const TString& procSelfCGroup) {
            auto content = TryReadFile(procSelfCGroup);
            if (!content) {
                return std::nullopt;
            }

            TCGroupV1ControllerPaths result;
            TStringInput input(*content);
            TString line;
            while (input.ReadLine(line)) {
                TVector<TString> fields;
                StringSplitter(line).Split(':').Collect(&fields);
                if (fields.size() != 3 || fields[1].empty()) {
                    continue;
                }

                const TString path = NormalizeCGroupPath(fields[2]);
                TVector<TString> controllers;
                StringSplitter(fields[1]).Split(',').SkipEmpty().Collect(&controllers);
                for (const TString& controller : controllers) {
                    if (controller == "cpu") {
                        result.Cpu = path;
                    } else if (controller == "cpuacct") {
                        result.CpuAcct = path;
                    } else if (controller == "memory") {
                        result.Memory = path;
                    } else if (controller == "blkio") {
                        result.BlkIo = path;
                    } else if (controller == "pids") {
                        result.Pids = path;
                    }
                }
            }

            return result.Empty()
                ? std::nullopt
                : std::optional<TCGroupV1ControllerPaths>(std::move(result));
        }

        struct TMemoryStatFields {
            std::optional<ui64> Rss;
            std::optional<ui64> TotalRss;
            std::optional<ui64> Cache;
            std::optional<ui64> TotalCache;
            std::optional<ui64> KernelStack;
            std::optional<ui64> TotalKernelStack;
            std::optional<ui64> Slab;
            std::optional<ui64> TotalSlab;
            std::optional<ui64> HierarchicalMemoryLimit;
        };

        TMemoryStatFields ReadMemoryStat(const TFsPath& directory) {
            TMemoryStatFields result;
            if (auto content = TryReadFile(directory / "memory.stat")) {
                ParseKeyValueFile(*content, [&result](TStringBuf key, ui64 value) {
                    if (key == "rss") {
                        result.Rss = value;
                    } else if (key == "total_rss") {
                        result.TotalRss = value;
                    } else if (key == "cache") {
                        result.Cache = value;
                    } else if (key == "total_cache") {
                        result.TotalCache = value;
                    } else if (key == "kernel_stack") {
                        result.KernelStack = value;
                    } else if (key == "total_kernel_stack") {
                        result.TotalKernelStack = value;
                    } else if (key == "slab") {
                        result.Slab = value;
                    } else if (key == "total_slab") {
                        result.TotalSlab = value;
                    } else if (key == "hierarchical_memory_limit") {
                        result.HierarchicalMemoryLimit = value;
                    }
                });
            }
            return result;
        }

        std::optional<TCGroupV1MemoryStats> ReadMemoryStats(
                const TFsPath& directory,
                const TString& cGroupPath) {
            auto currentBytes = TryReadUInt(directory / "memory.usage_in_bytes");
            if (!currentBytes) {
                return std::nullopt;
            }

            TCGroupV1MemoryStats result;
            result.CGroupPath = cGroupPath;
            result.CurrentBytes = *currentBytes;
            result.PeakBytes = TryReadUInt(directory / "memory.max_usage_in_bytes");
            result.MaxBytes = MakeMemoryLimit(TryReadUInt(directory / "memory.limit_in_bytes"));
            result.HighBytes = MakeMemoryLimit(TryReadUInt(directory / "memory.soft_limit_in_bytes"));
            result.MaxEvents = TryReadUInt(directory / "memory.failcnt").value_or(0);

            const TMemoryStatFields memoryStat = ReadMemoryStat(directory);
            result.AnonBytes = memoryStat.TotalRss.value_or(memoryStat.Rss.value_or(0));
            result.FileBytes = memoryStat.TotalCache.value_or(memoryStat.Cache.value_or(0));
            result.KernelBytes =
                memoryStat.TotalKernelStack.value_or(memoryStat.KernelStack.value_or(0)) +
                memoryStat.TotalSlab.value_or(memoryStat.Slab.value_or(0));

            if (auto hierarchicalLimit = MakeMemoryLimit(memoryStat.HierarchicalMemoryLimit)) {
                if (!hierarchicalLimit->Unlimited &&
                        (!result.MaxBytes || result.MaxBytes->Unlimited ||
                            hierarchicalLimit->Value < result.MaxBytes->Value)) {
                    result.MaxBytes = hierarchicalLimit;
                }
            }

            if (auto memswCurrent = TryReadUInt(directory / "memory.memsw.usage_in_bytes")) {
                result.SwapCurrentBytes = *memswCurrent > result.CurrentBytes
                    ? *memswCurrent - result.CurrentBytes
                    : 0;
            }
            if (auto memswLimit = MakeMemoryLimit(TryReadUInt(directory / "memory.memsw.limit_in_bytes"))) {
                if (memswLimit->Unlimited) {
                    result.SwapMaxBytes = TCGroupV1Limit{.Unlimited = true};
                } else if (result.MaxBytes && !result.MaxBytes->Unlimited) {
                    result.SwapMaxBytes = TCGroupV1Limit{
                        .Value = memswLimit->Value > result.MaxBytes->Value
                            ? memswLimit->Value - result.MaxBytes->Value
                            : 0,
                    };
                }
            }

            if (auto oomControl = TryReadFile(directory / "memory.oom_control")) {
                ParseKeyValueFile(*oomControl, [&result](TStringBuf key, ui64 value) {
                    if (key == "under_oom") {
                        result.UnderOom = value != 0;
                    } else if (key == "oom_kill") {
                        result.OomKillEvents = value;
                    }
                });
            }

            return result;
        }

        std::optional<TCGroupV1MemoryStats> ReadMemoryStats(
                const TString& root,
                const TString& cGroupPath) {
            if (auto result = ReadMemoryStats(MakeCGroupDirectory(root, cGroupPath), cGroupPath)) {
                return result;
            }
            if (cGroupPath != "/") {
                // A cgroup namespace may expose the process group as the mount root.
                return ReadMemoryStats(TFsPath(root), cGroupPath);
            }
            return std::nullopt;
        }

        bool ReadCpuAcctStats(const TFsPath& directory, TCGroupV1CpuStats& result) {
            bool found = false;
            if (auto usage = TryReadUInt(directory / "cpuacct.usage")) {
                result.UsageNsec = *usage;
                found = true;
            }
            if (auto stat = TryReadFile(directory / "cpuacct.stat")) {
                found = true;
                ParseKeyValueFile(*stat, [&result](TStringBuf key, ui64 value) {
                    if (key == "user") {
                        result.UserTicks = value;
                    } else if (key == "system") {
                        result.SystemTicks = value;
                    }
                });
            }
            return found;
        }

        bool ReadCpuControllerStats(const TFsPath& directory, TCGroupV1CpuStats& result) {
            bool found = false;
            if (auto quota = TryReadInt(directory / "cpu.cfs_quota_us")) {
                result.QuotaUsec = *quota < 0
                    ? TCGroupV1Limit{.Unlimited = true}
                    : TCGroupV1Limit{.Value = static_cast<ui64>(*quota)};
                found = true;
            }
            if (auto period = TryReadUInt(directory / "cpu.cfs_period_us")) {
                result.PeriodUsec = *period;
                found = true;
            }
            if (auto burst = TryReadUInt(directory / "cpu.cfs_burst_us")) {
                result.BurstUsec = *burst;
                found = true;
            }
            if (auto shares = TryReadUInt(directory / "cpu.shares")) {
                result.Shares = *shares;
                found = true;
            }
            if (auto stat = TryReadFile(directory / "cpu.stat")) {
                found = true;
                ParseKeyValueFile(*stat, [&result](TStringBuf key, ui64 value) {
                    if (key == "nr_periods") {
                        result.NrPeriods = value;
                    } else if (key == "nr_throttled") {
                        result.NrThrottled = value;
                    } else if (key == "throttled_time") {
                        result.ThrottledNsec = value;
                    } else if (key == "throttled_usec") {
                        result.ThrottledNsec = value * 1000;
                    }
                });
            }
            return found;
        }

        template<class TReader>
        bool ReadWithNamespaceFallback(
                const TString& root,
                const TString& cGroupPath,
                TReader&& reader) {
            if (reader(MakeCGroupDirectory(root, cGroupPath))) {
                return true;
            }
            return cGroupPath != "/" && reader(TFsPath(root));
        }

        std::optional<TCGroupV1CpuStats> ReadCpuStats(
                const TCGroupV1StatsConfig& config,
                const TCGroupV1ControllerPaths& paths) {
            TCGroupV1CpuStats result;
            bool found = false;

            if (paths.CpuAcct) {
                if (ReadWithNamespaceFallback(
                        config.CpuAcctCGroupRoot,
                        *paths.CpuAcct,
                        [&result](const TFsPath& directory) {
                            return ReadCpuAcctStats(directory, result);
                        })) {
                    result.CpuAcctCGroupPath = *paths.CpuAcct;
                    found = true;
                }
            }

            if (paths.Cpu) {
                if (ReadWithNamespaceFallback(
                        config.CpuCGroupRoot,
                        *paths.Cpu,
                        [&result](const TFsPath& directory) {
                            return ReadCpuControllerStats(directory, result);
                        })) {
                    result.CpuCGroupPath = *paths.Cpu;
                    found = true;
                }
            }

            return found
                ? std::optional<TCGroupV1CpuStats>(std::move(result))
                : std::nullopt;
        }

        TCGroupV1IoDeviceStats& GetIoDevice(TCGroupV1IoStats& stats, TStringBuf device) {
            for (auto& current : stats.Devices) {
                if (current.Device == device) {
                    return current;
                }
            }
            auto& result = stats.Devices.emplace_back();
            result.Device = device;
            return result;
        }

        void ParseIoStatsFile(const TString& content, bool operations, TCGroupV1IoStats& result) {
            TStringInput input(content);
            TString line;
            while (input.ReadLine(line)) {
                TVector<TString> fields;
                StringSplitter(line).Split(' ').SkipEmpty().Collect(&fields);
                if (fields.size() != 3 || fields[0] == "Total") {
                    continue;
                }

                ui64 value = 0;
                if (!TryFromString(fields[2], value)) {
                    continue;
                }

                if (fields[1] != "Read" && fields[1] != "Write" && fields[1] != "Discard") {
                    // Sync, Async and Total overlap Read/Write and must not be
                    // included in the aggregate a second time.
                    continue;
                }

                auto& device = GetIoDevice(result, fields[0]);
                if (fields[1] == "Read") {
                    if (operations) {
                        device.ReadOperations = value;
                    } else {
                        device.ReadBytes = value;
                    }
                } else if (fields[1] == "Write") {
                    if (operations) {
                        device.WriteOperations = value;
                    } else {
                        device.WriteBytes = value;
                    }
                } else if (fields[1] == "Discard") {
                    if (operations) {
                        device.DiscardOperations = value;
                    } else {
                        device.DiscardBytes = value;
                    }
                }
            }
        }

        std::optional<TString> ReadFirstIoFile(
                const TFsPath& directory,
                TStringBuf throttleFile,
                TStringBuf recursiveFile,
                TStringBuf regularFile) {
            if (auto content = TryReadFile(directory / throttleFile)) {
                return content;
            }
            if (auto content = TryReadFile(directory / recursiveFile)) {
                return content;
            }
            return TryReadFile(directory / regularFile);
        }

        std::optional<TCGroupV1IoStats> ReadIoStats(
                const TFsPath& directory,
                const TString& cGroupPath) {
            auto bytes = ReadFirstIoFile(
                directory,
                "blkio.throttle.io_service_bytes",
                "blkio.io_service_bytes_recursive",
                "blkio.io_service_bytes");
            auto operations = ReadFirstIoFile(
                directory,
                "blkio.throttle.io_serviced",
                "blkio.io_serviced_recursive",
                "blkio.io_serviced");
            if (!bytes && !operations) {
                return std::nullopt;
            }

            TCGroupV1IoStats result;
            result.CGroupPath = cGroupPath;
            if (bytes) {
                ParseIoStatsFile(*bytes, false, result);
            }
            if (operations) {
                ParseIoStatsFile(*operations, true, result);
            }
            for (const auto& device : result.Devices) {
                result.ReadBytes += device.ReadBytes;
                result.WriteBytes += device.WriteBytes;
                result.ReadOperations += device.ReadOperations;
                result.WriteOperations += device.WriteOperations;
                result.DiscardBytes += device.DiscardBytes;
                result.DiscardOperations += device.DiscardOperations;
            }
            return result;
        }

        std::optional<TCGroupV1IoStats> ReadIoStats(
                const TString& root,
                const TString& cGroupPath) {
            if (auto result = ReadIoStats(MakeCGroupDirectory(root, cGroupPath), cGroupPath)) {
                return result;
            }
            if (cGroupPath != "/") {
                return ReadIoStats(TFsPath(root), cGroupPath);
            }
            return std::nullopt;
        }

        std::optional<TCGroupV1PidsStats> ReadPidsStats(
                const TFsPath& directory,
                const TString& cGroupPath) {
            auto current = TryReadUInt(directory / "pids.current");
            if (!current) {
                return std::nullopt;
            }

            TCGroupV1PidsStats result;
            result.CGroupPath = cGroupPath;
            result.Current = *current;
            result.Max = TryReadMaxLimit(directory / "pids.max");
            if (auto events = TryReadFile(directory / "pids.events")) {
                ParseKeyValueFile(*events, [&result](TStringBuf key, ui64 value) {
                    if (key == "max") {
                        result.MaxEvents = value;
                    }
                });
            }
            return result;
        }

        std::optional<TCGroupV1PidsStats> ReadPidsStats(
                const TString& root,
                const TString& cGroupPath) {
            if (auto result = ReadPidsStats(MakeCGroupDirectory(root, cGroupPath), cGroupPath)) {
                return result;
            }
            if (cGroupPath != "/") {
                return ReadPidsStats(TFsPath(root), cGroupPath);
            }
            return std::nullopt;
        }

        std::optional<TCGroupV1Stats> ReadCGroupV1Stats(const TCGroupV1StatsConfig& config) {
            auto paths = FindControllerPaths(config.ProcSelfCGroup);
            if (!paths) {
                return std::nullopt;
            }

            TCGroupV1Stats result;
            result.Cpu = ReadCpuStats(config, *paths);
            if (paths->Memory) {
                result.Memory = ReadMemoryStats(config.MemoryCGroupRoot, *paths->Memory);
            }
            if (paths->BlkIo) {
                result.Io = ReadIoStats(config.BlkIoCGroupRoot, *paths->BlkIo);
            }
            if (paths->Pids) {
                result.Pids = ReadPidsStats(config.PidsCGroupRoot, *paths->Pids);
            }
            return result;
        }

        TCGroupMemoryStatsPtr MakeMemoryStats(const TCGroupV1StatsPtr& stats) {
            if (!stats || !stats->Memory) {
                return nullptr;
            }

            const auto& source = *stats->Memory;
            TCGroupMemoryStats result;
            result.Version = ECGroupVersion::V1;
            result.CGroupPath = source.CGroupPath;
            result.CurrentBytes = source.CurrentBytes;
            result.PeakBytes = source.PeakBytes;
            result.MaxBytes = source.MaxBytes;
            result.HighBytes = source.HighBytes;
            result.SwapCurrentBytes = source.SwapCurrentBytes;
            result.SwapMaxBytes = source.SwapMaxBytes;
            result.AnonBytes = source.AnonBytes;
            result.FileBytes = source.FileBytes;
            result.KernelBytes = source.KernelBytes;
            result.MaxEvents = source.MaxEvents;
            return std::make_shared<const TCGroupMemoryStats>(std::move(result));
        }

        enum : ui32 {
            EvReadStats = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvReadStats : TEventLocal<TEvReadStats, EvReadStats> {
            TActorId Recipient;
            ui64 Cookie = 0;
            bool MemoryOnly = false;

            TEvReadStats(TActorId recipient, ui64 cookie, bool memoryOnly)
                : Recipient(recipient)
                , Cookie(cookie)
                , MemoryOnly(memoryOnly)
            {
            }
        };

        class TCGroupV1StatsActor : public TActorBootstrapped<TCGroupV1StatsActor> {
        public:
            explicit TCGroupV1StatsActor(TCGroupV1StatsConfig config)
                : Config(std::move(config))
            {
            }

            void Bootstrap() {
                Become(&TThis::StateWork);
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvReadStats, Handle)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )

        private:
            void Handle(TEvReadStats::TPtr& ev) {
                const TMonotonic now = TActivationContext::Monotonic();
                if (!CacheInitialized || Config.RefreshPeriod == TDuration::Zero() || now >= NextRefresh) {
                    try {
                        auto stats = ReadCGroupV1Stats(Config);
                        CachedStats = stats
                            ? std::make_shared<const TCGroupV1Stats>(std::move(*stats))
                            : nullptr;
                        CacheInitialized = true;
                        NextRefresh = now + Config.RefreshPeriod;
                    } catch (...) {
                        Reply(*ev->Get(), nullptr);
                        return;
                    }
                }

                Reply(*ev->Get(), CachedStats);
            }

            void Reply(const TEvReadStats& request, TCGroupV1StatsPtr stats) {
                if (request.MemoryOnly) {
                    Send(request.Recipient,
                        new TEvCGroupMemoryStats(MakeMemoryStats(stats)),
                        0,
                        request.Cookie);
                } else {
                    Send(request.Recipient,
                        new TEvCGroupV1Stats(std::move(stats)),
                        0,
                        request.Cookie);
                }
            }

        private:
            const TCGroupV1StatsConfig Config;
            TCGroupV1StatsPtr CachedStats;
            TMonotonic NextRefresh;
            bool CacheInitialized = false;
        };

    } // namespace

    TCGroupV1StatsSubSystem::TCGroupV1StatsSubSystem(TCGroupV1StatsConfig config)
        : Config(std::move(config))
    {
        Y_ABORT_UNLESS(Config.RefreshPeriod >= TDuration::Zero(),
            "cgroup v1 stats refresh period must not be negative");
    }

    void TCGroupV1StatsSubSystem::ReadStats(const TActorId& recipient, ui64 cookie) const {
        Y_ABORT_UNLESS(recipient, "cannot send cgroup v1 stats to an empty actor id");
        TActorSystem* actorSystem = nullptr;
        TActorId readerActorId;
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(ActorSystem, "cgroup v1 stats subsystem is not running");
            actorSystem = ActorSystem;
            if (!Stopping) {
                readerActorId = ReaderActorId;
            }
        }

        if (!readerActorId || !actorSystem->Send(readerActorId,
                new TEvReadStats(recipient, cookie, false))) {
            actorSystem->Send(recipient, new TEvCGroupV1Stats(nullptr), 0, cookie);
        }
    }

    void TCGroupV1StatsSubSystem::ReadMemoryStats(
            const TActorId& recipient,
            ui64 cookie) const {
        Y_ABORT_UNLESS(recipient, "cannot send cgroup v1 memory stats to an empty actor id");
        TActorSystem* actorSystem = nullptr;
        TActorId readerActorId;
        {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(ActorSystem, "cgroup v1 stats subsystem is not running");
            actorSystem = ActorSystem;
            if (!Stopping) {
                readerActorId = ReaderActorId;
            }
        }

        if (!readerActorId || !actorSystem->Send(readerActorId,
                new TEvReadStats(recipient, cookie, true))) {
            actorSystem->Send(recipient, new TEvCGroupMemoryStats(nullptr), 0, cookie);
        }
    }

    void TCGroupV1StatsSubSystem::OnAfterStart(TActorSystem& actorSystem) {
        const TActorId readerActorId = actorSystem.Register(
            new TCGroupV1StatsActor(Config), TMailboxType::Simple, Config.ExecutorPoolId);

        TGuard<TMutex> guard(Mutex);
        ActorSystem = &actorSystem;
        ReaderActorId = readerActorId;
        Stopping = false;
    }

    void TCGroupV1StatsSubSystem::OnBeforeStop(TActorSystem& actorSystem) {
        TActorId readerActorId;
        {
            TGuard<TMutex> guard(Mutex);
            Stopping = true;
            readerActorId = ReaderActorId;
        }
        if (readerActorId) {
            actorSystem.Send(readerActorId, new TEvents::TEvPoison());
        }
    }

    void TCGroupV1StatsSubSystem::OnAfterStop(TActorSystem&) {
        TGuard<TMutex> guard(Mutex);
        ActorSystem = nullptr;
        ReaderActorId = {};
    }

    std::unique_ptr<TCGroupV1StatsSubSystem> MakeCGroupV1StatsSubSystem(TCGroupV1StatsConfig config) {
        return std::make_unique<TCGroupV1StatsSubSystem>(std::move(config));
    }

    const TCGroupV1StatsSubSystem& GetCGroupV1StatsSubSystem(const TActorSystem& actorSystem) {
        auto* subSystem = actorSystem.GetSubSystem<TCGroupV1StatsSubSystem>();
        Y_ABORT_UNLESS(subSystem, "cgroup v1 stats subsystem is not registered");
        return *subSystem;
    }

    const TCGroupV1StatsSubSystem& GetCGroupV1StatsSubSystem() {
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto* subSystem = actorSystem->GetSubSystem<TCGroupV1StatsSubSystem>();
        Y_ABORT_UNLESS(subSystem, "cgroup v1 stats subsystem is not registered");
        return *subSystem;
    }

} // namespace NActors
