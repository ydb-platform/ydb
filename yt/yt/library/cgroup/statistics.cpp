#include "statistics.h"
#include "process.h"

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/leaky_global.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#include <util/system/fs.h>

namespace NYT::NCGroups {

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "CGroups");

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// NB: Consecutive slashes from empty components are harmless, so we do not bother normalizing.
TString JoinCGroupPath(TStringBuf relativePath, TStringBuf fileName)
{
    return Format("/sys/fs/cgroup/%v/%v", relativePath, fileName);
}

TString StatFilePath(TStringBuf directory, TStringBuf fileName)
{
    return Format("/%v/%v", directory, fileName);
}

THashMap<std::string, i64> ReadAndParseStatFile(const TString& fileName)
{
    THashMap<std::string, i64> statistics;

    TString rawStatFile;
    try {
        rawStatFile = TFileInput(fileName).ReadAll();
    } catch (const TFileError& error) {
        return {};
    }

    for (const auto& line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        if (fields.size() != 2) {
            continue;
        }

        auto [_, emplaced] = statistics.emplace(fields[0], FromString<i64>(fields[1]));
        if (!emplaced) {
            THROW_ERROR_EXCEPTION("Failed to collect CGroup statistics: key already exists")
                .With("filename", fileName)
                .With("key", fields[0]);
        }
    }

    return statistics;
}

std::optional<i64> ReadMemoryLimit(const TString& path)
{
    try {
        if (!NFs::Exists(path)) {
            return std::nullopt;
        }
        auto rawValue = TFileInput(path).ReadLine();
        if (rawValue == "max") {
            return std::nullopt;
        }
        return FromString<i64>(rawValue);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

THashMap<std::string, i64> ReadAndParseBlkIOStatFile(const TString& fileName)
{
    THashMap<std::string, i64> statistics;

    auto rawStatFile = TFileInput(fileName).ReadAll();
    for (const auto& line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        if (fields.size() != 3) {
            continue;
        }

        statistics[fields[1]] += FromString<i64>(fields[2]);
    }

    return statistics;
}

THashMap<std::string, i64> ReadAndParseIOStatFile(const TString& fileName)
{
    THashMap<std::string, i64> statistics;

    auto rawStatFile = TFileInput(fileName).ReadAll();
    for (auto line : SplitString(rawStatFile, "\n")) {
        auto fields = SplitString(line, " ");
        for (int index = 1; index < std::ssize(fields); ++index) {
            auto tokens = SplitString(fields[index], "=");
            if (tokens.size() != 2) {
                continue;
            }
            statistics[tokens[0]] += FromString<i64>(tokens[1]);
        }
    }

    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

// A controller's resolved cgroup directory and the file format to read it in.
struct TCGroupLocation
{
    TString Path;
    bool IsV2 = false;
};

void FormatValue(TStringBuilderBase* builder, const TCGroupLocation& value, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Path: %v, IsV2: %v}", value.Path, value.IsV2);
}

////////////////////////////////////////////////////////////////////////////////

// Resolves where a controller lives for the current process by probing the
// canonical locations for the file the controller is expected to expose.
// This is needed, because Porto exports cgroupv2 mounts under sysfs instead of cgroupv2,
// so we cannot reliably detect whether the system is V1 or V2.
struct TControllerProbe
{
    TStringBuf V1Controller;
    TStringBuf V1ProbeFile;
    TStringBuf V2ProbeFile;
};

std::optional<TCGroupLocation> ResolveController(
    const TControllerProbe& probe,
    const THashMap<std::string, std::string>& selfCGroups)
{
    // NB: There are issues with cgroup namespaces in Kubernetes
    // (see https://github.com/kubernetes/enhancements/pull/1370),
    // where /proc/self/cgroup contains real cgroup path
    // but in /sys/fs/cgroup it is just root cgroup.
    if (auto it = selfCGroups.find(probe.V1Controller); it != selfCGroups.end()) {
        for (auto path : {JoinCGroupPath(probe.V1Controller, it->second), JoinCGroupPath(probe.V1Controller, "")}) {
            if (NFs::Exists(StatFilePath(path, probe.V1ProbeFile))) {
                return TCGroupLocation{.Path = std::move(path), .IsV2 = false};
            }
        }
    }

    if (auto it = selfCGroups.find(""); it != selfCGroups.end()) {
        for (auto path : {JoinCGroupPath(it->second, ""), TString("/sys/fs/cgroup")}) {
            if (NFs::Exists(StatFilePath(path, probe.V2ProbeFile))) {
                return TCGroupLocation{.Path = std::move(path), .IsV2 = true};
            }
        }
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

class TMemoryStatisticsFetcher
{
public:
    explicit TMemoryStatisticsFetcher(const THashMap<std::string, std::string>& selfCGroups)
        : Location(ResolveController({"memory", "memory.stat", "memory.stat"}, selfCGroups))
    { }

    TMemoryStatistics GetStatistics() const
    {
        if (!Location) {
            return {};
        }

        auto statistics = ReadAndParseStatFile(StatFilePath(Location->Path, "memory.stat"));

        return Location->IsV2
            ? TMemoryStatistics{
                .AnonWithSwapCached = statistics["anon"] + statistics["swapcached"],
                .TmpfsUsage = statistics["shmem"],
                .MappedFile = statistics["file_mapped"],
                .MajorPageFaults = statistics["pgmajfault"],
                .Cache = statistics["file"],
                .RssHuge = statistics["anon_thp"],
                .Dirty = statistics["file_dirty"],
                .Writeback = statistics["file_writeback"],
            }
            : TMemoryStatistics{
                .AnonWithSwapCached = statistics["total_rss"],
                .TmpfsUsage = statistics["total_shmem"],
                .MappedFile = statistics["total_mapped_file"],
                .MajorPageFaults = statistics["total_pgmajfault"],
                .Cache = statistics["total_cache"],
                .RssHuge = statistics["total_rss_huge"],
                .Dirty = statistics["total_dirty"],
                .Writeback = statistics["total_writeback"],
            };
    }

    TMemoryLimits GetLimits() const
    {
        if (!Location) {
            return {};
        }

        TMemoryLimits result;
        if (Location->IsV2) {
            // Anonymous memory limit does not exist on cgroups v2.
            result.AnonymousMemoryLimit = std::nullopt;
            result.MemoryLimit = ReadMemoryLimit(StatFilePath(Location->Path, "memory.max"));
        } else {
            result.AnonymousMemoryLimit = ReadMemoryLimit(StatFilePath(Location->Path, "memory.anon.limit"));
            auto statistics = ReadAndParseStatFile(StatFilePath(Location->Path, "memory.stat"));
            auto* memoryLimit = statistics.FindPtr("hierarchical_memory_limit");
            result.MemoryLimit = memoryLimit ? std::optional(*memoryLimit) : std::nullopt;
        }
        return result;
    }

    i64 GetOomKillCount() const
    {
        if (!Location) {
            return 0;
        }

        auto oomPath = StatFilePath(Location->Path, Location->IsV2 ? "memory.events" : "memory.oom_control");
        if (!NFs::Exists(oomPath)) {
            return 0;
        }
        return ReadAndParseStatFile(oomPath)["oom_kill"];
    }

    const std::optional<TCGroupLocation> Location;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuStatisticsFetcher
{
public:
    explicit TCpuStatisticsFetcher(const THashMap<std::string, std::string>& selfCGroups)
        : CpuLocation(ResolveController({"cpu", "cpu.stat", "cpu.stat"}, selfCGroups))
        , CpuAcctLocation(ResolveController({"cpuacct", "cpuacct.stat", "cpu.stat"}, selfCGroups))
    { }

    TCpuStatistics GetStatistics() const
    {
        if (!CpuAcctLocation) {
            return {};
        }

        auto statPath = StatFilePath(CpuAcctLocation->Path, CpuAcctLocation->IsV2 ? "cpu.stat" : "cpuacct.stat");
        auto statistics = ReadAndParseStatFile(statPath);

        if (CpuAcctLocation->IsV2) {
            return TCpuStatistics{
                .UserTime = TDuration::MicroSeconds(statistics["user_usec"]),
                .SystemTime = TDuration::MicroSeconds(statistics["system_usec"]),
            };
        } else {
            static const auto ticksPerSecond = [] () -> i64 {
                #if defined(__linux__)
                    return sysconf(_SC_CLK_TCK);
                #else
                    return 1'000'000'000;
                #endif
            }();

            auto fromJiffies = [&] (i64 jiffies) {
                return TDuration::MicroSeconds(1'000'000 * jiffies / ticksPerSecond);
            };

            return TCpuStatistics{
                .UserTime = fromJiffies(statistics["user"]),
                .SystemTime = fromJiffies(statistics["system"]),
            };
        }
    }

    TCpuThrottlingStatistics GetThrottlingStatistics() const
    {
        if (!CpuLocation) {
            return {};
        }

        auto statistics = ReadAndParseStatFile(StatFilePath(CpuLocation->Path, "cpu.stat"));

        return TCpuThrottlingStatistics{
            .NrPeriods = static_cast<ui64>(statistics["nr_periods"]),
            .NrThrottled = static_cast<ui64>(statistics["nr_throttled"]),
            .ThrottledTime = GetThrottledTime(statistics),
            .WaitTime = GetWaitTime(),
        };
    }

    // NB(pavook): we prefer the h_throttled_* fields (kernel extension) because they are hierarchical,
    // i.e. also account for throttling caused by the limits of ancestor cgroups;
    // on stock kernels we fall back to the standard throttled_* fields of the cgroup itself.
    TDuration GetThrottledTime(const THashMap<std::string, i64>& statistics) const
    {
        if (CpuLocation->IsV2) {
            if (auto* throttled = statistics.FindPtr("h_throttled_usec")) {
                return TDuration::MicroSeconds(*throttled);
            }
            return TDuration::MicroSeconds(statistics.Value("throttled_usec", 0));
        }

        // NB: V1 reports throttled time in nanoseconds.
        if (auto* throttled = statistics.FindPtr("h_throttled_time")) {
            return TDuration::MicroSeconds(*throttled / 1000);
        }
        return TDuration::MicroSeconds(statistics.Value("throttled_time", 0) / 1000);
    }

    // NB(pavook): we prefer cpuacct.wait (kernel extension) because it is an honest sum of individual thread wait times,
    // not the union of those wait times ("pressure", upstream).
    TDuration GetWaitTime() const
    {
        if (!CpuAcctLocation) {
            return TDuration::Zero();
        }

        auto waitPath = StatFilePath(CpuAcctLocation->Path, "cpuacct.wait");
        if (NFs::Exists(waitPath)) {
            try {
                auto ns = FromString<ui64>(TFileInput(waitPath).ReadLine());
                return TDuration::MicroSeconds(ns / 1000);
            } catch (const std::exception&) {
            }
        }

        // The PSI fallback only exists in the v2 unified hierarchy.
        if (!(CpuLocation && CpuLocation->IsV2)) {
            return TDuration::Zero();
        }

        auto pressurePath = StatFilePath(CpuLocation->Path, "cpu.pressure");
        try {
            auto raw = TFileInput(pressurePath).ReadAll();
            for (const auto& line : SplitString(raw, "\n")) {
                if (!line.StartsWith("some ")) {
                    continue;
                }
                for (auto token : StringSplitter(line).Split(' ')) {
                    TStringBuf buf(token);
                    if (buf.SkipPrefix("total=")) {
                        return TDuration::MicroSeconds(FromString<ui64>(buf));
                    }
                }
            }
        } catch (const std::exception&) {
        }

        return TDuration::Zero();
    }

    const std::optional<TCGroupLocation> CpuLocation;
    const std::optional<TCGroupLocation> CpuAcctLocation;
};

////////////////////////////////////////////////////////////////////////////////

class TIOStatisticsFetcher
{
public:
    explicit TIOStatisticsFetcher(const THashMap<std::string, std::string>& selfCGroups)
        : Location(ResolveController({"blkio", "blkio.reset_stats", "io.stat"}, selfCGroups))
    { }

    TIOStatistics GetStatistics() const
    {
        if (!Location) {
            return {};
        }

        if (Location->IsV2) {
            auto ioStatistics = ReadAndParseIOStatFile(StatFilePath(Location->Path, "io.stat"));

            return TIOStatistics{
                .IOReadByte = ioStatistics["rbytes"],
                .IOWriteByte = ioStatistics["wbytes"],
                .IOReadOps = ioStatistics["rios"],
                .IOWriteOps = ioStatistics["wios"],
            };
        } else {
            TIOStatistics statistics;

            for (auto fileName : {"blkio.io_service_bytes_recursive", "blkio.throttle.io_service_bytes_recursive"}) {
                auto filePath = StatFilePath(Location->Path, fileName);
                if (!NFs::Exists(filePath)) {
                    continue;
                }

                auto blkioStatistics = ReadAndParseBlkIOStatFile(filePath);
                statistics.IOReadByte += blkioStatistics["Read"];
                statistics.IOWriteByte += blkioStatistics["Write"];
                break;
            }

            for (auto fileName : {"blkio.io_serviced_recursive", "blkio.throttle.io_serviced_recursive"}) {
                auto filePath = StatFilePath(Location->Path, fileName);
                if (!NFs::Exists(filePath)) {
                    continue;
                }

                auto blkioStatistics = ReadAndParseBlkIOStatFile(filePath);
                statistics.IOReadOps += blkioStatistics["Read"];
                statistics.IOWriteOps += blkioStatistics["Write"];
                break;
            }

            return statistics;
        }
    }

    const std::optional<TCGroupLocation> Location;
};

////////////////////////////////////////////////////////////////////////////////

// NB: only used for determining whether the pids controller is v2 or not.
class TPidsStatisticsFetcher
{
public:
    explicit TPidsStatisticsFetcher(const THashMap<std::string, std::string>& selfCGroups)
        : Location(ResolveController({"pids", "cgroup.procs", "cgroup.procs"}, selfCGroups))
    { }

    const std::optional<TCGroupLocation> Location;
};

////////////////////////////////////////////////////////////////////////////////

// Reads each controller from its own resolved location, in that controller's format.
class TCGroupStatisticsFetcher
    : public ICGroupStatisticsFetcher
{
public:
    explicit TCGroupStatisticsFetcher(const THashMap<std::string, std::string>& selfCGroups)
        : Memory_(selfCGroups)
        , Cpu_(selfCGroups)
        , IO_(selfCGroups)
        , Pids_(selfCGroups)
    {
        THashMap<ECGroupController, std::optional<TCGroupLocation>> resolved;
        resolved[ECGroupController::Memory] = Memory_.Location;
        resolved[ECGroupController::Cpu] = Cpu_.CpuLocation;
        resolved[ECGroupController::CpuAcct] = Cpu_.CpuAcctLocation;
        resolved[ECGroupController::IO] = IO_.Location;
        resolved[ECGroupController::Pids] = Pids_.Location;
        YT_TLOG_INFO("CGroups statistics fetcher initialized")
            .With("Controllers", resolved);
    }

    bool IsControllerV2(ECGroupController controller) const override
    {
        std::optional<TCGroupLocation> location;
        switch (controller) {
            case ECGroupController::Memory:
                location = Memory_.Location;
                break;
            case ECGroupController::Cpu:
                location = Cpu_.CpuLocation;
                break;
            case ECGroupController::CpuAcct:
                location = Cpu_.CpuAcctLocation;
                break;
            case ECGroupController::IO:
                location = IO_.Location;
                break;
            case ECGroupController::Pids:
                location = Pids_.Location;
                break;
        }

        // If the controller couldn't be resolved, fall back to V1.
        return location ? location->IsV2 : false;
    }

    TMemoryStatistics GetMemoryStatistics() const override
    {
        return Memory_.GetStatistics();
    }

    TMemoryLimits GetMemoryLimits() const override
    {
        return Memory_.GetLimits();
    }

    TCpuStatistics GetCpuStatistics() const override
    {
        return Cpu_.GetStatistics();
    }

    TCpuThrottlingStatistics GetCpuThrottlingStatistics() const override
    {
        return Cpu_.GetThrottlingStatistics();
    }

    TIOStatistics GetIOStatistics() const override
    {
        return IO_.GetStatistics();
    }

    i64 GetOomKillCount() const override
    {
        return Memory_.GetOomKillCount();
    }

private:
    const TMemoryStatisticsFetcher Memory_;
    const TCpuStatisticsFetcher Cpu_;
    const TIOStatisticsFetcher IO_;
    const TPidsStatisticsFetcher Pids_;
};

////////////////////////////////////////////////////////////////////////////////

TString ReadProcFile(const TString& path)
{
    try {
        return TFileInput(path).ReadAll();
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Failed to read cgroup statistics")
            .With("Path", path)
            .With(ex);
        return {};
    }
}

std::unique_ptr<ICGroupStatisticsFetcher> CreateFetcher()
{
    auto selfCGroups = ParseProcessCGroups(ReadProcFile("/proc/self/cgroup"));
    return std::make_unique<TCGroupStatisticsFetcher>(selfCGroups);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

const ICGroupStatisticsFetcher* TSelfCGroupsStatisticsFetcher::Get()
{
    static auto impl = CreateFetcher();
    return impl.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroups
