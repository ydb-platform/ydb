#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <util/datetime/base.h>

#include <util/generic/hash.h>

namespace NYT::NCGroups {

////////////////////////////////////////////////////////////////////////////////

struct TMemoryStatistics
{
    // NB: V1 RSS bundles anon with swap cached; V2 can export those separately,
    // but we keep the V1-style value for common API.
    i64 AnonWithSwapCached = 0;
    i64 TmpfsUsage = 0;
    i64 MappedFile = 0;
    i64 MajorPageFaults = 0;
    i64 Cache = 0;
    i64 RssHuge = 0;
    i64 Dirty = 0;
    i64 Writeback = 0;
};

struct TMemoryLimits
{
    std::optional<i64> MemoryLimit;
    std::optional<i64> AnonymousMemoryLimit;
};

struct TCpuStatistics
{
    TDuration UserTime;
    TDuration SystemTime;
};

struct TCpuThrottlingStatistics
{
    ui64 NrPeriods = 0;
    ui64 NrThrottled = 0;
    TDuration ThrottledTime;
    TDuration WaitTime;
};

struct TIOStatistics
{
    i64 IOReadByte = 0;
    i64 IOWriteByte = 0;

    i64 IOReadOps = 0;
    i64 IOWriteOps = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECGroupController,
    (Memory)
    (Cpu)
    (CpuAcct)
    (IO)
    (Pids)
);

struct ICGroupStatisticsFetcher
{
    virtual ~ICGroupStatisticsFetcher() = default;

    // Whether the given controller uses the cgroup v2 file format for the current
    // process. Returns |false| if the controller could not be resolved.
    virtual bool IsControllerV2(ECGroupController controller) const = 0;

    virtual TMemoryStatistics GetMemoryStatistics() const = 0;
    virtual TMemoryLimits GetMemoryLimits() const = 0;
    virtual TCpuStatistics GetCpuStatistics() const = 0;
    virtual TCpuThrottlingStatistics GetCpuThrottlingStatistics() const = 0;
    virtual TIOStatistics GetIOStatistics() const = 0;
    virtual i64 GetOomKillCount() const = 0;
};

// NB(pavook): cgroup paths are detected once at construction and cached.
// If the process gets migrated to a different cgroup at runtime, the cached
// paths will be stale. This process cgroup migration is not a typical behavior
// from YT's standpoint, and it can't be feasibly supported: there will either be
// a window where the cgroup paths are stale, or the paths would be re-loaded
// on each statistic fetch, which is too costly.
struct TSelfCGroupsStatisticsFetcher
{
    static const ICGroupStatisticsFetcher* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroups
