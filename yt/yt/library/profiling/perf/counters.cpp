#include "counters.h"

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <linux/perf_event.h>

#include <sys/syscall.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TPerfEventDescription final
{
    int EventType;
    int EventConfig;
};

constexpr TPerfEventDescription SoftwareEvent(int perfName) noexcept
{
    return {PERF_TYPE_SOFTWARE, perfName};
}

constexpr TPerfEventDescription HardwareEvent(int perfName) noexcept
{
    return {PERF_TYPE_HARDWARE, perfName};
}

enum class ECacheEventType
{
    Access,
    Miss,
};

enum class ECacheActionType
{
    Load,
    Store,
};

TPerfEventDescription CacheEvent(
    int perfName,
    ECacheActionType eventType,
    ECacheEventType eventResultType) noexcept
{
    constexpr auto kEventNameShift = 0;
    constexpr auto kCacheActionTypeShift = 8;
    constexpr auto kEventTypeShift = 16;

    int cacheActionTypeForConfig = [&] {
        switch (eventType) {
        case ECacheActionType::Load:
            return PERF_COUNT_HW_CACHE_OP_READ;
        case ECacheActionType::Store:
            return PERF_COUNT_HW_CACHE_OP_WRITE;
        default:
            YT_ABORT();
        }
    }();

    int eventTypeForConfig = [&] {
        switch (eventResultType) {
        case ECacheEventType::Access:
            return PERF_COUNT_HW_CACHE_RESULT_ACCESS;
        case ECacheEventType::Miss:
            return PERF_COUNT_HW_CACHE_RESULT_MISS;
        default:
            YT_ABORT();
        }
    }();

    int eventConfig = (perfName << kEventNameShift) |
        (cacheActionTypeForConfig << kCacheActionTypeShift) |
        (eventTypeForConfig << kEventTypeShift);

    return {PERF_TYPE_HW_CACHE, eventConfig};
}

TPerfEventDescription GetDescription(EPerfEventType type)
{
    switch (type) {
        case EPerfEventType::CpuCycles:
            return HardwareEvent(PERF_COUNT_HW_CPU_CYCLES);
        case EPerfEventType::Instructions:
            return HardwareEvent(PERF_COUNT_HW_INSTRUCTIONS);
        case EPerfEventType::CacheReferences:
            return HardwareEvent(PERF_COUNT_HW_CACHE_REFERENCES);
        case EPerfEventType::CacheMisses:
            return HardwareEvent(PERF_COUNT_HW_CACHE_MISSES);
        case EPerfEventType::BranchInstructions:
            return HardwareEvent(PERF_COUNT_HW_BRANCH_INSTRUCTIONS);
        case EPerfEventType::BranchMisses:
            return HardwareEvent(PERF_COUNT_HW_BRANCH_MISSES);
        case EPerfEventType::BusCycles:
            return HardwareEvent(PERF_COUNT_HW_BUS_CYCLES);
        case EPerfEventType::StalledCyclesFrontend:
            return HardwareEvent(PERF_COUNT_HW_STALLED_CYCLES_FRONTEND);
        case EPerfEventType::StalledCyclesBackend:
            return HardwareEvent(PERF_COUNT_HW_STALLED_CYCLES_BACKEND);
        case EPerfEventType::RefCpuCycles:
            return HardwareEvent(PERF_COUNT_HW_REF_CPU_CYCLES);


        case EPerfEventType::PageFaults:
            return SoftwareEvent(PERF_COUNT_SW_PAGE_FAULTS);
        case EPerfEventType::MinorPageFaults:
            return SoftwareEvent(PERF_COUNT_SW_PAGE_FAULTS_MIN);
        case EPerfEventType::MajorPageFaults:
            return SoftwareEvent(PERF_COUNT_SW_PAGE_FAULTS_MAJ);

        // `cpu-clock` is a bit broken according to this: https://stackoverflow.com/a/56967896
        case EPerfEventType::CpuClock:
            return SoftwareEvent(PERF_COUNT_SW_CPU_CLOCK);
        case EPerfEventType::TaskClock:
            return SoftwareEvent(PERF_COUNT_SW_TASK_CLOCK);
        case EPerfEventType::ContextSwitches:
            return SoftwareEvent(PERF_COUNT_SW_CONTEXT_SWITCHES);
        case EPerfEventType::CpuMigrations:
            return SoftwareEvent(PERF_COUNT_SW_CPU_MIGRATIONS);
        case EPerfEventType::AlignmentFaults:
            return SoftwareEvent(PERF_COUNT_SW_ALIGNMENT_FAULTS);
        case EPerfEventType::EmulationFaults:
            return SoftwareEvent(PERF_COUNT_SW_EMULATION_FAULTS);
        case EPerfEventType::DataTlbReferences:
            return CacheEvent(PERF_COUNT_HW_CACHE_DTLB, ECacheActionType::Load, ECacheEventType::Access);
        case EPerfEventType::DataTlbMisses:
            return CacheEvent(PERF_COUNT_HW_CACHE_DTLB, ECacheActionType::Load, ECacheEventType::Miss);
        case EPerfEventType::DataStoreTlbReferences:
            return CacheEvent(PERF_COUNT_HW_CACHE_DTLB, ECacheActionType::Store, ECacheEventType::Access);
        case EPerfEventType::DataStoreTlbMisses:
            return CacheEvent(PERF_COUNT_HW_CACHE_DTLB, ECacheActionType::Store, ECacheEventType::Miss);

        // Apparently it doesn't make sense to treat these values as relative:
        // https://stackoverflow.com/questions/49933319/how-to-interpret-perf-itlb-loads-itlb-load-misses
        case EPerfEventType::InstructionTlbReferences:
            return CacheEvent(PERF_COUNT_HW_CACHE_ITLB, ECacheActionType::Load, ECacheEventType::Access);
        case EPerfEventType::InstructionTlbMisses:
            return CacheEvent(PERF_COUNT_HW_CACHE_ITLB, ECacheActionType::Load, ECacheEventType::Miss);
        case EPerfEventType::LocalMemoryReferences:
            return CacheEvent(PERF_COUNT_HW_CACHE_NODE, ECacheActionType::Load, ECacheEventType::Access);
        case EPerfEventType::LocalMemoryMisses:
            return CacheEvent(PERF_COUNT_HW_CACHE_NODE, ECacheActionType::Load, ECacheEventType::Miss);

        default:
            YT_ABORT();
    }
}

int OpenPerfEvent(int tid, int eventType, int eventConfig)
{
    perf_event_attr attr{};

    attr.type = eventType;
    attr.size = sizeof(attr);
    attr.config = eventConfig;
    attr.inherit = 1;

    int fd = HandleEintr(syscall, SYS_perf_event_open, &attr, tid, -1, -1, PERF_FLAG_FD_CLOEXEC);
    if (fd == -1) {
        THROW_ERROR_EXCEPTION("Failed to open perf event descriptor")
            << TError::FromSystem()
            << TErrorAttribute("type", eventType)
            << TErrorAttribute("config", eventConfig);
    }
    return fd;
}

ui64 FetchPerfCounter(int fd)
{
    ui64 num{};
    if (HandleEintr(read, fd, &num, sizeof(num)) != sizeof(num)) {
        THROW_ERROR_EXCEPTION("Failed to read perf event counter")
            << TError::FromSystem();
    }
    return num;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPerfEventCounter::TPerfEventCounter(EPerfEventType type)
    : FD_(OpenPerfEvent(
        0,
        GetDescription(type).EventType,
        GetDescription(type).EventConfig))
{ }

TPerfEventCounter::~TPerfEventCounter()
{
    if (FD_ != -1) {
        SafeClose(FD_, false);
    }
}

ui64 TPerfEventCounter::Read()
{
    return FetchPerfCounter(FD_);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCounterOwner)

struct TCounterOwner
    : public TRefCounted
{
    std::vector<std::unique_ptr<TPerfEventCounter>> Counters;
};

DEFINE_REFCOUNTED_TYPE(TCounterOwner)

void EnablePerfCounters()
{
    auto owner = LeakyRefCountedSingleton<TCounterOwner>();

    auto exportCounter = [&] (const TString& category, const TString& name, EPerfEventType type) {
        try {
            owner->Counters.emplace_back(new TPerfEventCounter{type});
            TProfiler{category}.AddFuncCounter(name, owner, [counter=owner->Counters.back().get()] {
                return counter->Read();
            });
        } catch (const std::exception&) {
        }
    };

    exportCounter("/cpu/perf", "/cycles", EPerfEventType::CpuCycles);
    exportCounter("/cpu/perf", "/instructions", EPerfEventType::Instructions);
    exportCounter("/cpu/perf", "/branch_instructions", EPerfEventType::BranchInstructions);
    exportCounter("/cpu/perf", "/branch_misses", EPerfEventType::BranchMisses);
    exportCounter("/cpu/perf", "/context_switches", EPerfEventType::ContextSwitches);

    exportCounter("/memory/perf", "/page_faults", EPerfEventType::PageFaults);
    exportCounter("/memory/perf", "/minor_page_faults", EPerfEventType::MinorPageFaults);
    exportCounter("/memory/perf", "/major_page_faults", EPerfEventType::MajorPageFaults);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
