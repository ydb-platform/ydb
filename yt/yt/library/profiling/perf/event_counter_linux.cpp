#include "event_counter.h"

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <linux/perf_event.h>

#include <sys/syscall.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TPerfEventDescription
{
    ui32 EventType;
    ui64 EventConfig;
};

constexpr TPerfEventDescription SoftwareEvent(ui64 perfName) noexcept
{
    return {PERF_TYPE_SOFTWARE, perfName};
}

constexpr TPerfEventDescription HardwareEvent(ui64 perfName) noexcept
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

    ui64 eventConfig =
        (perfName << kEventNameShift) |
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPerfEventCounter
    : public IPerfEventCounter
{
public:
    explicit TPerfEventCounter(EPerfEventType type)
        : Type_(type)
    {
        const auto& description =  GetDescription(type);

        perf_event_attr attr{};
        attr.type = description.EventType;
        attr.size = sizeof(attr);
        attr.config = description.EventConfig;
        attr.inherit = true;

        FD_ = HandleEintr(
            syscall,
            SYS_perf_event_open,
            &attr,
            /*pid*/ 0,
            /*cpu*/ -1,
            /*group_fd*/ -1,
            PERF_FLAG_FD_CLOEXEC);

        if (FD_ == -1) {
            THROW_ERROR_EXCEPTION("Failed to open %Qlv perf event descriptor",
                type)
                << TError::FromSystem();
        }
    }

    ~TPerfEventCounter()
    {
        if (FD_ != -1) {
            SafeClose(FD_, /*ignoreBadFD*/ false);
        }
    }

    i64 Read() final
    {
        i64 result = 0;
        if (HandleEintr(read, FD_, &result, sizeof(result)) != sizeof(result)) {
            THROW_ERROR_EXCEPTION("Failed to read perf event counter %Qlv",
                Type_)
                << TError::FromSystem();
        }
        return result;
    }

private:
    const EPerfEventType Type_;
    int FD_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IPerfEventCounter> CreatePerfEventCounter(EPerfEventType type)
{
    return std::make_unique<TPerfEventCounter>(type);
}

////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
