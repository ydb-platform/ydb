#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPerfEventType,
    (CpuCycles)
    (Instructions)
    (CacheReferences)
    (CacheMisses)
    (BranchInstructions)
    (BranchMisses)
    (BusCycles)
    (StalledCyclesFrontend)
    (StalledCyclesBackend)
    (RefCpuCycles)
    (CpuClock)
    (TaskClock)
    (ContextSwitches)
    (PageFaults)
    (MinorPageFaults)
    (MajorPageFaults)
    (CpuMigrations)
    (AlignmentFaults)
    (EmulationFaults)
    (DataTlbReferences)
    (DataTlbMisses)
    (DataStoreTlbReferences)
    (DataStoreTlbMisses)
    (InstructionTlbReferences)
    (InstructionTlbMisses)
    (LocalMemoryReferences)
    (LocalMemoryMisses)
);

////////////////////////////////////////////////////////////////////////////////

struct IPerfEventCounter
{
    virtual ~IPerfEventCounter() = default;

    //! Returns the counter increment since the last read.
    virtual i64 Read() = 0;
};

std::unique_ptr<IPerfEventCounter> CreatePerfEventCounter(EPerfEventType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
