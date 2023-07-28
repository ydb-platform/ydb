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

class TPerfEventCounter final
{
public:
    explicit TPerfEventCounter(EPerfEventType type);
    ~TPerfEventCounter();

    TPerfEventCounter(const TPerfEventCounter&) = delete;

    ui64 Read();

private:
    int FD_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

//! EnablePerfCounters creates selected set of perf counters.
void EnablePerfCounters();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
