#include "event_counter_profiler.h"

#include "event_counter.h"

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TPerfEventCounterProfilerImpl
    : public ISensorProducer
{
public:
    TPerfEventCounterProfilerImpl()
    {
        TProfiler("").AddProducer("/perf", this);
    }

private:
    TEnumIndexedArray<EPerfEventType, NThreading::TAtomicObject<std::unique_ptr<IPerfEventCounter>>> TypeToCounter_;

    IPerfEventCounter* GetCounter(EPerfEventType type)
    {
        return TypeToCounter_[type].Transform([&] (auto& counter) {
            if (!counter) {
                counter = CreatePerfEventCounter(type);
            }
            return counter.get();
        });
    }

    void CollectSensors(ISensorWriter* writer) final
    {
        auto writeCounter = [&] (const std::string& name, EPerfEventType type) {
            try {
                auto value = GetCounter(type)->Read();
                writer->AddCounter(name, value);
            } catch (...) {
            }
        };

        writeCounter("/cpu/cycles", EPerfEventType::CpuCycles);
        writeCounter("/cpu/instructions", EPerfEventType::Instructions);
        writeCounter("/cpu/branch_instructions", EPerfEventType::BranchInstructions);
        writeCounter("/cpu/branch_misses", EPerfEventType::BranchMisses);
        writeCounter("/cpu/context_switches", EPerfEventType::ContextSwitches);
        writeCounter("/memory/page_faults", EPerfEventType::PageFaults);
        writeCounter("/memory/minor_page_faults", EPerfEventType::MinorPageFaults);
        writeCounter("/memory/major_page_faults", EPerfEventType::MajorPageFaults);
    }
};

////////////////////////////////////////////////////////////////////////////////

void EnablePerfEventCounterProfiling()
{
    Y_UNUSED(LeakyRefCountedSingleton<TPerfEventCounterProfilerImpl>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
