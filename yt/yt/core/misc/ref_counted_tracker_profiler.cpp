#include "ref_counted_tracker_profiler.h"
#include "ref_counted_tracker.h"
#include "singleton.h"

#include <yt/yt/library/profiling/producer.h>

namespace NYT {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TRefCountedTrackerProfiler
    : public ISensorProducer
{
public:
    TRefCountedTrackerProfiler()
    {
        TProfiler profiler{"/ref_counted_tracker"};
        profiler.AddProducer("/total", MakeStrong(this));
    }

    void CollectSensors(ISensorWriter* writer) override
    {
        auto statistics = TRefCountedTracker::Get()->GetStatistics().TotalStatistics;

        writer->AddCounter("/objects_allocated", statistics.ObjectsAllocated);
        writer->AddCounter("/objects_freed", statistics.ObjectsFreed);
        writer->AddGauge("/objects_alive", statistics.ObjectsAlive);

        writer->AddCounter("/bytes_allocated", statistics.BytesAllocated);
        writer->AddCounter("/bytes_freed", statistics.BytesFreed);
        writer->AddGauge("/bytes_alive", statistics.BytesAlive);
    }
};

void EnableRefCountedTrackerProfiling()
{
    LeakyRefCountedSingleton<TRefCountedTrackerProfiler>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
