#include "memory_track.h"
#include "memory_tracker.h"

namespace NActors {
namespace NMemory {

namespace NPrivate {

TThreadLocalInfo::TThreadLocalInfo()
    : Metrics(TMemoryTracker::Instance()->GetCount())
{
    TMemoryTracker::Instance()->OnCreateThread(this);
}

TThreadLocalInfo::~TThreadLocalInfo() {
    TMemoryTracker::Instance()->OnDestroyThread(this);
}

TMetric* TThreadLocalInfo::GetMetric(size_t index) {
    if (Y_UNLIKELY(index >= Metrics.size())) {
        return &Null;
    }
    return &Metrics[index];
}

const std::vector<TMetric>& TThreadLocalInfo::GetMetrics() const {
    return Metrics;
}

size_t TBaseLabel::RegisterStaticMemoryLabel(const char* name, bool hasSensor) {
    return TMemoryTracker::Instance()->RegisterStaticMemoryLabel(name, hasSensor);
}

}

}
}

