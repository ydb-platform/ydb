#include "memory_tracker.h"

#include <util/generic/xrange.h>

namespace NActors {
namespace NMemory {

namespace NPrivate {

TMemoryTracker* TMemoryTracker::Instance() {
    return SingletonWithPriority<TMemoryTracker, 0>();
}

void TMemoryTracker::Initialize() {
    GlobalMetrics.resize(Indices.size());
}

const std::map<TString, size_t>& TMemoryTracker::GetMetricIndices() const {
    return Indices;
}

const std::unordered_set<size_t>& TMemoryTracker::GetSensors() const {
    return Sensors;
}

TString TMemoryTracker::GetName(size_t index) const {
    return Names[index];
}

size_t TMemoryTracker::GetCount() const {
    return Indices.size();
}

void TMemoryTracker::GatherMetrics(std::vector<TMetric>& metrics) const {
    metrics.resize(0);
    auto count = GetCount();

    if (!count || GlobalMetrics.size() != count) {
        return;
    }

    TReadGuard guard(LockThreadInfo);

    metrics.resize(count);
    for (size_t i : xrange(count)) {
        metrics[i] += GlobalMetrics[i];
    }

    for (auto info : ThreadInfo) {
        auto& localMetrics = info->GetMetrics();
        if (localMetrics.size() == count) {
            for (size_t i : xrange(count)) {
                metrics[i] += localMetrics[i];
            }
        }
    }
}

size_t TMemoryTracker::RegisterStaticMemoryLabel(const char* name, bool hasSensor) {
    size_t index = 0;
    auto found = Indices.find(name);
    if (found == Indices.end()) {
        TString str(name);
        auto next = Names.size();
        Indices.emplace(str, next);
        Names.push_back(str);
        index = next;
    } else {
        index = found->second;
    }

    if (hasSensor) {
        Sensors.emplace(index);
    }
    return index;
}

void TMemoryTracker::OnCreateThread(TThreadLocalInfo* info) {
    TWriteGuard guard(LockThreadInfo);
    ThreadInfo.insert(info);
}

void TMemoryTracker::OnDestroyThread(TThreadLocalInfo* info) {
    TWriteGuard guard(LockThreadInfo);

    auto count = GetCount();
    if (count && GlobalMetrics.size() == count) {
        const auto& localMetrics = info->GetMetrics();
        if (localMetrics.size() == count) {
            for (size_t i : xrange(count)) {
                GlobalMetrics[i] += localMetrics[i];
            }
        }
    }

    ThreadInfo.erase(info);
}

}

}
}

