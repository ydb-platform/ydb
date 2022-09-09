#pragma once

#include "memory_track.h"

#include <map>
#include <unordered_map>
#include <unordered_set>

#include <util/system/rwlock.h>

namespace NActors {
namespace NMemory {

namespace NPrivate {

class TMemoryTracker {
public:
    static TMemoryTracker* Instance();

    void Initialize();

    const std::map<TString, size_t>& GetMetricIndices() const;
    const std::unordered_set<size_t>& GetSensors() const;
    TString GetName(size_t index) const;
    size_t GetCount() const;

    void GatherMetrics(std::vector<TMetric>& metrics) const;

private:
    size_t RegisterStaticMemoryLabel(const char* name, bool hasSensor);

    void OnCreateThread(TThreadLocalInfo* info);
    void OnDestroyThread(TThreadLocalInfo* info);

private:
    std::map<TString, size_t> Indices;
    std::vector<TString> Names;

    std::vector<TMetric> GlobalMetrics;

    std::unordered_set<size_t> Sensors;

    std::unordered_set<TThreadLocalInfo*> ThreadInfo;
    TRWMutex LockThreadInfo;

    friend class TThreadLocalInfo;
    friend class TBaseLabel;
};

}

}
}
