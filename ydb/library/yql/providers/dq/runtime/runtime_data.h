#pragma once

#include <util/generic/string.h>
#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/system/rusage.h>
#include <util/system/spinlock.h>

#include <atomic>

namespace NYql {

struct TWorkerRuntimeData {
    std::atomic<ui32> Epoch = 0;

    TGUID WorkerId;
    TString ClusterName;

    void AddRusageDelta(const TRusage& delta);

    TDuration GetRusageWindow();
    TRusage GetRusageDelta();
    TRusage GetRusage();

    void UpdateChannelInputDelay(TDuration duration);
    void UpdateChannelOutputDelay(TDuration duration);

    void OnWorkerStart(const TString& operationId);
    void OnWorkerStop(const TString& operationId);

    THashMap<TString,int> GetRunningWorkers() const;

private:
    TAdaptiveLock Lock;
    const int WindowSize = 60;
    TVector<TRusage> Rusages = TVector<TRusage>(WindowSize);
    TInstant LastUpdate;
    std::atomic<bool> RusageFull = false;
    int Index = 0;

    TAdaptiveLock WorkersLock;
    THashMap<TString,int> RunningWorkers;
};

} // namespace NYql
