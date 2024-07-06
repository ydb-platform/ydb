#pragma once
#include "common/owner.h"

#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/common.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/util/gen_step.h>

namespace NKikimr::NOlap {
class TTabletsByBlob;
}

namespace NKikimr::NColumnShard {

class TBlobsManagerGCCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::THistogramPtr KeepsCountBytes;
    NMonitoring::THistogramPtr KeepsCountBlobs;
    NMonitoring::THistogramPtr KeepsCountTasks;
    NMonitoring::THistogramPtr DeletesCountBytes;
    NMonitoring::THistogramPtr DeletesCountBlobs;
    NMonitoring::THistogramPtr DeletesCountTasks;
    NMonitoring::TDynamicCounters::TCounterPtr FullGCTasks;
    NMonitoring::TDynamicCounters::TCounterPtr MoveBarriers;
    NMonitoring::TDynamicCounters::TCounterPtr DontMoveBarriers;
    NMonitoring::TDynamicCounters::TCounterPtr GCTasks;
    NMonitoring::TDynamicCounters::TCounterPtr EmptyGCTasks;
public:
    const NMonitoring::TDynamicCounters::TCounterPtr SkipCollectionEmpty;
    const NMonitoring::TDynamicCounters::TCounterPtr SkipCollectionThrottling;

    TBlobsManagerGCCounters(const TCommonCountersOwner& sameAs, const TString& componentName);

    void OnGCTask(const ui32 keepsCount, const ui32 keepBytes, const ui32 deleteCount, const ui32 deleteBytes,
        const bool isFull, const bool moveBarrier) const;

    void OnEmptyGCTask() const {
        EmptyGCTasks->Add(1);
    }
};

class TBlobsManagerCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    const NMonitoring::TDynamicCounters::TCounterPtr BlobsToDeleteCount;
    const NMonitoring::TDynamicCounters::TCounterPtr BlobsToDeleteDelayedCount;
    const NMonitoring::TDynamicCounters::TCounterPtr BlobsToKeepCount;
public:
    const NMonitoring::TDynamicCounters::TCounterPtr CurrentGen;
    const NMonitoring::TDynamicCounters::TCounterPtr CurrentStep;
    const TBlobsManagerGCCounters GCCounters;
    TBlobsManagerCounters(const TString& module);
    void OnBlobsToDelete(const NOlap::TTabletsByBlob& blobs) const {
        BlobsToDeleteCount->Set(blobs.GetSize());
    }
    void OnBlobsToKeep(const NOlap::TBlobsByGenStep& blobs) const {
        BlobsToKeepCount->Set(blobs.GetSize());
    }
    void OnBlobsToDeleteDelayed(const NOlap::TTabletsByBlob& blobs) const {
        BlobsToDeleteDelayedCount->Set(blobs.GetSize());
    }
};

}
