#pragma once
#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/common.h>
#include <ydb/core/util/gen_step.h>

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

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

    void OnGCTask(const ui32 keepsCount, const ui32 keepBytes, const ui32 deleteCount, const ui32 deleteBytes, const bool isFull,
        const bool moveBarrier) const;

    void OnEmptyGCTask() const {
        EmptyGCTasks->Add(1);
    }
};

// A stalled cut has several externally indistinguishable causes: GC queues not
// drained, blobs still shared out, an entry in disproval backoff, a poisoned
// channel. These separate them. Aggregate rather than per-channel labels — the
// channel is already in the poison warning.
class THistoryCutterCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr Nominations;
    NMonitoring::TDynamicCounters::TCounterPtr SweepsCompleted;
    NMonitoring::TDynamicCounters::TCounterPtr EntriesCut;
    NMonitoring::TDynamicCounters::TCounterPtr BarriersFailed;
    NMonitoring::TDynamicCounters::TCounterPtr SweepCandidates;
    NMonitoring::TDynamicCounters::TCounterPtr ChannelsPoisoned;
    NMonitoring::TDynamicCounters::TCounterPtr EntriesDisproved;

public:
    THistoryCutterCounters(const TCommonCountersOwner& sameAs, const TString& componentName);

    void OnNomination() const {
        Nominations->Add(1);
    }

    void OnSweepCompleted() const {
        SweepsCompleted->Add(1);
    }

    void OnBarrierResult(const bool ok) const {
        if (ok) {
            EntriesCut->Add(1);
        } else {
            BarriersFailed->Add(1);
        }
    }

    // Deltas, not absolute values: every tablet owns a TBlobsManagerCounters but
    // they share one module_id=BlobsManager subgroup, so Set() would be
    // last-tablet-wins. The caller owns the per-tablet last-published state.
    void OnLevelsDelta(const i64 sweepCandidates, const i64 channelsPoisoned, const i64 entriesDisproved) const {
        SweepCandidates->Add(sweepCandidates);
        ChannelsPoisoned->Add(channelsPoisoned);
        EntriesDisproved->Add(entriesDisproved);
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
    const THistoryCutterCounters HistoryCutterCounters;
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

}   // namespace NKikimr::NColumnShard
