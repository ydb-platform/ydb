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

// A stalled cut has several indistinguishable causes; these separate them.
class THistoryCutterCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr Nominations;
    NMonitoring::TDynamicCounters::TCounterPtr TriggeredNominations;
    NMonitoring::TDynamicCounters::TCounterPtr SweepsCompleted;
    NMonitoring::TDynamicCounters::TCounterPtr EntriesCut;
    NMonitoring::TDynamicCounters::TCounterPtr SweepCandidates;
    NMonitoring::TDynamicCounters::TCounterPtr ChannelsPoisoned;
    NMonitoring::TDynamicCounters::TCounterPtr EntriesDisproved;
    NMonitoring::TDynamicCounters::TCounterPtr EntriesProven;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingState;
    NMonitoring::TDynamicCounters::TCounterPtr PortionKeysCount;
    NMonitoring::TDynamicCounters::TCounterPtr Tombstones;
    NMonitoring::TDynamicCounters::TCounterPtr Underflows;
    NMonitoring::TDynamicCounters::TCounterPtr AuditComparableZero;
    NMonitoring::TDynamicCounters::TCounterPtr AuditComparableNonzero;
    NMonitoring::TDynamicCounters::TCounterPtr AuditAgreements;
    NMonitoring::TDynamicCounters::TCounterPtr AuditChanged;
    NMonitoring::TDynamicCounters::TCounterPtr AuditUndercounts;
    NMonitoring::TDynamicCounters::TCounterPtr AuditOvercounts;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingsStarted;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingsCompleted;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingsFailed;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingBatches;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingExecuteRetries;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingPortionsTotal;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingBytesCharged;
    NMonitoring::TDynamicCounters::TCounterPtr SeedingDurationMs;

public:
    THistoryCutterCounters(const TCommonCountersOwner& sameAs, const TString& componentName);

    void OnNomination() const {
        Nominations->Add(1);
    }

    void OnTriggeredNomination() const {
        TriggeredNominations->Add(1);
    }

    void OnSweepCompleted() const {
        SweepsCompleted->Add(1);
    }

    void OnEntryCut() const {
        EntriesCut->Add(1);
    }

    // Deltas, not absolute values: tablets share one subgroup, so Set() would be last-tablet-wins.
    void OnLevelsDelta(const i64 sweepCandidates, const i64 channelsPoisoned, const i64 entriesDisproved) const {
        SweepCandidates->Add(sweepCandidates);
        ChannelsPoisoned->Add(channelsPoisoned);
        EntriesDisproved->Add(entriesDisproved);
    }

    // Passed every gate and the final re-check; in measure-only mode this is where the entry stops.
    void OnEntryProven() const {
        EntriesProven->Add(1);
    }

    // Delta-based level sensors for seeding state machine.
    void OnSeedLevelsDelta(const i64 seedingState, const i64 portionKeys, const i64 tombstones) const {
        SeedingState->Add(seedingState);
        PortionKeysCount->Add(portionKeys);
        Tombstones->Add(tombstones);
    }

    void OnUnderflow() const {
        Underflows->Add(1);
    }

    void OnAuditComparableZero() const {
        AuditComparableZero->Add(1);
    }

    void OnAuditComparableNonzero() const {
        AuditComparableNonzero->Add(1);
    }

    void OnAuditAgreement() const {
        AuditAgreements->Add(1);
    }

    void OnAuditChanged() const {
        AuditChanged->Add(1);
    }

    void OnAuditUndercount() const {
        AuditUndercounts->Add(1);
    }

    void OnAuditOvercount() const {
        AuditOvercounts->Add(1);
    }

    void OnSeedingStarted() const {
        SeedingsStarted->Add(1);
    }

    void OnSeedingCompleted() const {
        SeedingsCompleted->Add(1);
    }

    void OnSeedingFailed() const {
        SeedingsFailed->Add(1);
    }

    void OnSeedingBatch(const ui64 portionCount, const ui64 bytesCharged) const {
        SeedingBatches->Add(1);
        SeedingPortionsTotal->Add(portionCount);
        SeedingBytesCharged->Add(bytesCharged);
    }

    void OnSeedingExecuteRetry() const {
        SeedingExecuteRetries->Add(1);
    }

    void OnSeedingDurationMs(const ui64 ms) const {
        SeedingDurationMs->Add(ms);
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
