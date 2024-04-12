#pragma once
#include "defs.h"
#include "flat_sausage_slicer.h"
#include "flat_exec_commit.h"
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ranges>

namespace NKikimr {
namespace NTabletFlatExecutor {

struct TGCTime {
    ui32 Generation;
    ui32 Step;

    constexpr inline TGCTime() : TGCTime(0, 0) {}
    constexpr inline TGCTime(ui32 generation, ui32 step) : Generation(generation), Step(step) {}
    inline bool operator ==(const TGCTime& another) const { return Generation == another.Generation && Step == another.Step; }
    inline bool operator <(const TGCTime& another) const { return Generation < another.Generation || (Generation == another.Generation && Step < another.Step); }
    inline bool operator <=(const TGCTime& another) const { return Generation < another.Generation || (Generation == another.Generation && Step <= another.Step); }
    inline bool Valid() const { return Generation != 0 || Step != 0; }
    inline void Clear() { Generation = Step = 0; }
    static TGCTime Infinity() { return TGCTime(std::numeric_limits<ui32>::max(), std::numeric_limits<ui32>::max()); }

    explicit operator bool() const noexcept { return Valid(); }
};

struct TGCLogEntry {
    TGCTime Time;
    TGCBlobDelta Delta;

    TGCLogEntry() {}
    TGCLogEntry(const TGCTime& time) : Time(time) {}
    TGCLogEntry(const TGCTime& time, const TGCBlobDelta& delta) : Time(time), Delta(delta) {}
};

class TExecutorGCLogic {
public:
    TExecutorGCLogic(TIntrusiveConstPtr<TTabletStorageInfo>, TAutoPtr<NPageCollection::TSteppedCookieAllocator>);
    void WriteToLog(TLogCommit &logEntry);
    TGCLogEntry SnapshotLog(ui32 step);
    void SnapToLog(NKikimrExecutorFlat::TLogSnapshot &logSnapshot, ui32 step);
    void OnCommitLog(ui32 step, ui32 confirmedOnSend, const TActorContext &ctx);                 // notification about log commit - could send GC to blob storage
    void OnCollectGarbageResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev);             // notification on any garbage collection results
    void ApplyLogEntry(TGCLogEntry &entry);                                                      // apply one log entry, used during recovery and also from WriteToLog
    void ApplyLogSnapshot(TGCLogEntry &snapshot, const  TVector<std::pair<ui32, ui64>> &barriers);
    void HoldBarrier(ui32 step);                                // holds GC on no more than this step for channels specified
    void ReleaseBarrier(ui32 step);
    ui32 GetActiveGcBarrier();
    void FollowersSyncComplete(bool isBoot);

    auto ListChannels() const {
        auto getKey = [](const std::pair<ui32, TChannelInfo>& p) { return p.first; };
        return ChannelInfo | std::ranges::views::transform(getKey);
    }

    struct TIntrospection {
        ui64 UncommitedEntries;
        ui64 UncommitedBlobIds;
        ui64 UncommitedEntriesBytes;
        ui64 CommitedEntries;
        ui64 CommitedBlobIdsKnown;
        ui64 CommitedBlobIdsLeft;
        ui64 CommitedEntriesBytes;
        ui64 BarriersSetSize;

        TIntrospection()
            : UncommitedEntries(0)
            , UncommitedBlobIds(0)
            , UncommitedEntriesBytes(0)
            , CommitedEntries(0)
            , CommitedBlobIdsKnown(0)
            , CommitedBlobIdsLeft(0)
            , CommitedEntriesBytes(0)
            , BarriersSetSize(0)
        {}
    };

    TIntrospection IntrospectStateSize() const;
protected:
    const TIntrusiveConstPtr<TTabletStorageInfo> TabletStorageInfo;
    const TAutoPtr<NPageCollection::TSteppedCookieAllocator> Cookies;
    const ui32 Generation;
    NPageCollection::TSlicer Slicer;

    struct TChannelInfo {
        TMap<TGCTime, TGCBlobDelta> CommittedDelta; // we don't really need per-step map, what we really need is distinction b/w sent and not-yet-sent idsets
        TGCTime CollectSent;
        TGCTime KnownGcBarrier;
        TGCTime CommitedGcBarrier;
        ui32 GcCounter;
        ui32 GcWaitFor;

        inline TChannelInfo();
        void ApplyDelta(TGCTime time, TGCBlobDelta &delta);
        void SendCollectGarbage(TGCTime uncommittedTime, const TTabletStorageInfo *tabletStorageInfo, ui32 channel, ui32 generation, const TActorContext& executor);
        void SendCollectGarbageEntry(const TActorContext &ctx, TVector<TLogoBlobID> &&keep, TVector<TLogoBlobID> &&notKeep, ui64 tabletid, ui32 channel, ui32 bsgroup, ui32 generation);
        void OnCollectGarbageSuccess();
        void OnCollectGarbageFailure();
    };

    ui32 SnapshotStep;
    ui32 PrevSnapshotStep;
    ui32 ConfirmedOnSendStep;
    THashMap<ui32, TChannelInfo> ChannelInfo;
    TMap<TGCTime, TGCLogEntry> UncommittedDeltaLog;
    TSet<TGCTime> HoldBarriersSet;

    bool AllowGarbageCollection;

    void ApplyDelta(TGCTime time, TGCBlobDelta &delta);
    void SendCollectGarbage(const TActorContext& executor);
    static inline void MergeVectors(THolder<TVector<TLogoBlobID>>& destination, const TVector<TLogoBlobID>& source);
    static inline void MergeVectors(TVector<TLogoBlobID>& destination, const TVector<TLogoBlobID>& source);
    static inline TVector<TLogoBlobID>* CreateVector(const TVector<TLogoBlobID>& source);
};

void DeduplicateGCKeepVectors(TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep, ui32 barrierGen, ui32 barrierStep);

}
}
