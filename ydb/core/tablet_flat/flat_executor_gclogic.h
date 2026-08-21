#pragma once
#include "defs.h"
#include "flat_sausage_slicer.h"
#include "flat_exec_commit.h"
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_history_cutter.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/util/backoff.h>

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

    explicit operator bool() const { return Valid(); }
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
    // The executor's history cutter judges an entry cuttable when no executor-known
    // blob generation falls in its range, but it is seeded only from the local DB
    // parts (bootlogic ExtractState). A tablet that writes channel blobs outside the
    // executor — ColumnShard's portions go through TBlobManager — has channel
    // contents the criterion cannot see, so cutting there strands those blobs below
    // the surviving history and GroupFor() resolves them to the Max<ui32> sentinel
    // forever. Such tablets run their own drain-gated cutter on their data channels;
    // channels 0 and 1 hold only executor-written blobs and stay with this cutter.
    static bool IsHistoryCuttingSound(const TTabletStorageInfo& info, ui32 channel);

    TExecutorGCLogic(TIntrusiveConstPtr<TTabletStorageInfo>, TAutoPtr<NPageCollection::TSteppedCookieAllocator>);
    void WriteToLog(TLogCommit &logEntry);
    TGCLogEntry SnapshotLog(ui32 step);
    void SnapToLog(NKikimrExecutorFlat::TLogSnapshot &logSnapshot, ui32 step);
    void OnCommitLog(ui32 step, ui32 confirmedOnSend, const TActorContext &ctx);                 // notification about log commit - could send GC to blob storage
    TDuration OnCollectGarbageResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
                                     const TActorContext &ctx, TActorId launcher);               // notification on any garbage collection results
    void OnConfirmSnapshot(ui32 step, const TActorContext &ctx);                                 // notification about snapshot confirmation - will GC blobs in storage
    void ApplyLogEntry(TGCLogEntry &entry);                                                      // apply one log entry, used during recovery and also from WriteToLog
    void ApplyLogSnapshot(TGCLogEntry &snapshot, const  TVector<std::pair<ui32, ui64>> &barriers);
    void HoldBarrier(ui32 step);                                // holds GC on no more than this step for channels specified
    void ReleaseBarrier(ui32 step);
    ui32 GetActiveGcBarrier();
    void FollowersSyncComplete(bool isBoot);
    void SendCollectGarbage(const TActorContext& executor);
    bool HasGarbageBefore(TGCTime snapshotTime);
    void RetryGcRequests(ui32 channel, const TActorContext& ctx);
    void Confirm(const TActorContext &ctx);

    THistoryCutter HistoryCutter;

    // Marks dropped by the sentinel guard since the last drain; the executor moves
    // this into the GcSentinelDroppedMarks cumulative counter on its periodic
    // counters update (open item 7).
    ui64 TakeSentinelDroppedMarks() { return std::exchange(SentinelDroppedMarks, 0); }
    ui64 SentinelDroppedMarks = 0;


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
        enum class ECutHistoryStatus {
            None,
            SentBarrier,
            Cut,
        };

        TMap<TGCTime, TGCBlobDelta> CommittedDelta; // we don't really need per-step map, what we really need is distinction b/w sent and not-yet-sent idsets
        TGCTime CollectSent;
        TGCTime KnownGcBarrier;
        TGCTime CommitedGcBarrier;
        TGCTime MinUncollectedTime;
        ui32 GcCounter;
        ui32 GcWaitFor;
        ECutHistoryStatus CutHistoryStatus = ECutHistoryStatus::None;

        // retry failed GC logic
        ui32 TryCounter;
        TBackoffTimer BackoffTimer;
        bool PendingRetry;
        ui32 FailCount;

        inline TChannelInfo();
        // Returns the number of GC marks dropped by the sentinel guard (group resolves
        // to Max<ui32>() below the first surviving history entry).
        ui64 SendCollectGarbage(TGCTime uncommittedTime, const TTabletStorageInfo *tabletStorageInfo, ui32 channel, ui32 generation, const TActorContext& executor);
        void SendCollectGarbageEntry(const TActorContext &ctx, TVector<TLogoBlobID> &&keep, TVector<TLogoBlobID> &&notKeep, ui64 tabletid, ui32 channel, ui32 bsgroup, ui32 generation, bool hard, std::optional<TGCTime> barrier = std::nullopt);
        bool OnCollectGarbageSuccess();
        void OnCollectGarbageFailure();
        TDuration TryScheduleGcRequestRetries();
        void RetryGcRequests(const TTabletStorageInfo *tabletStorageInfo, ui32 channel, ui32 generation, const TActorContext& ctx);
    };

    ui32 SnapshotStep;
    ui32 PrevSnapshotStep;
    ui32 ConfirmedOnSendStep;
    THashMap<ui32, TChannelInfo> ChannelInfo;
    TMap<TGCTime, TGCLogEntry> UncommittedDeltaLog;
    TSet<TGCTime> HoldBarriersSet;

    bool AllowGarbageCollection;

    THashSet<ui32> ChannelsToCutHistory;

    void ApplyDelta(TGCTime time, TGCBlobDelta &delta);
    static inline void MergeVectors(THolder<TVector<TLogoBlobID>>& destination, const TVector<TLogoBlobID>& source);
    static inline void MergeVectors(TVector<TLogoBlobID>& destination, const TVector<TLogoBlobID>& source);
    static inline TVector<TLogoBlobID>* CreateVector(const TVector<TLogoBlobID>& source);
};

void DeduplicateGCKeepVectors(TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep, ui32 barrierGen, ui32 barrierStep);

}
}
