#pragma once

#include "flat_comp.h"
#include "flat_stat_part.h"
#include "util_fmt_line.h"

#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/queue.h>

namespace NKikimr {
namespace NTable {
namespace NCompShard {

    // Forward declarations
    struct TTableInfo;
    struct TTablePart;
    struct TTableShard;

    /**
     * Stats used for data estimation
     */
    struct TStats {
        // Size of data we may potentially read during compaction
        ui64 Size = 0;

        // Number of rows we may potentially read during compaction (including any deletion markers)
        ui64 Rows = 0;

        // Estimated number of deletion markers in the [0, Rows] range
        double Drops = 0.0;

        void Reset() {
            Size = 0;
            Rows = 0;
            Drops = 0.0;
        }

        void Init(const TPart* part, const TSlice& slice) {
            TPartDataSizeHelper helper(part);
            Init(part, slice, helper);
        }

        void Init(const TPart* part, const TSlice& slice, TPartDataSizeHelper& helper) {
            Size = helper.CalcSize(slice.BeginRowId(), slice.EndRowId());
            Rows = slice.Rows();
            if (part->Stat.Rows > 0) {
                ui64 partRows = part->Stat.Rows;
                ui64 partDrops = part->Stat.Drops;

                // Assume worst case: every drop has a hidden row that won't go away
                partDrops -= Min(partDrops, part->Stat.HiddenRows);

                if (Rows < partRows) {
                    Drops = double(partDrops) * Rows / partRows;
                } else {
                    Drops = partDrops;
                }
            } else {
                Drops = 0.0;
            }
        }

        TStats& operator+=(const TStats& o) {
            Size += o.Size;
            Rows += o.Rows;
            Drops += o.Drops;
            if (Drops > Rows) {
                Drops = Rows;
            }
            return *this;
        }

        TStats& operator-=(const TStats& o) {
            Size -= o.Size;
            Rows -= o.Rows;
            Drops -= o.Drops;
            if (Rows == 0) {
                Drops = 0.0;
            }
            return *this;
        }

        ui32 DroppedPercent() const {
            ui64 normalRows = Rows > Drops ? ui64(Rows - Drops) : 0;
            if (Drops > 0) {
                if (Drops >= normalRows) {
                    return 100;
                }
                if (normalRows > 0) {
                    return Min(ui32(Drops * 100 / normalRows), ui32(100));
                }
            }
            return 0;
        }
    };

    /**
     * A helper class used to find keys that evenly split slices
     */
    class TSplitStatIterator {
    public:
        TSplitStatIterator(const TKeyCellDefaults& keyDefaults)
            : KeyCellDefaults(keyDefaults)
            , InitQueue(TCmpHeapByFirstKey{KeyCellDefaults})
            , NextQueue(TCmpHeapByNextKey{KeyCellDefaults})
            , StartQueue(TCmpHeapByFirstKey{KeyCellDefaults})
            , StopQueue(TCmpHeapByLastKey{KeyCellDefaults})
        { }

        void AddSlice(const TPart* part, const TSlice& slice, ui64 size) noexcept;

        bool Next() noexcept;

        TCellsRef CurrentKey() const noexcept { return Key; }

        ui64 LeftSize() const noexcept { return LeftSize_; }
        ui64 RightSize() const noexcept { return RightSize_; }
        ui64 LeftRows() const noexcept { return LeftRows_; }
        ui64 RightRows() const noexcept { return RightRows_; }

    private:
        struct TItemState {
            const TPart* const Part;
            const TSlice Slice;
            TPartDataSizeHelper Helper;
            NPage::TIndex::TIter NextPage;
            TVector<TCell> NextKey;

            TRowId LastRowId;
            ui64 LastPageSize = 0;
            ui64 LastPageRows = 0;
            bool IsPartial = false;

            TItemState(const TPart* part, const TSlice& slice)
                : Part(part)
                , Slice(slice)
                , Helper(Part)
                , LastRowId(Slice.BeginRowId())
            { }

            // Make sure this structure is stable in memory
            TItemState(const TItemState&) = delete;
            TItemState(TItemState&&) = delete;

            void InitNextKey() noexcept;
            void InitPageSize() noexcept;
        };

        struct TCmpHeapByFirstKey {
            const TKeyCellDefaults& KeyCellDefaults;

            bool operator()(const TItemState* b, const TItemState* a) const noexcept;
        };

        struct TCmpHeapByNextKey {
            const TKeyCellDefaults& KeyCellDefaults;

            bool operator()(const TItemState* b, const TItemState* a) const noexcept;
        };

        struct TCmpHeapByLastKey {
            const TKeyCellDefaults& KeyCellDefaults;

            bool operator()(const TItemState* b, const TItemState* a) const noexcept;
        };

        template<class TCmp>
        using TItemQueue = TPriorityQueue<TItemState*, TVector<TItemState*>, TCmp>;

    private:
        bool HasStarted(const TItemState* item) const noexcept;
        bool HasStopped(const TItemState* item) const noexcept;

        void InitNextKey(TItemState* item) const noexcept;
        void InitPageSize(TItemState* item) const noexcept;

    private:
        const TKeyCellDefaults& KeyCellDefaults;
        TVector<TCell> Key;
        TDeque<TItemState> Items;
        TItemQueue<TCmpHeapByFirstKey> InitQueue;
        TItemQueue<TCmpHeapByNextKey> NextQueue;
        TItemQueue<TCmpHeapByFirstKey> StartQueue;
        TItemQueue<TCmpHeapByLastKey> StopQueue;
        TVector<TItemState*> ActivationQueue;
        ui64 LeftSize_ = 0;
        ui64 RightSize_ = 0;
        ui64 LeftRows_ = 0;
        ui64 RightRows_ = 0;
    };

    /**
     * A helper class used to find pages that may be reused during compaction
     */
    class TPageReuseBuilder {
    public:
        TPageReuseBuilder(const TKeyCellDefaults& keyDefaults)
            : KeyCellDefaults(keyDefaults)
        { }

        void AddSlice(const TPart* part, const TSlice& slice, bool reusable) noexcept;

    public:
        struct TReusable {
            const TPart* Part;
            TSlice Slice;
        };

        struct TResults {
            TDeque<TReusable> Reusable;
            size_t ExpectedSlices = 0;
        };

        TResults Build() noexcept;

    private:
        // A lightweight version of either left or right slice boundary
        struct TBoundary {
            TCellsRef Key;
            TRowId RowId;
            bool Inclusive;
        };

        struct TLeftBoundary : public TBoundary {
            TRowId BeginRowId() const {
                return RowId + !Inclusive;
            }
        };

        struct TRightBoundary : public TBoundary {
            TRowId EndRowId() const {
                return RowId + Inclusive;
            }
        };

        struct TItemState {
            const TPart* const Part;
            const TSlice Slice;
            NPage::TIndex::TIter NextPage;
            TVector<TCell> NextKey;
            TLeftBoundary First;
            bool Reusable;

            TItemState(const TPart* part, const TSlice& slice, bool reusable)
                : Part(part)
                , Slice(slice)
                , Reusable(reusable)
            { }

            // Make sure this structure is stable in memory
            TItemState(const TItemState&) = delete;
            TItemState(TItemState&&) = delete;

            bool InitNextKey() noexcept;
            TRowId GetNextRowId() const noexcept;
        };

    private:
        const TKeyCellDefaults& KeyCellDefaults;
        TDeque<TItemState> Items;
    };

    enum class EState {
        Free,
        Pending,
        Compacting,
    };

    struct TCompactionTask {
        EState State = EState::Free;
        ui32 Priority = 0;
        ui64 TaskId = 0;
        TInstant SubmissionTimestamp;
        ui64 CompactionId = 0;
    };

    /**
     * This structure is pinned in memory as long as strategy is alive
     */
    struct TTableInfo {
        // Current row scheme of the table, updated on schema changes
        TIntrusiveConstPtr<TRowScheme> RowScheme;

        // Current known shard keys, changed on split/merge
        THashMap<ui64, TSerializedCellVec> SplitKeys;

        // Last used key id (incremented for each new key)
        ui64 LastSplitKey = 0;

        // Last used shard id (incremented for each new shard)
        ui64 LastShardId = 0;

        // A set of undesirable part labels we would like to compact
        THashSet<TLogoBlobID> GarbageParts;
    };

    /**
     * This structure describes everything known about a part within a shard
     */
    struct TTablePart {
        struct TSliceId {
            const TRowId Begin;
            const TRowId End;

            TSliceId(TRowId begin, TRowId end)
                : Begin(begin)
                , End(end)
            { }

            TSliceId(const TSlice& slice)
                : Begin(slice.BeginRowId())
                , End(slice.EndRowId())
            { }

            friend bool operator<(const TSliceId& a, const TSliceId& b) {
                return a.End <= b.Begin;
            }
        };

        struct TItem {
            // A single slice description
            TSlice Slice;

            // Stats for the above slice (upper bound)
            TStats Stats;

            // A stable iterator into the level where slice is currently placed
            TLevels::iterator Level;

            // A stable iterator into the run where slice is currently placed
            TRun::iterator Position;
        };

        // Reference to the part
        TIntrusiveConstPtr<TPart> Part;

        // A sorted run of used slices
        TMap<TSliceId, TItem> Slices;
    };

    /**
     * This structure describes a single shard of the table
     */
    struct TTableShard
        : public TIntrusiveListItem<TTableShard>
    {
        // Unique shard id, primarily used in html and broker task names
        ui64 Id = 0;

        // Unique per-table id of left and right boundary keys
        // The special value 0 is used as a marker for +/- infinity
        ui64 LeftKey = 0;
        ui64 RightKey = 0;

        // Full map of part slices that are fully inside this shard
        THashMap<TLogoBlobID, TTablePart> Parts;

        // Full stats of the shard, this includes every active part
        TStats Stats;

        // Number of split operations that are currently blocking this shard
        size_t SplitBlocks = 0;

        // Non-null when dynamic levels are fully formed
        // TODO: may be better if this is not recomputed every time
        THolder<TLevels> Levels;

        // Valid when Levels are non-null, cached stats for each level
        TVector<TStats> PerLevelStats;

        // Valid when Levels are non-null, counts number of undesirable bytes
        // on each level, which may be freed after compaction.
        TVector<size_t> PerLevelGarbage;

        // The first level where invariants no longer hold
        size_t FailingLevel = Max<size_t>();

        // Current compaction task
        TCompactionTask Task;

        void RegisterItem(const TTablePart& info, TTablePart::TItem& item, bool isGarbage) noexcept;

        bool FindSplitKey(TSerializedCellVec& foundKey, const TKeyCellDefaults& keyDefaults) const noexcept;
    };

    /**
     * A result of a split of a single slice over a number of shards
     */
    struct TSliceSplitResult {
        TIntrusiveConstPtr<TPart> Part;
        TSlice OldSlice;
        TVector<TSlice> NewSlices;
        TVector<TTableShard*> Shards;
    };

    /**
     * Backend interface for consuming split results
     */
    class ISliceSplitResultConsumer {
    public:
        virtual ~ISliceSplitResultConsumer() = default;

        /**
         * Called when split completes successfully
         */
        virtual void OnSliceSplitResult(TSliceSplitResult result) = 0;
    };

    /**
     * A running split operation of a slice over a number of shards
     *
     * This object is only valid until operation has completed. The intrusive
     * list is used while it's still pending so it may be cancelled.
     */
    class TSliceSplitOp
        : public ICompactionRead
        , public TIntrusiveListItem<TSliceSplitOp>
    {
    public:
        TSliceSplitOp(
                ISliceSplitResultConsumer* consumer,
                const TTableInfo* table,
                TVector<TTableShard*> shards,
                TIntrusiveConstPtr<TPart> part,
                TSlice slice)
            : Consumer(consumer)
            , Table(table)
            , Shards(std::move(shards))
            , Part(std::move(part))
            , Slice(std::move(slice))
        {
            Y_VERIFY(Shards.size() >= 2, "You need at least two shards for a split");
            for (size_t idx = 1; idx < Shards.size(); ++idx) {
                Y_VERIFY(Shards[idx-1]->RightKey == Shards[idx]->LeftKey, "Adjacent shards must have matching keys");
                Y_VERIFY(Shards[idx]->LeftKey != 0, "Adjacent shards must have a valid key");
            }
        }

        bool Execute(IPages* env) override;

        void SetReadId(ui64 readId) {
            Y_VERIFY(ReadId == 0, "Read id must be set exactly once");
            Y_VERIFY(readId != 0, "Attempting to assign an invalid read id");
            ReadId = readId;
        }

        ui64 GetReadId() const {
            Y_VERIFY(ReadId != 0, "Read id has not been set");
            return ReadId;
        }

    public:
        ISliceSplitResultConsumer* const Consumer;
        const TTableInfo* const Table;
        const TVector<TTableShard*> Shards;
        const TIntrusiveConstPtr<TPart> Part;
        const TSlice Slice;

    private:
        ui64 ReadId = 0;
    };

    class TUnderlayMask final : public NPage::IKeySpace {
    public:
        TUnderlayMask(TIntrusiveConstPtr<TRowScheme> rowScheme, TVector<TBounds> bounds)
            : RowScheme(std::move(rowScheme))
            , Bounds(std::move(bounds))
        {
            Y_VERIFY(ValidateOrder(), "TUnderlayMask got bounds in an invalid order");
            Reset();
        }

        const TVector<TBounds>& GetBounds() const {
            return Bounds;
        }

        void Reset() noexcept override {
            Position = Bounds.begin();
        }

        bool HasKey(TCellsRef key) noexcept override;

    public:
        static THolder<TUnderlayMask> Build(
                TIntrusiveConstPtr<TRowScheme> rowScheme,
                TVector<const TBounds*>& input) noexcept;

    private:
        bool ValidateOrder() const noexcept;
        bool ValidatePosition(TCellsRef key) const noexcept;

    private:
        TIntrusiveConstPtr<TRowScheme> RowScheme;
        TVector<TBounds> Bounds;
        TVector<TBounds>::const_iterator Position;
    };

    class TSplitKeys final : public NPage::ISplitKeys {
    public:
        using TKeysVec = TVector<TSerializedCellVec>;

        TSplitKeys(TIntrusiveConstPtr<TRowScheme> rowScheme, TKeysVec keys);
        TSplitKeys(TIntrusiveConstPtr<TRowScheme> rowScheme, TVector<const TBounds*> bounds);

        const TKeysVec& GetKeys() const {
            return Keys;
        }

        void Reset() noexcept override {
            Position = Keys.begin();
        }

        bool ShouldSplit(TCellsRef key) noexcept override;

    private:
        bool ValidateOrder() const noexcept;
        bool ValidatePosition(TCellsRef key) const noexcept;
        bool IsInclusive(TKeysVec::const_iterator pos) const noexcept;

    private:
        TIntrusiveConstPtr<TRowScheme> RowScheme;
        TKeysVec Keys;
        TKeysVec::const_iterator Position;
        TVector<bool> Inclusive;
    };

    class TShardedCompactionParams final : public TCompactionParams {
    public:
        void Describe(IOutputStream& out) const noexcept override;

    public:
        TTableShard* InputShard = nullptr;
        TVector<TPartView> Original;
        TVector<TPartView> Reused;
    };

    class TShardedCompactionStrategy final
        : public ICompactionStrategy
        , public ISliceSplitResultConsumer
    {
    public:
        TShardedCompactionStrategy(
                ui32 table,
                ICompactionBackend* backend,
                IResourceBroker* broker,
                NUtil::ILogger* logger,
                ITimeProvider* time,
                TString taskNameSuffix)
            : Table(table)
            , Backend(backend)
            , Broker(broker)
            , Logger(logger)
            , Time(time)
            , TaskNameSuffix(std::move(taskNameSuffix))
        {
        }

        void Start(TCompactionState state) override;
        void Stop() override;
        void ReflectSchema() override;
        void ReflectRemovedRowVersions() override;
        void UpdateCompactions() override;
        float GetOverloadFactor() override;
        ui64 GetBackingSize() override;
        ui64 GetBackingSize(ui64 ownerTabletId) override;
        ui64 BeginMemCompaction(TTaskId taskId, TSnapEdge edge, ui64 forcedCompactionId) override;
        ui64 GetLastFinishedForcedCompactionId() const override { return 0; } // TODO!
        TInstant GetLastFinishedForcedCompactionTs() const override { return TInstant::Zero(); } // TODO!
        TCompactionChanges CompactionFinished(
                ui64 compactionId,
                THolder<TCompactionParams> params,
                THolder<TCompactionResult> result) override;
        void PartMerged(TPartView part, ui32 level) override;
        void PartMerged(TIntrusiveConstPtr<TColdPart> part, ui32 level) override;
        TCompactionChanges PartsRemoved(TArrayRef<const TLogoBlobID> parts) override;
        TCompactionChanges ApplyChanges() override;
        TCompactionState SnapshotState() override;
        bool AllowForcedCompaction() override;
        void OutputHtml(IOutputStream &out) override;

        void OnSliceSplitResult(TSliceSplitResult result) override;

    private:
        /**
         * Schedules an eventual ApplyChanges call
         */
        void RequestChanges() noexcept;

        /**
         * Check and schedule pending compactions
         */
        void CheckCompactions() noexcept;

        /**
         * Adds parts to current compaction state
         *
         * These parts don't necessarily have to be new parts, but it's
         * important that during replacements old data is removed first.
         */
        void AddParts(TVector<TPartView> parts);

        /**
         * Returns existing or creates a new TTablePart for the specified part
         */
        TTablePart* EnsurePart(TTableShard* shard, TIntrusiveConstPtr<TPart> part) noexcept;

        /**
         * Reinitializes dynamic levels and related structures
         */
        void RebuildLevels(TTableShard* shard) noexcept;

        /**
         * Common helper for cancelling compaction tasks
         */
        void CancelTask(TCompactionTask& task) noexcept;

        /**
         * Checks current shard state and schedules compaction when necessary
         */
        bool CheckShardCompaction(TTableShard* shard, bool schedule = true) noexcept;

        /**
         * Checks table and shcedules forced compaction when necessary
         */
        bool CheckForcedCompaction(bool schedule = true) noexcept;

        /**
         * Called when shard compaction is ready to start
         */
        void BeginShardCompaction(TTableShard* shard, TTaskId taskId) noexcept;

        /**
         * Called when forced compaction is ready to start
         */
        void BeginForcedCompaction(TTaskId taskId) noexcept;

        /**
         * Serializes current state info (header) in a specified string
         */
        void SerializeStateInfo(TString* out) const noexcept;

    private:
        struct TNurseryItem {
            TPartView PartView;
            ui64 DataSize = 0;
        };

        struct TGlobalPart {
            TIntrusiveConstPtr<TPart> Part;
            TIntrusiveConstPtr<TSlices> Slices;
            TVector<TTableShard*> Shards;
            ui64 TotalSize = 0;
            ui64 GarbageSize = 0;
            size_t SplitBlocks = 0;
        };

        TGlobalPart* EnsureGlobalPart(const TIntrusiveConstPtr<TPart>& part, const TIntrusiveConstPtr<TSlices>& slices) noexcept;
        void UpdateGarbageStats(TGlobalPart* allInfo) noexcept;

    private:
        const ui32 Table;
        ICompactionBackend* const Backend;
        IResourceBroker* const Broker;
        NUtil::ILogger* const Logger;
        ITimeProvider* const Time;
        const TString TaskNameSuffix;

        TTableInfo TableInfo;
        TIntrusiveConstPtr<TCompactionPolicy> Policy;
        TIntrusiveListWithAutoDelete<TTableShard, TDelete> Shards;
        TIntrusiveList<TSliceSplitOp> PendingSliceSplits;
        TVector<TSliceSplitResult> SliceSplitResults;
        THashMap<TLogoBlobID, TGlobalPart> AllParts; // does not include nursery
        ui64 AllBackingSize = 0;

        TVector<TIntrusiveConstPtr<TColdPart>> ColdParts;

        ui64 MemCompactionId = 0;
        bool MemCompactionForced = false;

        TDeque<TNurseryItem> Nursery;
        ui64 NurseryDataSize = 0;
        size_t NurseryTaken = 0;

        TCompactionTask ForcedCompactionTask;
        bool ForcedCompactionPending = false;

        bool RequestChangesPending = false;
    };

}
}
}
