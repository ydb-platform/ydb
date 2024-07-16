#pragma once

#include "flat_broker.h"
#include "flat_page_conf.h"
#include "flat_part_iface.h"
#include "flat_row_versions.h"
#include "flat_table_subset.h"
#include "flat_dbase_scheme.h"
#include <ydb/core/base/memory_controller_iface.h>

namespace NKikimr {
namespace NTable {

    /**
     * Allows performing reads in the context of a compaction strategy.
     */
    class ICompactionRead {
    public:
        virtual ~ICompactionRead() = default;

        /**
         * Execute is called with the environment that may be used to load
         * pages from currently valid parts. Method may return false indicating
         * it needs to wait for some missing pages, in which case it will
         * be restarted at a later time. Method must return true when there
         * is actually nothing for the backend to load.
         */
        virtual bool Execute(IPages* env) = 0;
    };

    /**
     * Compaction params specify which parts of the table to compact
     */
    class TCompactionParams {
    public:
        TCompactionParams() = default;

        virtual ~TCompactionParams() = default;

        // Used to log compaction parameters
        virtual void Describe(IOutputStream& out) const noexcept;

    public:
        ui32 Table = Max<ui32>();

        // Broker task id used for compaction. Once BeginCompaction is called
        // task ownership is moved to the backend, which will call FinishTask
        // as appropriate. It is possible to leave this as zero, in which
        // case FinishTask will not be called.
        TTaskId TaskId = 0;

        // In-memory snapshot edge up to which to compact
        TSnapEdge Edge;

        // Parts (or their subparts) which are selected for compaction
        TVector<TPartView> Parts;

        // Cold parts which are selected for compaction
        TVector<TIntrusiveConstPtr<TColdPart>> ColdParts;

        // New data will be added to cache if this flag is true
        bool KeepInCache = false;

        // When true all erase markers are removed
        bool IsFinal = false;

        // Underlay mask with possible keys under compacted data
        THolder<NPage::IKeySpace> UnderlayMask;

        // Compacted slices will not cross these split keys
        THolder<NPage::ISplitKeys> SplitKeys;
    };

    /**
     * Compaction result returned on successful compaction
     */
    struct TCompactionResult {
        // Epoch of generated parts
        TEpoch Epoch;

        // Parts that replaced original data
        TVector<TPartView> Parts;

        TCompactionResult(TEpoch epoch, TVector<TPartView> parts)
            : Epoch(epoch)
            , Parts(std::move(parts))
        { }

        TCompactionResult(TEpoch epoch, size_t parts)
            : Epoch(epoch)
        {
            Parts.reserve(parts);
        }
    };

    /**
     * Instructs backend to replace a set of slices
     *
     * This cannot be used to add or remove rows, new slices must address rows
     * that already exist in the database. As a result this may only be used
     * to split or merge slices.
     */
    struct TChangeSlices {
        // Label of an existing part
        TLogoBlobID Label;

        // A new set of slices that will replace existing slices
        TVector<TSlice> NewSlices;
    };

    /**
     * Changes that backend has to apply to reflect updated compaction state
     *
     * These changes are always local to the table that the strategy is
     * working on.
     */
    struct TCompactionChanges {
        // Legacy level where just compacted parts are placed
        ui32 NewPartsLevel = 255;

        // State changes to save in the log (binary data unique to strategy)
        // The data format is opaque as far as backend is concerned and will
        // be serialized as is.
        // Empty values are treated as deletion markers for corresponding keys.
        THashMap<ui64, TString> StateChanges;

        // Slice changes to apply to the database
        TVector<TChangeSlices> SliceChanges;
    };

    /**
     * Generic state for compactions
     */
    struct TCompactionState {
        // Legacy part level for each known part
        // It is ok to leave this empty, missing labels will get level 255.
        // This map is implicitly cleared on every strategy change.
        THashMap<TLogoBlobID, ui32> PartLevels;

        // State snapshot to save in the log (binary data unique to strategy)
        // The data format is opaque as far as backend is concerned and will
        // be serialized as is.
        // Strategy may use this to snapshot and restore its internal state
        // across reboots. Bear in mind this map becomes empty on every
        // strategy change.
        THashMap<ui64, TString> StateSnapshot;
    };

    /**
     * Backend interface for compaction strategy implementations
     */
    class ICompactionBackend {
    public:
        virtual ~ICompactionBackend() = default;

        /**
         * Returns tablet id of the backend owner
         */
        virtual ui64 OwnerTabletId() const = 0;

        /**
         * Returns schema of the entire database
         */
        virtual const TScheme& DatabaseScheme() = 0;

        /**
         * Returns row schema of the specified table
         */
        virtual TIntrusiveConstPtr<TRowScheme> RowScheme(ui32 table) = 0;

        /**
         * Returns schema of the specified table
         */
        virtual const TScheme::TTableInfo* TableScheme(ui32 table) = 0;

        /**
         * Returns size of in-memory data before epoch
         */
        virtual ui64 TableMemSize(ui32 table, TEpoch epoch = TEpoch::Max()) = 0;

        /**
         * Returns current part with the specified label
         */
        virtual TPartView TablePart(ui32 table, const TLogoBlobID& label) = 0;

        /**
         * Returns a complete list of parts from the specified table
         */
        virtual TVector<TPartView> TableParts(ui32 table) = 0;
        virtual TVector<TIntrusiveConstPtr<TColdPart>> TableColdParts(ui32 table) = 0;

        /**
         * Returns currently removed row versions
         */
        virtual const TRowVersionRanges& TableRemovedRowVersions(ui32 table) = 0;

        /**
         * Begins compaction with the specified params
         */
        virtual ui64 BeginCompaction(THolder<TCompactionParams> params) = 0;

        /**
         * Cancels compaction previously started with BeginCompaction
         */
        virtual bool CancelCompaction(ui64 compactionId) = 0;

        /**
         * Request backend to perform some page reads
         *
         * Note that the first Execute call may be performed before the return
         * from BeginRead. The returned id may be used for cancelling a pending
         * read, where a value of 0 is used to indicate the read has finished
         * before the return from BeginRead.
         */
        virtual ui64 BeginRead(THolder<ICompactionRead> read) = 0;

        /**
         * Cancels a previously started BeginRead call
         */
        virtual bool CancelRead(ui64 readId) = 0;

        /**
         * Requests backend to call ApplyChanges for the specified table
         *
         * Backend may schedule and call ApplyChanges at any later time, even
         * after the strategy has been stopped and recreated.
         */
        virtual void RequestChanges(ui32 table) = 0;
    };

    /**
     * Compaction strategies must implement this interface
     */
    class ICompactionStrategy {
    public:
        virtual ~ICompactionStrategy() = default;

        /**
         * Start is called when backend is ready to accept compaction requests.
         *
         * Strategy should initialize its internal state from the provided state.
         */
        virtual void Start(TCompactionState state) = 0;

        /**
         * Stop is called before compaction strategy is destroyed.
         *
         * Strategy must stop all current operations immediately.
         */
        virtual void Stop() = 0;

        /**
         * Called after the table schema may have changed
         */
        virtual void ReflectSchema() = 0;

        /**
         * Called after the table changes its removed row versions
         *
         * Strategy should use this opportunity to reclaculate its garbage
         * estimation and schedule compactions to free up space.
         */
        virtual void ReflectRemovedRowVersions() = 0;

        /**
         * Called periodically so strategy has a chance to re-evaluate its
         * current situation (e.g. update pending task priorities).
         */
        virtual void UpdateCompactions() = 0;

        /**
         * Overload factor must be in the [0, 1] range, where 0 means writes
         * are unrestricted and 1 means new writes must no longer be accepted
         */
        float GetOverloadFactor() { return OverloadFactor; }

        /**
         * Backing size returns total size of all currently active parts
         */
        virtual ui64 GetBackingSize() = 0;

        /**
         * Backing size of all currently active parts owned by this tablet id
         */
        virtual ui64 GetBackingSize(ui64 ownerTabletId) = 0;

        /**
         * Called when backend needs to compact in-memory data
         *
         * Strategy is required to start compaction with the specified task id
         * and snapshot edge, then return the resulting compaction id. Strategy
         * may choose to compact some parts in addition to in-memory data, as
         * long as consistency guarantees are not broken.
         *
         * When forcedCompactionId is non-zero it indicates the compaction was forced by the
         * client and strategy is expected to eventually recompact everything
         * up to the specified snapshot edge. forcedCompactionId must be
         * monotonically growing.
         *
         * Strategy is responsible to manage lifecycle of this compaction,
         * however backend may use the returned compaction id to detect when
         * this compaction has finished.
         */
        virtual ui64 BeginMemCompaction(
            TTaskId taskId,
            TSnapEdge edge,
            ui64 forcedCompactionId) = 0;

        /**
         * Called when backend needs to compact borrowed data
         */
        virtual bool ScheduleBorrowedCompaction() {
            return false;
        }

        /**
         * Called to signal it's ok to compact borrowed garbage
         * even if there's no private tablet data
         */
        virtual void AllowBorrowedGarbageCompaction() {
            // nothing
        }

        virtual ui64 GetLastFinishedForcedCompactionId() const = 0;

        virtual TInstant GetLastFinishedForcedCompactionTs() const = 0;

        /**
         * Called after BeginCompaction completes successfully and replaces
         * the initial subset with compacted results.
         */
        virtual TCompactionChanges CompactionFinished(
            ui64 compactionId,
            THolder<TCompactionParams> params,
            THolder<TCompactionResult> result) = 0;

        /**
         * Called after an external (borrowed) part is merged into the table
         *
         * A different subset of this part may have already been attached to
         * the table, in which case strategy may need to correctly merge new
         * subparts with its existing subparts.
         */
        virtual void PartMerged(TPartView part, ui32 level) = 0;
        virtual void PartMerged(TIntrusiveConstPtr<TColdPart> part, ui32 level) = 0;

        /**
         * Called after some parts have been removed from the table
         *
         * Usually this happens because these parts have been moved to
         * another table. Strategy must make sure any active compactions
         * involving these parts are cancelled and not referenced anymore.
         */
        virtual TCompactionChanges PartsRemoved(TArrayRef<const TLogoBlobID> parts) = 0;

        /**
         * Called some time after RequestChanges and allows the strategy to
         * perform and commit changes to its internal state.
         */
        virtual TCompactionChanges ApplyChanges() = 0;

        /**
         * Called to snapshot compaction state to log
         */
        virtual TCompactionState SnapshotState() = 0;

        /**
         * Called to check whether a new force compaction is allowed
         *
         * As long as strategy returns false the force compaction button will
         * not be shown in the backend UI. However strategy must be ready for
         * forced compaction starting due to other reasons.
         */
        virtual bool AllowForcedCompaction() = 0;

        /**
         * Called to render current state as html
         */
        virtual void OutputHtml(IOutputStream& out) = 0;
    protected:
        float OverloadFactor = 0.0;
    };

    class IMemTableMemoryConsumersCollection {
    public:
        virtual void Register(ui32 table) = 0;
        virtual void Unregister(ui32 table) = 0;
        virtual void CompactionComplete(TIntrusivePtr<NMemory::IMemTableMemoryConsumer> consumer) = 0;
        virtual ~IMemTableMemoryConsumersCollection() = default;
    };

}
}
