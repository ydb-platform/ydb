#pragma once

#include "defs.h"
#include "fresh_appendix.h"
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullsatisfactionrank.h>

#include <ydb/core/blobstorage/base/ptr.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////
    // TFreshIndex forward declaration
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndex;


    /////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndData forward declaration
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndData;

    /////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndDataSnapshot
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndDataSnapshot {
    public:
        using TFreshIndexAndData = ::NKikimr::TFreshIndexAndData<TKey, TMemRec>;
        TFreshIndexAndDataSnapshot(const TIntrusivePtr<TFreshIndexAndData> &indexAndData)
            : IndexAndData(indexAndData)
            , SnapLsn(indexAndData->GetLastLsn())
        {}
        // for non-existen fresh segment
        TFreshIndexAndDataSnapshot() = default;
        TFreshIndexAndDataSnapshot(const TFreshIndexAndDataSnapshot &) = default;
        TFreshIndexAndDataSnapshot(TFreshIndexAndDataSnapshot &&) = default;
        TFreshIndexAndDataSnapshot &operator=(const TFreshIndexAndDataSnapshot &) = default;
        TFreshIndexAndDataSnapshot &operator=(TFreshIndexAndDataSnapshot &&) = default;
        ~TFreshIndexAndDataSnapshot() = default;

        class TForwardIterator;
        class TBackwardIterator;

        void Destroy() {
            IndexAndData.Drop();
        }

    private:
        TIntrusivePtr<TFreshIndexAndData> IndexAndData;
        ui64 SnapLsn = 0;
    };

    /////////////////////////////////////////////////////////////////////////
    // TFreshIndexAndData
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshIndexAndData : public TAtomicRefCountWithDeleterInBatchPool<TFreshIndexAndData<TKey, TMemRec>> {
    public:
        using TBase = TAtomicRefCountWithDeleterInBatchPool<TFreshIndexAndData<TKey, TMemRec>>;
        using TFreshIndex = ::NKikimr::TFreshIndex<TKey, TMemRec>;

        TFreshIndexAndData(THullCtxPtr hullCtx, std::shared_ptr<TRopeArena> arena);

        ui64 InPlaceSizeApproximation() const {
            ui64 index = (sizeof(TKey) + sizeof(TMemRec)) * Inserts + sizeof(TIdxDiskPlaceHolder);
            return index + MemDataSize;
        }

        ui64 HugeSizeApproximation() const {
            return HugeDataSize;
        }

        ui64 GetFirstLsn() const { return FirstLsn; }
        ui64 GetLastLsn() const { return LastLsn; }
        bool Empty() const { return Inserts == 0; }
        ui64 ElementsInserted() const { return Inserts; }
        ui64 GetMemDataSize() const { return MemDataSize; }
        ui64 GetHugeDataSize() const { return HugeDataSize; }

        // put newly received item into fresh segment
        void PutLogoBlobWithData(ui64 lsn, const TKey &key, ui8 partId, const TIngress &ingress, TRope buffer);
        const TRope& GetLogoBlobData(const TMemPart& memPart) const;
        void Put(ui64 lsn, const TKey &key, const TMemRec &memRec);
        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
        void GetHugeBlobs(TSet<TDiskPart> &hugeBlobs) const;

        class TBaseIterator;
        class TForwardIterator;
        class TBackwardIterator;

        // FIXME: implement:
        // 0. Implement TRawIterator
        // 1. AddAppendix                                           DONE
        // 2. Compactioin/ApplyCompactionResult for AppendixTree    DONE
        // 3. Handle compaction race condition                      DONE
        // 4. Iterators                                             DONE
        // 5. HtmlOutput                                            DONE
        // 6. Memory cosumption                                     DONE
        // 7. Count inserts correctly                               DONE
        // 8. FreshSegment compaction triggers
        // 9. Check performance test (stored data < inserted)

    private:
        THullCtxPtr HullCtx;
        std::unique_ptr<TFreshIndex> Index;

        static constexpr size_t RopeExtentSize = 4096;
        using TRopeExtent = std::array<TRope, RopeExtentSize>;

        // Track FreshData memory consumption
        TMemoryConsumerWithDropOnDestroy FreshDataMemConsumer;
        // arena for small-bounded data allocations
        std::shared_ptr<TRopeArena> Arena;
        // a list of extents; only the last one is being filled
        TList<TRopeExtent> RopeExtents;
        size_t LastRopeExtentSize = RopeExtentSize;

        ui64 FirstLsn = ui64(-1);
        ui64 LastLsn = 0;
        ui64 MemDataSize = 0;
        ui64 HugeDataSize = 0;
        ui64 Inserts = 0;

        void PutPrepared(ui64 lsn, const TKey &key, const TMemRec &memRec);
    };

    /////////////////////////////////////////////////////////////////////////
    // TFreshSegment forward declaration
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshSegment;

    /////////////////////////////////////////////////////////////////////////
    // TFreshSegmentSnapshot
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshSegmentSnapshot {
    public:
        using TFreshIndexAndDataSnapshot = ::NKikimr::TFreshIndexAndDataSnapshot<TKey, TMemRec>;
        using TFreshAppendixTreeSnap = ::NKikimr::TFreshAppendixTreeSnap<TKey, TMemRec>;
        class TForwardIterator;
        class TBackwardIterator;
        class TIteratorWOMerge;
        friend class TFreshSegment<TKey, TMemRec>;

        // empty snapshot for non-existing fresh segment
        TFreshSegmentSnapshot() = default;
        TFreshSegmentSnapshot(const TFreshSegmentSnapshot &) = default;
        TFreshSegmentSnapshot(TFreshSegmentSnapshot &&) = default;
        TFreshSegmentSnapshot &operator=(const TFreshSegmentSnapshot &) = default;
        TFreshSegmentSnapshot &operator=(TFreshSegmentSnapshot &&) = default;
        ~TFreshSegmentSnapshot() = default;

        void Destroy() {
            IndexAndDataSnap.Destroy();
            AppendixTreeSnap.Destroy();
        }

    private:
        TFreshSegmentSnapshot(
                TFreshIndexAndDataSnapshot &&indexAndDataSnap,
                TFreshAppendixTreeSnap &&appendixTreeSnap)
            : IndexAndDataSnap(std::move(indexAndDataSnap))
            , AppendixTreeSnap(std::move(appendixTreeSnap))
        {}

        TFreshIndexAndDataSnapshot IndexAndDataSnap;
        TFreshAppendixTreeSnap AppendixTreeSnap;
    };

    /////////////////////////////////////////////////////////////////////////
    // TFreshSegment
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshSegment : public TThrRefBase {
    public:
        using TThis = ::NKikimr::TFreshSegment<TKey, TMemRec>;
        using TFreshIndexAndData = ::NKikimr::TFreshIndexAndData<TKey, TMemRec>;
        using TFreshIndexAndDataSnapshot = ::NKikimr::TFreshIndexAndDataSnapshot<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TFreshAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;

        static constexpr size_t AppendixTreeStagingCapacity = 4;
        static constexpr size_t HugeBlobsCompactionThreshold = 8;

        // Compact fresh appendix job
        struct TCompactionJob {
            TIntrusivePtr<TThis> FreshSegment;
            std::shared_ptr<ISTreeCompaction> Job;

            TCompactionJob() = default;
            TCompactionJob(TIntrusivePtr<TThis> &&seg, std::shared_ptr<ISTreeCompaction> &&job)
                : FreshSegment(std::move(seg))
                , Job(std::move(job))
            {}

            bool Empty() const { return !FreshSegment; }
            void Work() { Job->Work(); }
            TCompactionJob ApplyCompactionResult() {
                TCompactionJob newJob = FreshSegment->ApplyCompactionResult(Job);
                FreshSegment.Drop();
                Job.reset();
                return newJob;
            }
        };

        TFreshSegment(THullCtxPtr hullCtx, ui64 compThreshold, TInstant startTime, std::shared_ptr<TRopeArena> arena)
            : CompThreshold(Max<ui64>(hullCtx->ChunkSize, compThreshold))
            , ChunkSize(hullCtx->ChunkSize)
            , StartTime(startTime)
            , IndexAndData(MakeIntrusive<TFreshIndexAndData>(hullCtx, std::move(arena)))
            , AppendixTree(hullCtx, AppendixTreeStagingCapacity)
        {}

        ui64 GetFirstLsn() const { return Min(IndexAndData->GetFirstLsn(), AppendixTree.GetFirstLsn()); }
        ui64 GetLastLsn() const { return Max(IndexAndData->GetLastLsn(), AppendixTree.GetLastLsn()); }
        bool Empty() const { return IndexAndData->Empty(); }
        ui64 ElementsInserted() const { return IndexAndData->ElementsInserted() + AppendixTree.ElementsInserted(); }
        ui64 GetFirstLsnToKeep() const { return GetFirstLsn(); }
        bool NeedsCompactionByYard(ui64 yardFreeUpToLsn) const { return yardFreeUpToLsn > GetFirstLsn(); }
        ui64 InPlaceSizeApproximation() const {
            const ui64 inPlaceSizeApproximation = IndexAndData->InPlaceSizeApproximation()
                + AppendixTree.SizeApproximation();
            return inPlaceSizeApproximation;
        }
        TSatisfactionRank GetSatisfactionRank() const {
            const ui64 inPlaceSizeApproximation = IndexAndData->InPlaceSizeApproximation()
                + AppendixTree.SizeApproximation();
            return TSatisfactionRank::MkRatio(inPlaceSizeApproximation, CompThreshold);
        }
        bool NeedsCompactionBySize() const {
            const ui64 inPlaceSizeApproximation = IndexAndData->InPlaceSizeApproximation()
                + AppendixTree.SizeApproximation();
            bool byInPlaceSize = inPlaceSizeApproximation > (CompThreshold / 32 * 31);
            bool byHugeSize = IndexAndData->HugeSizeApproximation() > HugeBlobsCompactionThreshold * ChunkSize;
            return !Empty() && (byInPlaceSize || byHugeSize);
        }

        // put newly received item into fresh segment
        void PutLogoBlobWithData(ui64 lsn, const TKey &key, ui8 partId, const TIngress &ingress, TRope buffer) {
            IndexAndData->PutLogoBlobWithData(lsn, key, partId, ingress, std::move(buffer));
        }
        const TRope& GetLogoBlobData(const TMemPart& memPart) const { return IndexAndData->GetLogoBlobData(memPart); }
        void Put(ui64 lsn, const TKey &key, const TMemRec &memRec) { return IndexAndData->Put(lsn, key, memRec); }
        void PutAppendix(std::shared_ptr<TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn) {
            AppendixTree.AddAppendix(std::move(a), firstLsn, lastLsn);
        }
        void OutputHtml(const TString &which, IOutputStream &str) const;
        void OutputProto(NKikimrVDisk::FreshSegmentStat *stat) const;
        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const { return IndexAndData->GetOwnedChunks(chunks); }
        void GetHugeBlobs(TSet<TDiskPart> &hugeBlobs) const { return IndexAndData->GetHugeBlobs(hugeBlobs); }
        // Appendix Compact/ApplyCompactionResult
        TCompactionJob Compact() { return MkCompactJob(AppendixTree.Compact()); }
        TCompactionJob ApplyCompactionResult(std::shared_ptr<ISTreeCompaction> cjob) {
            return MkCompactJob(AppendixTree.ApplyCompactionResult(cjob));
        }
        const TSTreeCompactionStat &GetCompactionStat() const { return AppendixTree.GetCompactionStat(); }
        // Snapshot
        TFreshSegmentSnapshot GetSnapshot() {
            return TFreshSegmentSnapshot(TFreshIndexAndDataSnapshot(IndexAndData), AppendixTree.GetSnapshot());
        }

    private:
        const ui64 CompThreshold;
        const ui64 ChunkSize;
        TInstant StartTime;
        TIntrusivePtr<TFreshIndexAndData> IndexAndData;
        // FIXME: implement TIntrusivePtr with deletion in batch pool
        TFreshAppendixTree<TKey, TMemRec> AppendixTree;

        TCompactionJob MkCompactJob(std::shared_ptr<ISTreeCompaction> &&job) {
            if (job) {
                return TCompactionJob(TIntrusivePtr<TThis>(this), std::move(job));
            } else {
                return {};
            }
        }
        void OutputBasicStatHtml(IOutputStream &str) const;
    };

} // NKikimr

#include "fresh_segment_impl.h"

namespace NKikimr {

    extern template class TFreshIndexAndDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshIndexAndDataSnapshot<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshIndexAndDataSnapshot<TKeyBlock, TMemRecBlock>;

    extern template class TFreshIndexAndData<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshIndexAndData<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshIndexAndData<TKeyBlock, TMemRecBlock>;

    extern template class TFreshSegmentSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshSegmentSnapshot<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshSegmentSnapshot<TKeyBlock, TMemRecBlock>;

    extern template class TFreshSegment<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshSegment<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshSegment<TKeyBlock, TMemRecBlock>;

 } // NKikimr

