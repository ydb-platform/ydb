#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_data.h>
#include "hullds_sstvec.h"
#include "hulldb_bulksstmngr.h"

#include <ydb/core/blobstorage/vdisk/hullop/hullcompdelete/blobstorage_hullcompdelete.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelIndexCtx
    // Some data that is used through the whole TLevelIndex
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLevelIndexCtx {
    public:
        // Every Sst at Level 0 has volatile growing id
        ui64 VolatileOrderId = 0;
    };


    /////////////////////////////////////////////////////////////////////////
    // INDEX DATA STRUCTURES
    /////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevel0
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevel0 {
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;
        typedef ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec> TUnorderedLevelSegments;
        typedef TIntrusivePtr<TUnorderedLevelSegments> TUnorderedLevelSegmentsPtr;

        // Max number of ssts at Level 0
        const ui32 MaxSstsAtOnce;
        // Context
        std::shared_ptr<TLevelIndexCtx> Ctx;
        TUnorderedLevelSegmentsPtr Segs;

        TLevel0(const TLevelIndexSettings &settings,
                const std::shared_ptr<TLevelIndexCtx> &ctx)
            : MaxSstsAtOnce(settings.GetMaxSstsAtLevel0AtOnce())
            , Ctx(ctx)
            , Segs(MakeIntrusive<TUnorderedLevelSegments>(settings.HullCtx->VCtx))
        {}

        TLevel0(const TLevelIndexSettings &settings,
                const std::shared_ptr<TLevelIndexCtx> &ctx,
                const NKikimrVDiskData::TLevel0 &pb)
            : MaxSstsAtOnce(settings.GetMaxSstsAtLevel0AtOnce())
            , Ctx(ctx)
            , Segs(MakeIntrusive<TUnorderedLevelSegments>(settings.HullCtx->VCtx, pb.GetSsts()))
        {
            Ctx->VolatileOrderId = pb.GetSsts().size();
        }

        bool Empty() const {
            return Segs->Empty();
        }

        // how many chunks this level uses
        ui32 ChunksNum(ui32 numLimit) const {
            return Segs->ChunksNum(numLimit);
        }

        ui32 CurSstsNum() const {
            return Segs->CurSstsNum();
        }

        void Put(const TLevelSegmentPtr &ptr) {
            if (ptr->VolatileOrderId == 0) {
                // new sst is inserted, assign order id for it
                Segs->Put(ptr, ++Ctx->VolatileOrderId);
            } else {
                // existing sst is inserted
                Segs->Put(ptr, ptr->VolatileOrderId);
            }
        }

        // NOTE: this is the obsolete way of discovering Last Compacted Lsn
        TOptLsn ObsoleteLastCompactedLsn() const {
            Y_ABORT_UNLESS(!Empty());
            return Segs->ObsoleteLastCompactedLsn();
        }

        void OutputHtml(ui32 &index, IOutputStream &str, TIdxDiskPlaceHolder::TInfo &sum) const {
            Segs->OutputHtml(index, str, sum);
        }

        void OutputProto(google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const {
            Segs->OutputProto(rows);
        }

        typename TUnorderedLevelSegments::TSstIterator GetSstIterator(ui32 numLimit) const {
            return typename TUnorderedLevelSegments::TSstIterator(Segs.Get(), numLimit);
        }

        void SerializeToProto(NKikimrVDiskData::TLevel0 &pb) const {
            Segs->SerializeToProto(pb);
        }

        TSatisfactionRank GetSatisfactionRank() const {
            return TSatisfactionRank::MkRatio(Segs->CurSstsNum(), MaxSstsAtOnce);
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            Segs->GetOwnedChunks(chunks);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TSortedLevel
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TSortedLevel {
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;

        TOrderedLevelSegmentsPtr Segs;
        TKey LastCompactedKey;

        TSortedLevel(const TKey &lastCompactedKey)
            : Segs(MakeIntrusive<TOrderedLevelSegments>())
            , LastCompactedKey(lastCompactedKey)
        {}

        TSortedLevel(TVDiskContextPtr vctx, const NKikimrVDiskData::TLevelX &pb)
            : Segs(MakeIntrusive<TOrderedLevelSegments>(vctx, pb.GetSsts()))
            , LastCompactedKey(*(TKey*)pb.GetLastCompacted().data())
        {}

        void SerializeToProto(NKikimrVDiskData::TLevelX &pb) const {
            auto lc = pb.MutableLastCompacted();
            lc->append((const char *)&LastCompactedKey, sizeof(LastCompactedKey));
            Segs->SerializeToProto(pb);
        }

        bool Empty() const {
            return Segs->Empty();
        }

        // how many chunks this level uses
        ui32 ChunksNum() const {
            return Segs->ChunksNum();
        }

        void Put(TLevelSegmentPtr &ptr) {
            Segs->Put(ptr);
        }

        // NOTE: this is the obsolete way of discovering Last Compacted Lsn
        TOptLsn ObsoleteLastCompactedLsn() const {
            Y_ABORT_UNLESS(!Empty());
            return Segs->ObsoleteLastCompactedLsn();
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            Segs->GetOwnedChunks(chunks);
        }

        void OutputHtml(ui32 &index, ui32 level, IOutputStream &str, TIdxDiskPlaceHolder::TInfo &sum) const {
            Segs->OutputHtml(index, level, str, sum);
        }

        void OutputProto(ui32 level, google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const {
            Segs->OutputProto(level, rows);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSlice
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TLevelSlice : public TThrRefBase {
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec> TUnorderedLevelSegments;
        typedef ::NKikimr::TLevelSlice<TKey, TMemRec> TThis;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
        typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
        typedef ::NKikimr::TLevel0<TKey, TMemRec> TLevel0;
        typedef ::NKikimr::TSortedLevel<TKey, TMemRec> TSortedLevel;
        typedef TVector<TSortedLevel> TSortedLevels;

        TLevel0 Level0;
        TSortedLevels SortedLevels;
        std::shared_ptr<TLevelIndexCtx> Ctx;

        // In ChunksToDelete we store chunks that are old and subject for deletion,
        // but previous snapshot can still use them
        TVector<ui32> ChunksToDelete;
        // Bulk-formed segments description; they contain only these bulk-formed SST references, which are required
        // to recover SyncLog in case of failure
        TBulkFormedSstInfoSet BulkFormedSegments;
        // last calculated total storage ratio for the whole level slice
        NHullComp::TSstRatio LastPublishedRatio;

        TLevelSlice(const TLevelIndexSettings &settings,
                    const std::shared_ptr<TLevelIndexCtx> &ctx)
            : Level0(settings, ctx)
            , SortedLevels()
            , Ctx(ctx)
            , ChunksToDelete()
            , BulkFormedSegments()
        {}

        TLevelSlice(const TLevelIndexSettings &settings,
                    const std::shared_ptr<TLevelIndexCtx> &ctx,
                    const NKikimrVDiskData::TLevelIndex &pb)
            : Level0(settings, ctx, pb.GetLevel0())
            , SortedLevels()
            , Ctx(ctx)
            , ChunksToDelete()
            , BulkFormedSegments(pb.GetBulkFormedSstInfoSet())
        {
            // deserialize chunks to delete
            ChunksToDelete.reserve(pb.DeletedChunksSize());
            for (const auto &x : pb.GetDeletedChunks()) {
                Y_ABORT_UNLESS(x);
                ChunksToDelete.push_back(x);
            }

            // deserialize other levels
            SortedLevels.reserve(pb.OtherLevelsSize());
            for (const auto &x : pb.GetOtherLevels()) {
                SortedLevels.push_back(TSortedLevel(settings.HullCtx->VCtx, x));
            }
        }

        ~TLevelSlice()
        {}

        void SerializeToProto(NKikimrVDiskData::TLevelIndex &pb) const {
            // write chunks to delete
            auto dc = pb.MutableDeletedChunks();
            dc->Reserve(ChunksToDelete.size());
            for (const auto &x : ChunksToDelete) {
                Y_ABORT_UNLESS(x);
                dc->Add(x);
            }
            // serialize level0
            Level0.SerializeToProto(*pb.MutableLevel0());
            // serialize other levels
            for (const auto &x : SortedLevels) {
                auto level = pb.AddOtherLevels();
                x.SerializeToProto(*level);
            }
            // serialize BulkFormedSstInfoSet
            BulkFormedSegments.SerializeToProto(*pb.MutableBulkFormedSstInfoSet());
        }

        void Put(TLevelSstPtr &pair) {
            if (pair.Level == 0)
                Level0.Put(pair.SstPtr);
            else
                SortedLevels[pair.Level - 1].Put(pair.SstPtr);
        }

        // NOTE: this is the obsolete way of discovering Last Compacted Lsn
        TOptLsn ObsoleteLastCompactedLsn() const {
            TOptLsn lastCompactedLsn;
            if (!Level0.Empty()) {
                lastCompactedLsn.SetMax(Level0.ObsoleteLastCompactedLsn());
            }
            for (const auto &x : SortedLevels) {
                if (!x.Empty()) {
                    lastCompactedLsn.SetMax(x.ObsoleteLastCompactedLsn());
                }
            }
            return lastCompactedLsn;
        }

        bool Empty() const {
            if (!Level0.Empty())
                return false;

            for (const auto &x : SortedLevels) {
                if (!x.Empty())
                    return false;
            }
            return true;
        }

        bool FullyCompacted() const {
            if (!Level0.Empty())
                return false;
            bool prev = true;
            for (const auto &x : SortedLevels) {
                if (!prev)
                    return false;
                prev = x.Empty();
            }
            return true;
        }

        TString ToString(const TString &prefix = "") const {
            TStringStream str;
            TSstIterator it(this, Level0CurSstsNum());
            it.SeekToFirst();
            ui32 level = ui32(-1);
            while (it.Valid()) {
                TLevelSstPtr p = it.Get();
                if (p.Level != level) {
                    level = p.Level;
                    str << prefix << "Level " << level << "\n";
                }
                str << prefix << "  SST: chunks=[" << p.SstPtr->ChunksToString() << "]\n";
                it.Next();
            }
            return str.Str();
        }

        void OutputHtml(IOutputStream &str) const;
        void OutputProto(google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const;

        ui32 GetLevelXNumber() const {
            return SortedLevels.size();
        }

        ui32 GetLevel0ChunksNum(ui32 numLimit) const {
            return Level0.ChunksNum(numLimit);
        }

        ui32 GetLevelXChunksNum(ui32 idx) const {
            return SortedLevels[idx].ChunksNum();
        }

        typename TUnorderedLevelSegments::TSstIterator GetLevel0SstIterator(ui32 numLimit) const {
            return Level0.GetSstIterator(numLimit);
        }

        const TSortedLevel &GetLevelXRef(ui32 idx) const {
            return SortedLevels[idx];
        }

        ui32 Level0CurSstsNum() const {
            return Level0.CurSstsNum();
        }

        TSatisfactionRank GetSatisfactionRank() const {
            return Level0.GetSatisfactionRank();
        }

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
            // we include deleted chunks
            for (TChunkIdx chunkIdx : ChunksToDelete) {
                const bool inserted = chunks.insert(chunkIdx).second;
                Y_ABORT_UNLESS(inserted);
            }

            // include bulk formed segments
            BulkFormedSegments.GetOwnedChunks(chunks);

            // include levels
            Level0.GetOwnedChunks(chunks);
            for (const auto& level : SortedLevels) {
                level.GetOwnedChunks(chunks);
            }
        }

        // iterator through sorted levels (doesn't include Level0)
        class TSortedLevelsIter;
        // iterator through ssts (all SortedLevels)
        class TSortedLevelsSSTIter;
        // iterator through ssts (Level0 + SortedLevels)
        class TSstIterator;

        class TForwardIterator;
        class TBackwardIterator;
    };

    extern template struct TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template struct TLevelSlice<TKeyBarrier, TMemRecBarrier>;
    extern template struct TLevelSlice<TKeyBlock, TMemRecBlock>;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelSliceSnapshot
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSliceSnapshot : public TThrRefBase {
    private:
        typedef ::NKikimr::TLevelSlice<TKey, TMemRec> TLevelSlice;
        typedef TIntrusivePtr<TLevelSlice> TLevelSlicePtr;

    public:
        using TSortedLevel = typename TLevelSlice::TSortedLevel;
        using TLevelSegment = typename TLevelSlice::TLevelSegment;
        using TLevelSegmentPtr = typename TLevelSlice::TLevelSegmentPtr;
        using TSegments = TVector<TLevelSegmentPtr>;
        using TUnorderedLevelSegments = ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec>;

    private:
        TLevelSlicePtr Slice; // NOTE: it's unsafe using Slice manually, do it via iterators only
        ui32 Level0SegsNum;

    public:
        TLevelSliceSnapshot(const TLevelSlicePtr &slice, ui32 level0SegsNum)
            : Slice(slice)
            , Level0SegsNum(level0SegsNum)
        {}

        TLevelSliceSnapshot(const TLevelSliceSnapshot &snap) = default;
        TLevelSliceSnapshot(TLevelSliceSnapshot &&snap) = default;

        void Destroy() {
            Slice.Drop();
            Level0SegsNum = 0;
        }

        ui32 GetLevelXNumber() const {
            return Slice->GetLevelXNumber();
        }

        ui32 GetLevel0ChunksNum() const {
            return Slice->GetLevel0ChunksNum(Level0SegsNum);
        }

        ui32 GetLevelXChunksNum(ui32 idx) const {
            return Slice->GetLevelXChunksNum(idx);
        }

        typename TUnorderedLevelSegments::TSstIterator GetLevel0SstIterator() const {
            return Slice->GetLevel0SstIterator(Level0SegsNum);
        }

        const TSortedLevel &GetLevelXRef(ui32 idx) const {
            return Slice->GetLevelXRef(idx);
        }

        ui32 GetLevel0SstsNum() const {
            return Level0SegsNum;
        }

        void Output(IOutputStream &str) const {
            TSstIterator it(this);
            it.SeekToFirst();
            unsigned curLevel = 0;
            bool firstIteration = true;

            while (it.Valid()) {
                auto val = it.Get();
                if (firstIteration) {
                    firstIteration = false;
                    curLevel = val.Level;
                    str << "=== LEVEL: " << curLevel << " ===\n";
                } else {
                    // next level at new line
                    if (curLevel != val.Level) {
                        curLevel = val.Level;
                        str << "=== " << curLevel << "===\n";
                    } else {
                        str << "\n";
                    }
                }
                val.SstPtr->Output(str);
                it.Next();
            }
        }

        class TSstIterator;
        class TSortedLevelsIter;
        class TForwardIterator;
        class TBackwardIterator;
    };

    extern template class TLevelSliceSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TLevelSliceSnapshot<TKeyBarrier, TMemRecBarrier>;
    extern template class TLevelSliceSnapshot<TKeyBlock, TMemRecBlock>;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLevelIndexActorCtx
    // Some data that is used by LevelIndexActor
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLevelIndexActorCtx {
    public:
        // Active actors that are owned by TLevelIndexActor
        TActiveActors ActiveActors;
    };

} // NKikimr
