#pragma once

#include "defs.h"
#include "fresh_datasnap.h"
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_settings.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullsatisfactionrank.h>

#include <ydb/core/blobstorage/base/ptr.h>

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////
    // FreshData
    /////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshData : TNonCopyable {
    public:
        using TFreshSegment = ::NKikimr::TFreshSegment<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TFreshDataSnapshot = ::NKikimr::TFreshDataSnapshot<TKey, TMemRec>;
        using TFreshAppendix = ::NKikimr::TFreshAppendix<TKey, TMemRec>;
        using TCompactionJob = typename TFreshSegment::TCompactionJob;

    private:
        THullCtxPtr HullCtx;
        TIntrusivePtr<ITimeProvider> TimeProvider;
        const ui64 CompThreshold;
        TIntrusivePtr<TFreshSegment> Old;
        TIntrusivePtr<TFreshSegment> Dreg;
        TIntrusivePtr<TFreshSegment> Cur;
        ui64 OldSegLastKeepLsn = ui64(-1);
        bool WaitForCommit = false;
        const bool UseDreg;
        std::shared_ptr<TRopeArena> Arena;

        static constexpr ui64 CalculateBufLowWatermark(ui32 chunkSize, bool useDreg) {
            return ui64(chunkSize) * (2u + !!useDreg);
        }

    public:
        TFreshData(const TLevelIndexSettings &s, const TIntrusivePtr<ITimeProvider> &tp,
                std::shared_ptr<TRopeArena> arena)
            : HullCtx(s.HullCtx)
            , TimeProvider(tp)
            , CompThreshold(s.CompThreshold)
            , Cur(new TFreshSegment(HullCtx, s.CompThreshold, tp->Now(), arena))
            , UseDreg(s.FreshUseDreg)
            , Arena(std::move(arena))
        {}

        // Puts
        void Put(ui64 lsn, const TKey &key, const TMemRec &memRec);
        void PutLogoBlobWithData(ui64 lsn, const TKey &key, ui8 partId, const TIngress &ingress, TRope buffer);
        void PutAppendix(std::shared_ptr<TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn);

        // Compaction
        bool NeedsCompaction(ui64 yardFreeUpToLsn, bool force) const;
        TIntrusivePtr<TFreshSegment> FindSegmentForCompaction();
        void CompactionSstCreated(TIntrusivePtr<TFreshSegment> &&freshSegment);
        void CompactionFinished();
        bool CompactionInProgress() const { return Old.Get() || WaitForCommit; }

        // Appendix Compact/ApplyCompactionResult
        TCompactionJob CompactAppendix();
        TCompactionJob ApplyAppendixCompactionResult(TCompactionJob &&job);

        // you can't read from TFreshData directly, take a snapshot instead
        TFreshDataSnapshot GetSnapshot();
        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
        ui64 GetFirstLsnToKeep() const;
        ui64 GetFirstLsn() const;
        ui64 GetLastLsn() const;
        TSatisfactionRank GetSatisfactionRank() const;
        void OutputHtml(IOutputStream &str) const;
        void OutputProto(NKikimrVDisk::FreshStat *stat) const;

        bool Empty() const {
            return (!Old || Old->Empty()) && (!Dreg || Dreg->Empty()) && (!Cur || Cur->Empty());
        }

    private:
        void SwapWithDregIfRequired();
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TFreshData implementation
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::Put(ui64 lsn, const TKey &key, const TMemRec &memRec) {
        Cur->Put(lsn, key, memRec);
        SwapWithDregIfRequired();
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::PutLogoBlobWithData(
            ui64 lsn,
            const TKey &key,
            ui8 partId,
            const TIngress &ingress,
            TRope buffer)
    {
        Cur->PutLogoBlobWithData(lsn, key, partId, ingress, std::move(buffer));
        SwapWithDregIfRequired();
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::PutAppendix(std::shared_ptr<TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn) {
        Y_DEBUG_ABORT_UNLESS(lastLsn >= firstLsn);
        Cur->PutAppendix(std::move(a), firstLsn, lastLsn);
        SwapWithDregIfRequired();
    }

    template <class TKey, class TMemRec>
    bool TFreshData<TKey, TMemRec>::NeedsCompaction(ui64 yardFreeUpToLsn, bool force) const {
        if (CompactionInProgress()) {
            return false;
        } else if (force) {
            return true;
        } else {
            const bool compactDregByYard = UseDreg && Dreg && Dreg->NeedsCompactionByYard(yardFreeUpToLsn);
            const bool compactCurByYard = Cur && Cur->NeedsCompactionByYard(yardFreeUpToLsn);
            const bool compactCurBySize = Cur && Cur->NeedsCompactionBySize() && (!UseDreg || Dreg);
            return compactDregByYard || compactCurByYard || compactCurBySize;
        }
    }

    template <class TKey, class TMemRec>
    TIntrusivePtr<TFreshSegment<TKey, TMemRec>> TFreshData<TKey, TMemRec>::FindSegmentForCompaction() {
        Y_ABORT_UNLESS(!CompactionInProgress());
        if (Dreg) {
            Old.Swap(Dreg);
            Dreg.Swap(Cur);
        } else {
            Old.Swap(Cur);
        }

        OldSegLastKeepLsn = Old->GetFirstLsnToKeep();
        Cur = MakeIntrusive<TFreshSegment>(HullCtx, CompThreshold, TimeProvider->Now(), Arena);
        return Old;
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::CompactionSstCreated(TIntrusivePtr<TFreshSegment> &&freshSegment) {
        // FIXME ref count = 2?
        Y_ABORT_UNLESS(Old && Old.Get() == freshSegment.Get());
        freshSegment.Drop();
        Old.Drop();
        WaitForCommit = true;
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::CompactionFinished() {
        Y_ABORT_UNLESS(!Old && WaitForCommit);
        WaitForCommit = false;
        OldSegLastKeepLsn = ui64(-1);
    }

    template <class TKey, class TMemRec>
    typename TFreshData<TKey, TMemRec>::TCompactionJob TFreshData<TKey, TMemRec>::CompactAppendix() {
        return Cur->Compact();
    }

    template <class TKey, class TMemRec>
    typename TFreshData<TKey, TMemRec>::TCompactionJob TFreshData<TKey, TMemRec>::ApplyAppendixCompactionResult(
        TFreshData<TKey, TMemRec>::TCompactionJob &&job) {
        return job.ApplyCompactionResult();
    }

    template <class TKey, class TMemRec>
    TFreshDataSnapshot<TKey, TMemRec> TFreshData<TKey, TMemRec>::GetSnapshot() {
        return TFreshDataSnapshot(
                (Old ? TFreshSegmentSnapshot(Old->GetSnapshot()) : TFreshSegmentSnapshot()),
                (Dreg ? TFreshSegmentSnapshot(Dreg->GetSnapshot()) : TFreshSegmentSnapshot()),
                (Cur ? TFreshSegmentSnapshot(Cur->GetSnapshot()) : TFreshSegmentSnapshot()));
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::GetOwnedChunks(TSet<TChunkIdx>& chunks) const {
        if (Old)
            Old->GetOwnedChunks(chunks);
        if (Dreg)
            Dreg->GetOwnedChunks(chunks);
        Cur->GetOwnedChunks(chunks);
    }

    template <class TKey, class TMemRec>
    ui64 TFreshData<TKey, TMemRec>::GetFirstLsnToKeep() const {
        ui64 dregLsn = Dreg ? Dreg->GetFirstLsnToKeep() : Max<ui64>();
        ui64 curLsn = Cur->GetFirstLsnToKeep();
        return Min(OldSegLastKeepLsn, Min(dregLsn, curLsn));
    }

    template <class TKey, class TMemRec>
    ui64 TFreshData<TKey, TMemRec>::GetFirstLsn() const {
        ui64 dregLsn = Dreg ? Dreg->GetFirstLsn() : Max<ui64>();
        ui64 oldLsn = Old ? Old->GetFirstLsn() : Max<ui64>();
        return Min(Cur->GetFirstLsn(), Min(dregLsn, oldLsn));
    }

    template <class TKey, class TMemRec>
    ui64 TFreshData<TKey, TMemRec>::GetLastLsn() const {
        ui64 dregLsn = Dreg ? Dreg->GetLastLsn() : 0;
        ui64 oldLsn = Old ? Old->GetLastLsn() : 0;
        return Max(Cur->GetLastLsn(), Max(dregLsn, oldLsn));
    }

    template <class TKey, class TMemRec>
    TSatisfactionRank TFreshData<TKey, TMemRec>::GetSatisfactionRank() const {
        TSatisfactionRank oldRank = Old ? Old->GetSatisfactionRank() : TSatisfactionRank::MkZero();
        TSatisfactionRank dregRank = Dreg ? Dreg->GetSatisfactionRank() : TSatisfactionRank::MkZero();
        TSatisfactionRank curRank = Cur->GetSatisfactionRank();
        TSatisfactionRank res = Worst(oldRank, Worst(dregRank, curRank));
        return res;
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::OutputHtml(IOutputStream &str) const {
        if (Cur.Get())
            Cur->OutputHtml("Current", str);
        if (Dreg.Get())
            Dreg->OutputHtml("Dreg", str);
        if (Old.Get())
            Old->OutputHtml("Old", str);
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::OutputProto(NKikimrVDisk::FreshStat *stat) const {
        if (Cur) {
            Cur->OutputProto(stat->mutable_current());
        }
        if (Dreg) {
            Dreg->OutputProto(stat->mutable_dreg());
        }
        if (Old) {
            Old->OutputProto(stat->mutable_old());
        }
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::SwapWithDregIfRequired() {
        const bool renewCur = UseDreg && !Dreg && Cur->NeedsCompactionBySize();
        if (renewCur) {
            Dreg.Swap(Cur);
            Cur = MakeIntrusive<TFreshSegment>(HullCtx, CompThreshold, TimeProvider->Now(), Arena);
        }
    }

    extern template class TFreshData<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshData<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshData<TKeyBlock, TMemRecBlock>;

} // NKikimr

