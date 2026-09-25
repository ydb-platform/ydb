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
        // Old was selected for compaction, that compaction gave up without writing
        // anything, and the very same segment is waiting to be tried again.
        bool RetryOldSegment = false;
        const bool UseDreg;
        std::shared_ptr<TRopeArena> Arena;

        // Records admitted against Cur's reserved chunks that have not reached Fresh yet.
        TFreshOutputEstimate InFlight;
        // Admitted records that have no LSN yet (see TFreshAdmission::Unsequenced): huge blobs whose data is still
        // being written, which can take a while. They may land in whichever segment is Cur by then without breaking
        // the LSN order of the segments, so a rotation carries them over to the new Cur together with enough of the
        // old one's reserved chunks to compact them, instead of waiting for them.
        TFreshOutputEstimate Unsequenced;
        // Cur is due to be rotated out, but records are still in flight. Rotation waits for them, so every
        // admitted record lands in the very segment its chunks were reserved for, and admission holds new
        // records back until it happens. Unsequenced records hold it back only when Cur's reserved chunks cannot
        // be split between Cur and what it carries over (see InFlightAllowsRotation()). The compaction flag is
        // recomputed by every NeedsCompaction().
        mutable bool CompactionRotationPending = false;
        bool DregRotationPending = false;
        // Admission asked for Cur to rotate out as it is, rather than grow past one SST; see RequestSizeRotation().
        bool SizeRotationRequested = false;

        static constexpr ui64 CalculateBufLowWatermark(ui32 chunkSize, bool useDreg) {
            return ui64(chunkSize) * (2u + !!useDreg);
        }

    public:
        TFreshData(const TLevelIndexSettings &s, const TIntrusivePtr<ITimeProvider> &tp,
                std::shared_ptr<TRopeArena> arena)
            : HullCtx(s.HullCtx)
            , TimeProvider(tp)
            , CompThreshold(Max<ui64>(HullCtx->ChunkSize, s.CompThreshold))
            , Cur(new TFreshSegment(HullCtx, s.CompThreshold, tp->Now(), arena))
            , UseDreg(s.FreshUseDreg)
            , Arena(std::move(arena))
        {}

        // Puts
        void Put(ui64 lsn, const TKey &key, const TMemRec &memRec);
        void PutLogoBlobWithData(ui64 lsn, const TKey &key, ui8 partId, const TIngress &ingress, TRope buffer,
            std::optional<ui64> checksum);
        void PutAppendix(std::shared_ptr<TFreshAppendix> &&a, ui64 firstLsn, ui64 lastLsn);

        // Compaction
        bool NeedsCompaction(ui64 yardFreeUpToLsn, bool force) const;
        ui64 GetFreeInPlaceSizeApproximation() const;

        TIntrusivePtr<TFreshSegment> FindSegmentForCompaction();
        void CompactionSstCreated(TIntrusivePtr<TFreshSegment> &&freshSegment);
        void CompactionFinished();
        void CompactionAborted();
        bool CompactionInProgress() const { return Old.Get() || WaitForCommit; }

        // Chunk reservation. A record is admitted only once Cur holds enough reserved chunks to compact
        // everything already in it, everything in flight, and the record itself -- and, while unsequenced records
        // are in flight, enough to compact those apart from the rest, so that a rotation can carry them over. Writers
        // that do not go through admission put into Cur all the same: their records are charged like any other and
        // may take Cur past its reservation, in which case its compaction reserves the rest itself.
        bool IsRotationPending() const { return CompactionRotationPending || DregRotationPending; }
        ui64 GetCurReservationShortfall(const TFreshOutputEstimate& record, bool unsequenced = false) const;
        void AddCurReservedChunks(const TVector<TChunkIdx>& chunks) { Cur->AddReservedChunks(chunks); }
        void AdmitInFlight(const TFreshOutputEstimate& record, bool unsequenced = false) {
            (unsequenced ? Unsequenced : InFlight).Merge(record);
        }
        // An unsequenced record has got its LSN: from here on it lands in the current Cur, like any other.
        void SequenceInFlight(const TFreshOutputEstimate& record);
        // Called once an admitted record has been put into Fresh, or instead of that if it never will be. Not
        // before the Put(): landing may let a pending rotation happen, and that must not move the record into a
        // segment nothing was reserved for.
        void LandInFlight(const TFreshOutputEstimate& record, bool unsequenced = false);
        const TFreshOutputEstimate& GetInFlight() const { return InFlight; }
        const TFreshOutputEstimate& GetUnsequenced() const { return Unsequenced; }

        // Cur would need more than one SST to compact once `record` and everything in flight have landed in it.
        bool WouldOutgrowSst(const TFreshOutputEstimate& record) const;
        // Cur can be rotated out now: into Dreg, or by starting a compaction.
        bool CanRotateCur() const { return (UseDreg && !Dreg) || !CompactionInProgress(); }
        // Rotate Cur out as it is, so that it compacts into a single SST instead of growing past it. Into Dreg this
        // happens right here; otherwise NeedsCompaction() asks for it. Records in flight hold it back, as always.
        void RequestSizeRotation();

        // Appendix Compact/ApplyCompactionResult
        TCompactionJob CompactAppendix();
        TCompactionJob ApplyAppendixCompactionResult(TCompactionJob &&job);

        // you can't read from TFreshData directly, take a snapshot instead
        TFreshDataSnapshot GetSnapshot();
        template <class TCallback>
        void ForEachHugeBlob(TCallback&& callback) const {
            if (Old)
                Old->ForEachHugeBlob(callback);
            if (Dreg)
                Dreg->ForEachHugeBlob(callback);
            Cur->ForEachHugeBlob(callback);
        }
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
        void RenewCur(TFreshSegment& previous);
        // Nothing in flight keeps Cur from rotating out now: no sequenced record is on its way into it, and its
        // reserved chunks cover both what it holds and the unsequenced records it would carry over.
        bool InFlightAllowsRotation() const { return InFlight.Empty() && CoversCarry(*Cur); }
        bool CoversCarry(const TFreshSegment& segment) const {
            return Unsequenced.Empty() || segment.GetReservedChunks().size()
                >= segment.GetOutputChunks() + Unsequenced.GetChunks(segment.GetOutputGeometry());
        }
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
    void TFreshData<TKey, TMemRec>::PutLogoBlobWithData(ui64 lsn, const TKey &key, ui8 partId, const TIngress &ingress,
            TRope buffer, std::optional<ui64> checksum) {
        Cur->PutLogoBlobWithData(lsn, key, partId, ingress, std::move(buffer), checksum);
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
        CompactionRotationPending = false;
        if (RetryOldSegment) {
            // Old is still held by an attempt that gave up; it has to be written out
            // before anything else can be compacted, and until it is the recovery log
            // can not be cut past it. Nothing rotates, so nothing has to wait.
            return true;
        } else if (CompactionInProgress()) {
            return false;
        }
        bool wanted = force;
        if (!wanted) {
            const bool compactDregByYard = UseDreg && Dreg && Dreg->NeedsCompactionByYard(yardFreeUpToLsn);
            const bool compactCurByYard = Cur && Cur->NeedsCompactionByYard(yardFreeUpToLsn);
            const bool compactCurBySize = Cur && (Cur->NeedsCompactionBySize() || SizeRotationRequested)
                && (!UseDreg || Dreg);
            wanted = compactDregByYard || compactCurByYard || compactCurBySize;
        }
        if (wanted && !InFlightAllowsRotation()) {
            // Starting the compaction rotates Cur out. Wait for the records in flight to land first.
            CompactionRotationPending = true;
            return false;
        }
        return wanted;
    }

    template <class TKey, class TMemRec>
    ui64 TFreshData<TKey, TMemRec>::GetFreeInPlaceSizeApproximation() const {
        auto threshold = CompThreshold / 32 * 40;
        if (UseDreg && !Dreg) {
            return threshold;
        }
        if (Cur) {
            auto size = Cur->InPlaceSizeApproximation();
            return size >= threshold ? 0 : threshold - size;
        }
        return threshold;
    }

    template <class TKey, class TMemRec>
    ui64 TFreshData<TKey, TMemRec>::GetCurReservationShortfall(const TFreshOutputEstimate& record,
            bool unsequenced) const {
        const TFreshOutputGeometry& geometry = Cur->GetOutputGeometry();
        TFreshOutputEstimate sequenced = Cur->GetOutputEstimate();
        sequenced.Merge(InFlight);
        TFreshOutputEstimate carried = Unsequenced;
        (unsequenced ? carried : sequenced).Merge(record);
        TFreshOutputEstimate total = sequenced;
        total.Merge(carried);
        ui64 needed = total.GetChunks(geometry);
        if (!carried.Empty()) {
            // Enough to split between Cur and what a rotation carries over (CoversCarry()). Compacting two parts on
            // their own can take an SST more than compacting them together.
            needed = Max(needed, sequenced.GetChunks(geometry) + carried.GetChunks(geometry));
        }
        const ui64 held = Cur->GetReservedChunks().size();
        return needed > held ? needed - held : 0;
    }

    template <class TKey, class TMemRec>
    bool TFreshData<TKey, TMemRec>::WouldOutgrowSst(const TFreshOutputEstimate& record) const {
        TFreshOutputEstimate total = Cur->GetOutputEstimate();
        total.Merge(InFlight);
        if (total.Empty()) {
            // A record that does not fit an empty segment cannot be helped by rotating it; nor can the unsequenced
            // records, which a rotation carries over.
            return false;
        }
        total.Merge(Unsequenced);
        total.Merge(record);
        const TFreshOutputGeometry& geometry = Cur->GetOutputGeometry();
        return total.GetChunks(geometry) > Max<ui32>(geometry.ChunksPerSst, 1);
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::RequestSizeRotation() {
        SizeRotationRequested = true;
        if (UseDreg && !Dreg) {
            SwapWithDregIfRequired();
        }
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::SequenceInFlight(const TFreshOutputEstimate& record) {
        // Cur's reserved chunks cover the record either way; only whether a rotation can carry it changes.
        Unsequenced.Subtract(record);
        InFlight.Merge(record);
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::LandInFlight(const TFreshOutputEstimate& record, bool unsequenced) {
        (unsequenced ? Unsequenced : InFlight).Subtract(record);
        if (DregRotationPending && InFlightAllowsRotation()) {
            // The swap was waiting for exactly this: every admitted record is in Cur now, or can be carried over,
            // so Cur can rotate out. It happens here rather than in the next Put(), which admission is holding back
            // meanwhile.
            SwapWithDregIfRequired();
        }
    }

    // A new Cur takes over whatever the previous one held beyond its own needs. Nothing sequenced is in
    // flight when this happens, so the previous segment keeps exactly what compacting it requires, and the
    // rest serves the unsequenced records it carries over and the new writes, instead of going back to
    // PDisk only to be reserved again.
    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::RenewCur(TFreshSegment& previous) {
        Y_VERIFY_DEBUG_S(InFlight.Empty(), HullCtx->VCtx->VDiskLogPrefix
            << "Fresh segment rotates with records in flight");
        Y_VERIFY_DEBUG_S(CoversCarry(previous), HullCtx->VCtx->VDiskLogPrefix
            << "Fresh segment rotates without the chunks for the unsequenced records it carries over");
        Cur = MakeIntrusive<TFreshSegment>(HullCtx, CompThreshold, TimeProvider->Now(), Arena);
        Cur->AddReservedChunks(previous.TakeSurplusReservedChunks());
        SizeRotationRequested = false;
    }

    template <class TKey, class TMemRec>
    TIntrusivePtr<TFreshSegment<TKey, TMemRec>> TFreshData<TKey, TMemRec>::FindSegmentForCompaction() {
        if (RetryOldSegment) {
            // Retry the segment the previous attempt gave up on. Nothing is swapped:
            // the segments keep the order they already have and OldSegLastKeepLsn still
            // describes this one.
            Y_VERIFY_S(Old && !WaitForCommit, HullCtx->VCtx->VDiskLogPrefix);
            RetryOldSegment = false;
            return Old;
        }
        Y_VERIFY_S(!CompactionInProgress(), HullCtx->VCtx->VDiskLogPrefix);
        TIntrusivePtr<TFreshSegment> previous = Cur;
        if (Dreg) {
            Old.Swap(Dreg);
            Dreg.Swap(Cur);
        } else {
            Old.Swap(Cur);
        }

        OldSegLastKeepLsn = Old->GetFirstLsnToKeep();
        RenewCur(*previous);
        return Old;
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::CompactionSstCreated(TIntrusivePtr<TFreshSegment> &&freshSegment) {
        // FIXME ref count = 2?
        Y_VERIFY_S(Old && Old.Get() == freshSegment.Get(), HullCtx->VCtx->VDiskLogPrefix);
        // Old's chunks went to its compaction as it started, which commits the ones it wrote and forgets the rest.
        Y_VERIFY_DEBUG_S(Old->GetReservedChunks().empty(), HullCtx->VCtx->VDiskLogPrefix
            << "compacted Fresh segment still holds reserved chunks");
        freshSegment.Drop();
        Old.Drop();
        WaitForCommit = true;
    }

    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::CompactionFinished() {
        Y_VERIFY_S(!Old && WaitForCommit, HullCtx->VCtx->VDiskLogPrefix);
        WaitForCommit = false;
        OldSegLastKeepLsn = ui64(-1);
    }

    // The compaction of Old gave up before creating an sst, so Old stays exactly where
    // it is and is marked for another attempt. Dropping it here would lose the records;
    // leaving it without this mark would keep CompactionInProgress() true forever, which
    // is what stops NeedsCompaction() from ever asking for it again and pins the
    // recovery log at this segment's first lsn.
    template <class TKey, class TMemRec>
    void TFreshData<TKey, TMemRec>::CompactionAborted() {
        Y_VERIFY_S(Old && !WaitForCommit, HullCtx->VCtx->VDiskLogPrefix);
        RetryOldSegment = true;
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
        if (!InFlight.Empty() || !Unsequenced.Empty() || IsRotationPending()) {
            str << "InFlightRecords: " << InFlight.GetRecords()
                << "    UnsequencedRecords: " << Unsequenced.GetRecords()
                << "    RotationPending: " << (IsRotationPending() ? "yes" : "no") << "\n";
        }
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
        const bool renewCur = UseDreg && !Dreg && (Cur->NeedsCompactionBySize() || SizeRotationRequested);
        // Rotating Cur out, like starting a compaction, waits for the records in flight to land.
        DregRotationPending = renewCur && !InFlightAllowsRotation();
        if (renewCur && !DregRotationPending) {
            Dreg.Swap(Cur);
            RenewCur(*Dreg);
        }
    }

    extern template class TFreshData<TKeyLogoBlob, TMemRecLogoBlob>;
    extern template class TFreshData<TKeyBarrier, TMemRecBarrier>;
    extern template class TFreshData<TKeyBlock, TMemRecBlock>;

} // NKikimr
