#pragma once

#include "defs.h"
#include "scrub_actor_impl.h"

namespace NKikimr {

    class TScrubCoroImpl::TBlobLocationExtractorMerger {
    protected:
        const TBlobStorageGroupType GType;

    public:
        std::vector<TBlobOnDisk> BlobsOnDisk;

    public:
        TBlobLocationExtractorMerger(const TBlobStorageGroupType& gtype)
            : GType(gtype)
        {}

        static bool HaveToMergeData() { return true; }

        // process on-disk data
        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key,
                ui64 /*circaLsn*/, const void* /*sst*/) {
            if (memRec.GetType() != TBlobType::DiskBlob) {
                return;
            }

            // extract blob location
            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, outbound);
            const TDiskPart& part = extr.SwearOne();
            if (part.ChunkIdx && part.Size) {
                const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
                BlobsOnDisk.push_back({key.LogoBlobID(), local, part});
            }
        }

        // just ignore in-memory data
        void AddFromFresh(const TMemRecLogoBlob& /*memRec*/, const TRope* /*data*/, const TKeyLogoBlob& /*key*/,
                ui64 /*lsn*/)
        {}

        void Clear() {
            BlobsOnDisk.clear();
        }
    };

    class TScrubCoroImpl::TSstBlobMerger : public TBlobLocationExtractorMerger {
        const TString VDiskLogPrefix;
        TLevelIndexSnapshot::TForwardIterator Iter; // whole database iterator to merge records for GC checking
        TIndexRecordMerger Merger; // merger for these records
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> Essence;
        const bool AllowKeepFlags;
        std::optional<bool> KeepData;

    public:
        TSstBlobMerger(const THullDsSnap& snap,
                TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> essence)
            : TBlobLocationExtractorMerger(snap.HullCtx->VCtx->Top->GType)
            , VDiskLogPrefix(snap.HullCtx->VCtx->VDiskLogPrefix)
            , Iter(snap.HullCtx, &snap.LogoBlobsSnap)
            , Merger(GType)
            , Essence(std::move(essence))
            , AllowKeepFlags(snap.HullCtx->AllowKeepFlags)
        {
            Iter.SeekToFirst();
        }

        static bool HaveToMergeData() { return true; }

        // process on-disk data
        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key,
                ui64 circaLsn, const auto *sst) {
            if (memRec.GetType() == TBlobType::DiskBlob) {
                TBlobLocationExtractorMerger::AddFromSegment(memRec, outbound, key, circaLsn, sst);
            }
        }

        // just ignore in-memory data
        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope *data, const TKeyLogoBlob& key, ui64 lsn) {
            TBlobLocationExtractorMerger::AddFromFresh(memRec, data, key, lsn);
        }

        void Clear() {
            KeepData.reset();
            TBlobLocationExtractorMerger::Clear();
        }

        bool Keep(const TLogoBlobID& id) {
            if (!KeepData) {
                // seek to the desired key; the key MUST exist in the whole database
                Y_VERIFY_S(Iter.Valid(), VDiskLogPrefix);
                Y_VERIFY_S(Iter.GetCurKey() <= id, VDiskLogPrefix);
                if (Iter.GetCurKey() < id) {
                    Iter.Next();
                    Y_VERIFY_S(Iter.Valid(), VDiskLogPrefix);
                    if (Iter.GetCurKey() < id) {
                        Iter.Seek(id);
                    }
                }
                Y_VERIFY_S(Iter.Valid() && Iter.GetCurKey() == id, VDiskLogPrefix);

                // put iterator value to merger
                Iter.PutToMerger(&Merger);
                Merger.Finish();

                // obtain keep status
                NGc::TKeepStatus status = Essence->Keep(id, Merger.GetMemRec(), {}, AllowKeepFlags, true /*allowGarbageCollection*/);

                // clear merger for next operation
                Merger.Clear();

                // return true unless we need to discard this blob out of the list
                KeepData.emplace(status.KeepData);
            }
            return *KeepData;
        }
    };

} // NKikimr
