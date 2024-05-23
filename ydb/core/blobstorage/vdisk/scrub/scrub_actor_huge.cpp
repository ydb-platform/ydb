#include "scrub_actor_impl.h"
#include "scrub_actor_huge_blob_merger.h"

namespace NKikimr {

    void TScrubCoroImpl::ScrubHugeBlobs() {
        TLevelIndexSnapshot::TBackwardIterator iter(Snap->HullCtx, &Snap->LogoBlobsSnap);
        if (State->HasBlobId()) {
            const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(State->GetBlobId());
            STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS19, VDISKP(LogPrefix, "resuming huge blob scrubbing"),
                (Id, id));
            iter.Seek(id);
            if (iter.Valid() && iter.GetCurKey() == id) {
                iter.Prev();
            }
        } else {
            STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS20, VDISKP(LogPrefix, "starting huge blob scrubbing"));
            // FIXME: check if this is correct logic?
            iter.Seek(TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie));
        }

        auto readHugeBlob = [&](const TDiskPart& location) {
            ++MonGroup.HugeBlobsRead();
            MonGroup.HugeBlobBytesRead() += location.Size;
            return Read(location);
        };

        auto essence = GetBarriersEssence();
        THugeBlobMerger merger(LogPrefix, Info->Type, readHugeBlob, this);
        TIndexRecordMerger indexMerger(Info->Type); // for GC checking

        const TInstant startTime = TActorCoroImpl::Now();
        do {
            if (!iter.Valid()) {
                break;
            }

            iter.PutToMerger(&indexMerger);
            indexMerger.Finish();
            auto status = essence->Keep(iter.GetCurKey(), indexMerger.GetMemRec(), {}, Snap->HullCtx->AllowKeepFlags,
                true /*allowGarbageCollection*/);
            indexMerger.Clear();

            const TLogoBlobID& id = iter.GetCurKey().LogoBlobID();

            if (status.KeepData) {
                merger.Begin(id);
                iter.PutToMerger(&merger);
                const NMatrix::TVectorType needed = merger.GetPartsToRestore();
                UpdateUnreadableParts(id, needed, merger.GetCorruptedPart());
                if (!needed.Empty()) {
                    Checkpoints |= TEvScrubNotify::HUGE_BLOB_SCRUBBED;
                }
                merger.Clear();
            } else {
                DropGarbageBlob(id);
            }

            iter.Prev();
        } while (TActorCoroImpl::Now() < startTime + TDuration::Seconds(5));

        if (iter.Valid()) {
            LogoBlobIDFromLogoBlobID(iter.GetCurKey().LogoBlobID(), State->MutableBlobId());
        } else {
            State->ClearBlobId();
        }
    }

} // NKikimr
