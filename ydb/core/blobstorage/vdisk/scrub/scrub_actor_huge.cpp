#include "scrub_actor_impl.h"
#include "scrub_actor_huge_blob_merger.h"
#include <optional>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>

namespace NKikimr {

    void TScrubCoroImpl::ScrubHugeBlobs() {
        TLevelIndexSnapshot::TBackwardIterator iter(Snap->HullCtx, &Snap->LogoBlobsSnap);
        THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, false> heapIt(&iter);
        if (State->HasBlobId()) {
            const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(State->GetBlobId());
            STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS19, VDISKP(LogPrefix, "resuming huge blob scrubbing"),
                (Id, id));
            heapIt.Seek(id);
            if (heapIt.Valid() && heapIt.GetCurKey() == id) {
                heapIt.Prev(); // skip already processed blob
            }
        } else {
            STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS20, VDISKP(LogPrefix, "starting huge blob scrubbing"));
            // FIXME: check if this is correct logic?
            heapIt.Seek(TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie));
        }

        auto readHugeBlob = [&](const TDiskPart& location) {
            ++MonGroup.HugeBlobsRead();
            MonGroup.HugeBlobBytesRead() += location.Size;
            return Read(location);
        };

        auto essence = GetBarriersEssence();
        THugeBlobAndIndexMerger merger(LogPrefix, Info->Type, readHugeBlob, this);

        const TInstant startTime = TActorCoroImpl::Now();
        bool timeout = false;
        heapIt.Walk(std::nullopt, &merger, [&](TKeyLogoBlob key, auto *merger) -> bool {
            const auto status = essence->Keep(key, merger->GetMemRec(), {}, Snap->HullCtx->AllowKeepFlags,
                true /*allowGarbageCollection*/);
            const TLogoBlobID& id = key.LogoBlobID();
            LogoBlobIDFromLogoBlobID(id, State->MutableBlobId()); // remember last processed blob
            if (status.KeepData) {
                if (ScrubCtx->EnableDeepScrubbing) {
                    EnqueueCheckIntegrity(id, true);
                }
                const NMatrix::TVectorType needed = merger->GetPartsToRestore();
                UpdateUnreadableParts(id, needed, merger->GetCorruptedPart());
                if (!needed.Empty()) {
                    Checkpoints |= TEvScrubNotify::HUGE_BLOB_SCRUBBED;
                }
            } else {
                DropGarbageBlob(id);
            }
            if (TActorCoroImpl::Now() < startTime + TDuration::Seconds(5)) {
                return true;
            }
            timeout = true;
            return false;
        });
        if (!timeout) { // we have finished walking without premature exit, so we have processed all the blobs
            State->ClearBlobId();
        }
    }

} // NKikimr
