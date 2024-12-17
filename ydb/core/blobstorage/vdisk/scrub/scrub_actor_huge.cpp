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
                heapIt.Prev();
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
        if (heapIt.Valid()) {
            auto callback = [&] (TKeyLogoBlob key, auto* merger) -> bool {
                auto status = essence->Keep(key, merger->GetMemRec(), {}, Snap->HullCtx->AllowKeepFlags,
                    true /*allowGarbageCollection*/);
                const TLogoBlobID& id = key.LogoBlobID();
                if (status.KeepData) {
                    const NMatrix::TVectorType needed = merger->GetPartsToRestore();
                    UpdateUnreadableParts(id, needed, merger->GetCorruptedPart());
                    if (!needed.Empty()) {
                        Checkpoints |= TEvScrubNotify::HUGE_BLOB_SCRUBBED;
                    }
                } else {
                    DropGarbageBlob(id);
                }
                return TActorCoroImpl::Now() < startTime + TDuration::Seconds(5);
            };
            heapIt.Walk(std::nullopt, &merger, callback);
        }
        if (heapIt.Valid()) {
            LogoBlobIDFromLogoBlobID(heapIt.GetCurKey().LogoBlobID(), State->MutableBlobId());
        } else {
            State->ClearBlobId();
        }
    }

} // NKikimr
