#include "scrub_actor_impl.h"
#include "scrub_actor_huge_blob_merger.h"
#include <optional>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_SCRUB

namespace NKikimr {

    void TScrubCoroImpl::ScrubHugeBlobs() {
        TLevelIndexSnapshot::TBackwardIterator iter(Snap->HullCtx, &Snap->LogoBlobsSnap);
        THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, false> heapIt(&iter);
        if (State->HasBlobId()) {
            const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(State->GetBlobId());
            YDB_LOG_CTX_INFO(GetActorContext(), VDISKP(LogPrefix, "resuming huge blob scrubbing"),
                {"Marker", "VDS19"},
                {"Id", id});
            heapIt.Seek(id);
            if (heapIt.Valid() && heapIt.GetCurKey() == id) {
                heapIt.Prev(); // skip already processed blob
            }
        } else {
            YDB_LOG_CTX_INFO(GetActorContext(), VDISKP(LogPrefix, "starting huge blob scrubbing"),
                {"Marker", "VDS20"});
            heapIt.SeekToLast();
        }

        auto readHugeBlob = [&](const TDiskPart& location, TLogoBlobID blobId) {
            ++MonGroup.HugeBlobsRead();
            MonGroup.HugeBlobBytesRead() += location.Size;
            return Read(location, blobId);
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
