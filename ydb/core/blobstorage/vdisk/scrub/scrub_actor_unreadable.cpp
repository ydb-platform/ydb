#include "scrub_actor_impl.h"
#include "restore_corrupted_blob_actor.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>

namespace NKikimr {

    void TScrubCoroImpl::DropGarbageBlob(const TLogoBlobID& fullId) {
        Y_ABORT_UNLESS(!fullId.PartId());
        if (const auto it = UnreadableBlobs.find(fullId); it != UnreadableBlobs.end()) {
            STLOGX(GetActorContext(), PRI_NOTICE, BS_VDISK_SCRUB, VDS39, VDISKP(LogPrefix,
                "dropped garbage unreadable blob"), (BlobId, it->first), (UnreadableParts, it->second.UnreadableParts));
            MonGroup.UnreadableBlobsFound() -= it->second.UnreadableParts.CountBits();
            UnreadableBlobs.erase(it);
        }
    }

    void TScrubCoroImpl::AddUnreadableParts(const TLogoBlobID& fullId, NMatrix::TVectorType corrupted, TDiskPart corruptedPart) {
        if (const auto it = UnreadableBlobs.find(fullId); it != UnreadableBlobs.end()) {
            corrupted |= it->second.UnreadableParts;
        }
        UpdateUnreadableParts(fullId, corrupted, corruptedPart);
    }

    void TScrubCoroImpl::UpdateUnreadableParts(const TLogoBlobID& fullId, NMatrix::TVectorType corrupted, TDiskPart corruptedPart) {
        Y_ABORT_UNLESS(!fullId.PartId());
        const auto it = UnreadableBlobs.find(fullId);

        const NMatrix::TVectorType prevCorrupted = it != UnreadableBlobs.end()
            ? it->second.UnreadableParts
            : NMatrix::TVectorType(0, corrupted.GetSize());

        if (prevCorrupted != corrupted) {
            const NMatrix::TVectorType becameOk = prevCorrupted & ~corrupted;
            const NMatrix::TVectorType becameCorrupted = corrupted & ~prevCorrupted;

            STLOGX(GetActorContext(), becameCorrupted.Empty() ? PRI_NOTICE : PRI_ERROR, BS_VDISK_SCRUB, VDS41,
                VDISKP(LogPrefix, "huge blob corrupted state updated"), (BlobId, fullId),
                (UnreadablePartsBefore, prevCorrupted), (UnreadablePartsAfter, corrupted),
                (BecameOk, becameOk), (BecameCorrupted, becameCorrupted), (CorruptedPart, corruptedPart));

            if (corrupted.Empty()) {
                UnreadableBlobs.erase(it);
            } else if (it != UnreadableBlobs.end()) {
                it->second.UnreadableParts = corrupted;
                it->second.CorruptedPart = corruptedPart;
                if (!becameCorrupted.Empty()) { // retry recovery ASAP -- new unreadable parts found
                    it->second.RecoveryInFlightCookie = 0;
                    it->second.RetryTimestamp = TInstant::Zero();
                }
            } else {
                UnreadableBlobs.try_emplace(fullId, corrupted, corruptedPart);
            }

            MonGroup.UnreadableBlobsFound() += corrupted.CountBits() - prevCorrupted.CountBits();
        }
    }

    void TScrubCoroImpl::UpdateReadableParts(const TLogoBlobID& fullId, NMatrix::TVectorType readable) {
        Y_ABORT_UNLESS(!fullId.PartId());
        if (const auto it = UnreadableBlobs.find(fullId); it != UnreadableBlobs.end()) {
            STLOGX(GetActorContext(), PRI_NOTICE, BS_VDISK_SCRUB, VDS42, VDISKP(LogPrefix,
                "read parts of previously unreadable blob"), (BlobId, it->first),
                (UnreadablePartsBefore, it->second.UnreadableParts), (ReadableParts, readable));
            MonGroup.UnreadableBlobsFound() -= (it->second.UnreadableParts & readable).CountBits();
            if ((it->second.UnreadableParts &= ~readable).Empty()) {
                UnreadableBlobs.erase(it);
            }
        }
    }

    ui64 TScrubCoroImpl::GenerateRestoreCorruptedBlobQuery() {
        const TInstant now = TActorCoroImpl::Now();
        std::vector<TEvRestoreCorruptedBlob::TItem> items;

        for (auto& [blobId, data] : UnreadableBlobs) {
            if (!data.RecoveryInFlightCookie && now >= data.RetryTimestamp) {
                data.RecoveryInFlightCookie = ++LastRestoreCookie;
                items.emplace_back(blobId, data.UnreadableParts, ScrubCtx->Info->Type, data.CorruptedPart,
                    data.RecoveryInFlightCookie);
                const auto& p = data;
                const auto& q = blobId;
                STLOG(PRI_INFO, BS_VDISK_SCRUB, VDS22, VDISKP(LogPrefix, "going to restore unreadable blob"),
                    (Cookie, p.RecoveryInFlightCookie), (BlobId, q), (UnreadableParts, p.UnreadableParts),
                    (CorruptedPart, p.CorruptedPart));
            }
        }

        if (!items.empty()) {
            const TInstant deadline = now + TDuration::Minutes(2);
            const ui64 cookie = ++LastRestoreCookie;
            Send(ScrubCtx->SkeletonId, new TEvRestoreCorruptedBlob(deadline, std::move(items), true, false), 0, cookie);
            return cookie;
        } else {
            return 0;
        }
    }

    void TScrubCoroImpl::Handle(TEvRestoreCorruptedBlobResult::TPtr ev) {
        const TInstant now = TActorCoroImpl::Now();

        for (const auto& item : ev->Get()->Items) {
            if (const auto it = UnreadableBlobs.find(item.BlobId); it != UnreadableBlobs.end()) {
                auto& data = it->second;

                if (!data.RecoveryInFlightCookie || data.RecoveryInFlightCookie != item.Cookie) {
                    continue;
                }
                data.RecoveryInFlightCookie = 0;
                data.RetryTimestamp = now + TDuration::Minutes(1);

                if (item.Status == NKikimrProto::OK) {
                    STLOG(PRI_NOTICE, BS_VDISK_SCRUB, VDS40, VDISKP(LogPrefix,
                        "recovered parts of previously unreadable blob"), (BlobId, it->first),
                        (UnreadablePartsBefore, data.UnreadableParts), (RecoveredParts, item.Needed));

                    MonGroup.UnreadableBlobsFound() -= (data.UnreadableParts & item.Needed).CountBits();
                    if ((data.UnreadableParts &= ~item.Needed).Empty()) {
                        UnreadableBlobs.erase(it);
                    }
                    ++MonGroup.BlobsFixed();
                    
                } else {
                    STLOG(PRI_WARN, BS_VDISK_SCRUB, VDS07, VDISKP(LogPrefix, "failed to restore corrupted blob"),
                        (BlobId, item.BlobId), (Status, item.Status));
                }
            }
        }

        if (UnreadableBlobs.empty()) { // all blobs found so far are repaired
            Send(ScrubCtx->SkeletonId, new TEvReportScrubStatus(false));
        } else if (!GenerateRestoreCorruptedBlobQueryScheduled) { // retry in some time
            TInstant when = TInstant::Max();
            for (const auto& [blobId, data] : UnreadableBlobs) {
                if (!data.RecoveryInFlightCookie) {
                    when = Min(when, data.RetryTimestamp);
                }
            }
            if (when != TInstant::Max()) {
                GetActorSystem()->Schedule(when, new IEventHandle(ScrubCtx->SkeletonId, SelfActorId,
                    new TEvTakeHullSnapshot(false)));
                GenerateRestoreCorruptedBlobQueryScheduled = true;
            }
        }

        LastReceivedRestoreCookie = Max(LastReceivedRestoreCookie, ev->Cookie);
    }

    void TScrubCoroImpl::Handle(TEvNonrestoredCorruptedBlobNotify::TPtr ev) {
        for (const auto& item : ev->Get()->Items) {
            AddUnreadableParts(item.BlobId, item.UnreadableParts, item.CorruptedPart);
        }
        GenerateRestoreCorruptedBlobQuery();
    }

    void TScrubCoroImpl::Handle(TEvTakeHullSnapshotResult::TPtr ev) {
        Y_ABORT_UNLESS(GenerateRestoreCorruptedBlobQueryScheduled);
        GenerateRestoreCorruptedBlobQueryScheduled = false;

        auto& snap = ev->Get()->Snap;
        auto barriers = snap.BarriersSnap.CreateEssence(snap.HullCtx);
        FilterUnreadableBlobs(snap, *barriers);

        GenerateRestoreCorruptedBlobQuery();
    }

    void TScrubCoroImpl::FilterUnreadableBlobs(THullDsSnap& snap, TBarriersSnapshot::TBarriersEssence& barriers) {
        TLevelIndexSnapshot::TForwardIterator iter(snap.HullCtx, &snap.LogoBlobsSnap);
        TIndexRecordMerger merger(Info->Type);

        for (auto it = UnreadableBlobs.begin(); it != UnreadableBlobs.end(); ) {
            const TLogoBlobID id = it->first;
            ++it;

            bool keepData = false;
            if (iter.Seek(id); iter.Valid() && iter.GetCurKey().LogoBlobID() == id) {
                iter.PutToMerger(&merger);
                merger.Finish();
                keepData = barriers.Keep(id, merger.GetMemRec(), {}, snap.HullCtx->AllowKeepFlags,
                    true /*allowGarbageCollection*/).KeepData;
                merger.Clear();
            }

            if (!keepData) {
                DropGarbageBlob(id);
            }
        }
    }

} // NKikimr
