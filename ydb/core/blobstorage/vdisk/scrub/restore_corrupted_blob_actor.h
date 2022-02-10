#pragma once

#include "defs.h"
#include "blob_recovery.h"

namespace NKikimr {

    struct TEvRestoreCorruptedBlob : TEventLocal<TEvRestoreCorruptedBlob, TEvBlobStorage::EvRestoreCorruptedBlob> {
        using TItem = TEvRecoverBlob::TItem;
        TInstant Deadline;
        std::vector<TItem> Items;
        bool WriteRestoredParts; // if true, then all restored parts are written back to VDisk
        bool ReportNonrestoredParts; // if true, nonrestored parts are reported to scrub actor for scrubbing

        TEvRestoreCorruptedBlob(TInstant deadline, std::vector<TItem>&& items, bool writeRestoredParts, bool reportNonrestoredParts)
            : Deadline(deadline)
            , Items(std::move(items))
            , WriteRestoredParts(writeRestoredParts)
            , ReportNonrestoredParts(reportNonrestoredParts)
        {}
    };

    struct TEvRestoreCorruptedBlobResult : TEventLocal<TEvRestoreCorruptedBlobResult, TEvBlobStorage::EvRestoreCorruptedBlobResult> {
        using TItem = TEvRecoverBlobResult::TItem;
        std::vector<TItem> Items; // status is either OK, ERROR, or DEADLINE

        TEvRestoreCorruptedBlobResult(std::vector<TItem>&& items)
            : Items(std::move(items))
        {}
    };

    struct TEvNonrestoredCorruptedBlobNotify : TEventLocal<TEvNonrestoredCorruptedBlobNotify, TEvBlobStorage::EvNonrestoredCorruptedBlobNotify> {
        struct TItem {
            TLogoBlobID BlobId;
            NMatrix::TVectorType UnreadableParts;
            TDiskPart CorruptedPart;

            TItem(const TLogoBlobID& blobId, NMatrix::TVectorType unreadableParts, TDiskPart corruptedPart)
                : BlobId(blobId)
                , UnreadableParts(unreadableParts)
                , CorruptedPart(corruptedPart)
            {}
        };
        std::vector<TItem> Items;

        TEvNonrestoredCorruptedBlobNotify(std::vector<TItem>&& items)
            : Items(std::move(items))
        {}
    };

    IActor *CreateRestoreCorruptedBlobActor(TActorId skeletonId, TEvRestoreCorruptedBlob::TPtr& ev,
        TIntrusivePtr<TBlobStorageGroupInfo> info, TIntrusivePtr<TVDiskContext> vctx,
        TPDiskCtxPtr pdiskCtx);

} // NKikimr
