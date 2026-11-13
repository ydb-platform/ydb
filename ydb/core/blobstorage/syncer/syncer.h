#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events_quoter.h>

namespace NKikimr::NGroupSyncer {

    struct TSyncerDataStats {
        std::atomic_uint64_t BytesTotal = 0;
        std::atomic_uint64_t BytesDone = 0;
        std::atomic_uint64_t BytesError = 0;
        std::atomic_uint64_t BlobsTotal = 0;
        std::atomic_uint64_t BlobsDone = 0;
        std::atomic_uint64_t BlobsError = 0;

        // Bytes of the source blobs the target doesn't have, found by the blob scan of this stage; the scan of a
        // stage before the data one tells how much the data stage is going to copy, as data doesn't get to the target
        // until then. Blob sizes are counted, not the sizes of their parts.
        std::atomic_uint64_t BytesToCopy = 0;
        // bytes of the blobs counted in BytesToCopy that need no copying any more: written to the target, or gone from
        // the source
        std::atomic_uint64_t BytesCopied = 0;
        // the scan has gone over all the blobs, so BytesToCopy is not going to grow any more, unless the target loses
        // a blob it has been found to have
        std::atomic_bool ScanFinished = false;
    };

    struct TEvMirror3dcRepairScanResult
        : TEventLocal<TEvMirror3dcRepairScanResult, TEvBlobStorage::EvMirror3dcRepairScanResult>
    {
        NKikimrProto::EReplyStatus Status;

        explicit TEvMirror3dcRepairScanResult(NKikimrProto::EReplyStatus status)
            : Status(status)
        {}
    };

    // Run one pass over the observed placements at this generation.
    IActor* CreateMirror3dcRepairSyncer(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId notifyId);

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TGroupId sourceGroupId, TGroupId targetGroupId,
        std::shared_ptr<TSyncerDataStats> syncerDataStats, TReplQuoter::TPtr syncRateQuoter);

} // NKikimr::NGroupSyncer
