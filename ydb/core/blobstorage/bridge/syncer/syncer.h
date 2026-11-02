#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events_quoter.h>

namespace NKikimr::NBridge {

    struct TSyncerDataStats {
        std::atomic_uint64_t BytesTotal = 0;
        std::atomic_uint64_t BytesDone = 0;
        std::atomic_uint64_t BytesError = 0;
        std::atomic_uint64_t BlobsTotal = 0;
        std::atomic_uint64_t BlobsDone = 0;
        std::atomic_uint64_t BlobsError = 0;
    };

    // with reconcile set, the syncer brings the part of HOT_STANDBY pile up to its source without changing the stage of
    // the part: one pass syncs blocks, keep flags and the blobs the part misses (every blob when thorough), and then the
    // barriers of the source
    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TGroupId sourceGroupId, TGroupId targetGroupId,
        std::shared_ptr<TSyncerDataStats> syncerDataStats, TReplQuoter::TPtr syncRateQuoter,
        TBlobStorageGroupType sourceGroupType, bool reconcile = false, bool thorough = false);

} // NKikimr::NBridge
