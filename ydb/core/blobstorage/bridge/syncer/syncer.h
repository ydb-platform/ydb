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

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TGroupId sourceGroupId, TGroupId targetGroupId,
        std::shared_ptr<TSyncerDataStats> syncerDataStats, TReplQuoter::TPtr syncRateQuoter,
        TBlobStorageGroupType sourceGroupType);

} // NKikimr::NBridge
