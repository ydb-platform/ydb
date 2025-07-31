#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NBridge {

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TBridgePileId targetPileId,
        TGroupId groupId);

} // NKikimr::NBridge
