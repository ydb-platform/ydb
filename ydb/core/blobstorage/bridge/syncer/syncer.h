#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NStorage::NBridge {

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TBridgePileId targetPileId);

} // NKikimr::NStorage::NBridge
