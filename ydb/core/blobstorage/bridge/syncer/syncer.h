#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NBridge {

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TGroupId sourceGroupId, TGroupId targetGroupId);

} // NKikimr::NBridge
