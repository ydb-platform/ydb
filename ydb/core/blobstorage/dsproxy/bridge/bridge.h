#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info);

} // NKikimr
