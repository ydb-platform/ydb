#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    // innerProxyId is the DS proxy serving inline pile of this group (when the group is an inline pile one); bridge proxy
    // takes ownership over the inner proxy and keeps its configuration up to date
    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId innerProxyId = {});

} // NKikimr
