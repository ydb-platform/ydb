#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    // bridge proxy of the group which has every pile served by the pile group's own DS proxy
    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info);

    // bridge proxy of the inline pile group; innerProxyId is the DS proxy serving the inline pile of this group, bridge
    // proxy takes ownership over the inner proxy and keeps its configuration up to date
    IActor *CreateInlinePileBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId innerProxyId);

} // NKikimr
