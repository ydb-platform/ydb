#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogdata.h>

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////
// PHANTOM FLAG STORAGE BUILDER CREATOR
// Creates the actor that asynchronously reads snapshot
////////////////////////////////////////////////////////////////////////////
NActors::IActor* CreatePhantomFlagBuilderActor(TIntrusivePtr<TSyncLogCtx> slCtx,
        TSyncLogSnapshotPtr snapshot);

}

} // namespace NKikimr
