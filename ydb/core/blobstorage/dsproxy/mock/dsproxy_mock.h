#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    namespace NFake {
        class TProxyDS;
    } // NFake

    IActor *CreateBlobStorageGroupProxyMockActor(TIntrusivePtr<NFake::TProxyDS> model);
    IActor *CreateBlobStorageGroupProxyMockActor(TGroupId groupId);

    using TBSFailureInjectionConfig = NKikimrBlobStorage::TNodeWardenServiceSet::TFailureInjectionConfig;
    IActor *CreateBlobStorageGroupFailureInjectingActor(TActorId actorId, TGroupId groupId, const TBSFailureInjectionConfig& config);

} // NKikimr
