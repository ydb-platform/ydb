#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage_common.h>
namespace NKikimr {

    namespace NFake {
        class TProxyDS;
    } // NFake

    IActor *CreateBlobStorageGroupProxyMockActor(TIntrusivePtr<NFake::TProxyDS> model);
    IActor *CreateBlobStorageGroupProxyMockActor(TGroupId groupId);

} // NKikimr
