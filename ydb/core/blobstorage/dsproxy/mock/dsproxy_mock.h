#pragma once

#include "defs.h"
#include <ydb/core/base/id_wrapper.h>
namespace NKikimr {

    namespace NFake {
        class TProxyDS;
    } // NFake

    IActor *CreateBlobStorageGroupProxyMockActor(TIntrusivePtr<NFake::TProxyDS> model);
    IActor *CreateBlobStorageGroupProxyMockActor(TIdWrapper<ui32, TGroupIdTag> groupId);

} // NKikimr
