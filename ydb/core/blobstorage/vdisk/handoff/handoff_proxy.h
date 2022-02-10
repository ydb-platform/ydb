#pragma once

#include "defs.h"
#include "handoff_basic.h"

namespace NKikimr {

    class TBlobStorageGroupInfo;
    struct THandoffParams;
    IActor *CreateHandoffProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info,
                           NHandoff::TProxiesPtr proxiesPtr,
                           NHandoff::TVDiskInfo *vdInfoPtr,
                           const THandoffParams &params);

} // NKikimr

