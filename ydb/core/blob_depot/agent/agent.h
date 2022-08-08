#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepotAgent(ui32 groupId, TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId proxyId);

} // NKikimr::NBlobDepot
