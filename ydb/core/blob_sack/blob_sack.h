#pragma once

#include "defs.h"

namespace NKikimr::NBlobSack {

    IActor *CreateBlobSack(const TActorId& tablet, TTabletStorageInfo *info);

} // NKikimr::NBlobSack
