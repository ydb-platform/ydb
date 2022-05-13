#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepot(const TActorId& tablet, TTabletStorageInfo *info);

} // NKikimr::NBlobDepot
