#pragma once
#include "defs.h"

namespace NKikimr {

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr
