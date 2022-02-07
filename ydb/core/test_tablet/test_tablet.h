#pragma once

#include "defs.h"

namespace NKikimr::NTestShard {

    IActor *CreateTestShard(const TActorId& tablet, TTabletStorageInfo *info);

} // NKikimr::NTestShard
