#pragma once

#include "defs.h"

namespace NKikimr {
namespace NDataShard {

struct TDataShardId {
    ui64 TabletId;
    ui64 Generation;
    TActorId ActorId;
};

} // NDataShard
} // NKikimr
