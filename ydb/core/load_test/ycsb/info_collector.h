#pragma once

#include "defs.h"

namespace NKikimr::NDataShardLoad {

// Requests info from all actors, replies with accumulated result to parent
IActor *CreateInfoCollector(const TActorId& parent, TVector<TActorId>&& actors);

} // NKikimr::NDataShardLoad
