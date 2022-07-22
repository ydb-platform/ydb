#pragma once

#include "defs.h"

namespace NKikimr::NDataShard {

NActors::IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

} // NKikimr::NDataShard
