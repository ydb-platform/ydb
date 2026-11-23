#pragma once

#include "defs.h"

namespace NKikimr {

class TVDiskContext;

IActor* CreateBurstCounterPublisher(TIntrusivePtr<TVDiskContext> vctx, TDuration interval);

} // namespace NKikimr
