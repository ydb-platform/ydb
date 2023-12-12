#pragma once

#include "defs.h"


namespace NKikimr {
    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx);
} // NKikimr
