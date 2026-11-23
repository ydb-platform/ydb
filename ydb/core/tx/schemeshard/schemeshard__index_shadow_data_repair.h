#pragma once

#include "schemeshard_identificators.h"

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

enum class EIndexShadowDataRepairStatus {
    Ready,
    Deferred,
    Done,
    Invalid,
};

struct TIndexShadowDataRepairCheck {
    EIndexShadowDataRepairStatus Status;
    TString Reason;
};

TIndexShadowDataRepairCheck CheckIndexShadowDataRepair(TSchemeShard* ss, TPathId pathId);

} // namespace NKikimr::NSchemeShard
