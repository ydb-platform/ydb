#pragma once

#include "defs.h"

namespace NKikimr {

struct TDynamicNameserviceConfig : public TThrRefBase {
    ui32 MaxStaticNodeId;
    ui32 MaxDynamicNodeId;
};

} // NKikimr
