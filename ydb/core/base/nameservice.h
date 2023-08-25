#pragma once

#include "defs.h"

namespace NKikimr {

struct TDynamicNameserviceConfig : public TThrRefBase {
    ui32 MaxStaticNodeId;
    ui32 MaxDynamicNodeId;
    ui32 MinDynamicNodeId;
};

} // NKikimr
