#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    struct TCGSI {
        ui32 Channel;
        ui32 Generation;
        ui32 Step;
        ui32 Index;
    };

} // NKikimr::NBlobDepot
