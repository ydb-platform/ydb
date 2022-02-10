#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NRedo {

    struct TUsage {
        ui32 Table;
        ui64 Items;
        ui64 Bytes;
    };

    struct TStats {
        ui64 Items;
        ui64 Memory;    /* Memory consumed by entry states  */
        ui64 LargeGlobIds;    /* Storage space consumed by redo   */
    };

}
}
}
