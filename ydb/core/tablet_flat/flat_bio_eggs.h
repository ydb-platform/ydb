#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBlockIO {

    constexpr static ui64 BlockSize = 8192 * 1024;

    enum class EPriority {
        None    = 0,
        Fast    = 1,    /* Low latency activity             */
        Bulk    = 2,    /* Huge bulk IO reads like scan ops */
        Bkgr    = 4,    /* System background read activity  */
        Low     = 5,    /* Low priority read activity       */
    };

}
}
}
