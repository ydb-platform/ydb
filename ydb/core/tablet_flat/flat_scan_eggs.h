#pragma once

#include "flat_table_subset.h"
#include "flat_scan_iface.h"
#include "flat_executor_misc.h"
#include "util_basics.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    namespace NOps {

        struct TConf {
            ui64 AheadHi = 0;
            ui64 AheadLo = 0;
            bool Trace = false; /* Record used by ref (seen) blobs  */
            ui64 Tablet = 0;    /* Allow blobs only for this tablet */
            NBlockIO::EPriority ReadPrio = NBlockIO::EPriority::Bulk;
        };

    }

    struct TScanSnapshot {
        const ui32 Table;
        TIntrusivePtr<TBarrier> Barrier;
        TAutoPtr<NTable::TSubset> Subset;
        TRowVersion Snapshot;
    };

}
}
