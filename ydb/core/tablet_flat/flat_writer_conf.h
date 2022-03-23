#pragma once

#include "flat_sausage_solid.h"
#include "flat_sausage_grind.h"
#include "flat_page_iface.h"

#include <util/system/types.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NWriter {

    struct TConf {
        using ECache = NTable::NPage::ECache;
        using TSlot = NPageCollection::TSlot;

        TConf() {
            // Always include at least 1 group
            Groups.emplace_back();
        }

        struct TGroup {
            ui8 Channel = 1;              /* Data channel for page collection */
            ECache Cache = ECache::None;  /* Keep data pages in memory */
            ui32 MaxBlobSize = 8 * 1024 * 1024; /* Page collection max blob size */
        };

        ui8 BlobsChannel = 1;   /* Channel for external blobs */
        ui8 OuterChannel = 1;   /* Channel for packed cell values */
        ui8 ExtraChannel = 1;   /* Channel for storing additional data */
        TVector<TGroup> Groups; /* Per-group page collection settings */
        TVector<TSlot> Slots;   /* Storage slots, referred by rooms */
    };

}
}
}
