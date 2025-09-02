#pragma once

#include "flat_dbase_scheme.h"
#include "flat_sausage_solid.h"
#include "flat_sausage_grind.h"
#include "flat_page_iface.h"
#include "util_channel.h"

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

        TVector<ui8> BlobsChannels = {NTable::TScheme::DefaultChannel};   /* Channel for external blobs */
        ui8 OuterChannel = NTable::TScheme::DefaultChannel;   /* Channel for packed cell values */
        ui8 ExtraChannel = NTable::TScheme::DefaultChannel;   /* Channel for storing additional data */
        NUtil::TChannelsShares ChannelsShares; /* Channels shares for data distribution */
        TVector<TGroup> Groups; /* Per-group page collection settings */
        TVector<TSlot> Slots;   /* Storage slots, referred by rooms */
        bool StickyFlatIndex = true;
    };

}
}
}
