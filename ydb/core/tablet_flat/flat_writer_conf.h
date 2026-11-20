#pragma once

#include "flat_dbase_scheme.h"
#include "flat_page_conf.h"
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
        using ECacheMode = NTable::NPage::ECacheMode;
        using TSlot = NPageCollection::TSlot;

        TConf() {
            // Always include at least 1 group
            Groups.emplace_back();
        }

        struct TGroup {
            ui8 Channel = 1;              /* Data channel for page collection */
            ECache Cache = ECache::None;  /* Keep data pages in memory */
            ECacheMode CacheMode = ECacheMode::Regular;
            ui32 MaxBlobSize = 8 * 1024 * 1024; /* Page collection max blob size */
        };

        TVector<ui8> BlobsChannels = {NTable::TScheme::DefaultChannel};   /* Channel for external blobs */
        ui8 OuterChannel = NTable::TScheme::DefaultChannel;   /* Channel for packed cell values */
        ui8 ExtraChannel = NTable::TScheme::DefaultChannel;   /* Channel for storing additional data */
        NUtil::TChannelsShares ChannelsShares; /* Channels shares for data distribution */
        TVector<TGroup> Groups; /* Per page collection settings, by slot of the part (see TPartSlots), except outer */
        TVector<TSlot> Slots;   /* Storage slots, referred by rooms */
        bool StickyFlatIndex = true;
        bool WriteBTreeIndexV1 = true;
        bool WriteBTreeIndexV2 = false;
    };

    /**
     * Descriptor pages are written to the main channel of the leader family room. A separate descriptor
     * collection requires a B-tree index and is used when the main group goes to another channel or
     * an index goes into its group's data collection. Returns true if a collection was added.
     */
    inline bool ApplyDescriptorChannel(NTable::NPage::TConf& layout, TConf& writer, ui8 descriptorChannel) {
        Y_ENSURE(!writer.Groups.empty());

        const bool writeBTreeIndex = layout.WriteBTreeIndexV1 || layout.WriteBTreeIndexV2;
        const bool indexesInDescriptor = layout.Layout.KeyIndexInDescriptor && layout.Layout.GroupIndexesInDescriptor;
        if (!writeBTreeIndex || (descriptorChannel == writer.Groups[0].Channel && indexesInDescriptor)) {
            return false;
        }

        layout.Layout.SeparateDescriptorCollection = true;
        layout.WriteFlatIndex = false;

        auto descriptor = writer.Groups[0];
        descriptor.Channel = descriptorChannel;
        writer.Groups.insert(writer.Groups.begin(), descriptor);
        return true;
    }

}
}
}
