#pragma once

#include "defs.h"
#include "flat_sausage_packet.h"
#include "flat_sausage_fetch.h"

namespace NKikimr {
namespace NTable {

    struct TPageCollectionComponents {
        // fully identified by this LargeGlobId
        NPageCollection::TLargeGlobId LargeGlobId;
        // loaded meta page
        TIntrusiveConstPtr<NPageCollection::TPageCollection> Packet;
        TVector<NPageCollection::TLoadedPage> Sticky;

        void ParsePacket(TSharedData meta);
    };

    struct TPartComponents {
        explicit operator bool() const
        {
            return bool(PageCollectionComponents);
        }

        // All required data for part cooking
        TVector<TPageCollectionComponents> PageCollectionComponents;
        // Legacy overlay for TPart, use Opaque
        TString Legacy;
        // Opaque app. defined bundle overlay
        TString Opaque;
        // Optional underlying part epoch
        TEpoch Epoch;

        TEpoch GetEpoch() const;
    };

}
}
