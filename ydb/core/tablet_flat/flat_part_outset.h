#pragma once

#include "defs.h"
#include "flat_sausage_packet.h"
#include "flat_sausage_fetch.h"

namespace NKikimr {
namespace NTable {

    struct TPageCollectionComponents {
        // fully identified by this LargeGlobId
        NPageCollection::TLargeGlobId LargeGlobId;
        // Optional pre-populated pages (compaction path only)
        TVector<NPageCollection::TLoadedPage> RegularPages;
        TVector<NPageCollection::TLoadedPage> StickyPages;
        // Parsed metadata, replaced with the shared-cache instance before building the part.
        TAtomicSharedPtr<const NPageCollection::TMeta> SharedMeta;

        void SetMeta(TSharedData raw);
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
        // Optional deltas to apply after loading
        TVector<TString> Deltas;
        // Optional underlying part epoch
        TEpoch Epoch;

        TEpoch GetEpoch() const;
    };

}
}
