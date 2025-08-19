#pragma once

#include <shared_cache_s3fifo.h>
#include <shared_page.h>
#include <ydb/core/util/cache_cache_iface.h>

namespace NKikimr::NSharedCache::NTest {

    struct TPage : public TIntrusiveListItem<TPage> {
        ui32 Id;
        size_t Size;

        TPage(ui32 id, size_t size) 
            : Id(id), Size(size)
        {}
        
        ECacheMode CacheMode : 2 = ECacheMode::Regular;

        ES3FIFOPageLocation S3FIFOLocation : 4 = ES3FIFOPageLocation::None;
        ui32 S3FIFOFrequency : 4 = 0;
    };

    struct TPageTraits {
        struct TPageKey {
            ui32 Id;

            TPageKey(ui32 id)
                : Id(id)
            {}
        };
        
        static ui64 GetSize(const TPage* page) {
            return page->Size;
        }

        static TPageKey GetKey(const TPage* page) {
            return {page->Id};
        }

        static size_t GetHash(const TPageKey& key) {
            return key.Id;
        }

        static TString ToString(const TPageKey& key) {
            return std::to_string(key.Id);
        }

        static TString GetKeyToString(const TPage* page) {
            return ToString(GetKey(page));
        }

        static ES3FIFOPageLocation GetLocation(const TPage* page) {
            return page->S3FIFOLocation;
        }

        static void SetLocation(TPage* page, ES3FIFOPageLocation location) {
            page->S3FIFOLocation = location;
        }

        static ui32 GetFrequency(const TPage* page) {
            return page->S3FIFOFrequency;
        }

        static void SetFrequency(TPage* page, ui32 frequency) {
            Y_ENSURE(frequency < (1 << 4));
            page->S3FIFOFrequency = frequency;
        }

        static ui32 GetTier(TPage* page) {
            return static_cast<ui32>(page->CacheMode);
        }
    };

} // namespace NKikimr::NSharedCache::NTest
