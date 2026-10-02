#pragma once

#include <shared_cache_s3fifo.h>
#include <shared_page.h>

namespace NKikimr::NSharedCache::NTest {

    struct TPage : public TIntrusiveListItem<TPage> {
        ui32 Id;
        size_t Size;

        TPage(ui32 id, size_t size) 
            : Id(id), Size(size)
        {}
        
        ui32 GetFrequency() const noexcept {
            return Frequency.load(std::memory_order_relaxed);
        }

        void IncrementFrequency() noexcept {
            ui32 value = Frequency.load(std::memory_order_relaxed);
            if (value < 3) { // S3FIFO frequency is capped to 3
                Frequency.compare_exchange_weak(value, value + 1,
                    std::memory_order_acq_rel, std::memory_order_relaxed);
            }
        }
    
        void SetFrequency(ui32 frequency) noexcept {
            Frequency.store(frequency, std::memory_order_release);
        }

        ECacheMode CacheMode : 2 = ECacheMode::Regular;

        ES3FIFOPageLocation Location : 4 = ES3FIFOPageLocation::None;
        std::atomic<ui32> Frequency;
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
            return page->Location;
        }

        static void SetLocation(TPage* page, ES3FIFOPageLocation location) {
            page->Location = location;
        }

        static ui32 GetFrequency(const TPage* page) {
            return page->GetFrequency();
        }
    
        static void SetFrequency(TPage* page, ui32 frequency) {
            page->SetFrequency(frequency);
        }

        static ui32 GetTier(TPage* page) {
            return static_cast<ui32>(page->CacheMode);
        }
    };

} // namespace NKikimr::NSharedCache::NTest
