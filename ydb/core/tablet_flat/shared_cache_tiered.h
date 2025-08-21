#pragma once

#include "shared_cache_s3fifo.h"

namespace NKikimr::NSharedCache {

    template <typename TPage, typename TPageTraits>
    class TTieredCache {
        using TCache = TS3FIFOCache<TPage, TPageTraits>;

    public:
        TTieredCache(ui64 limit)
            : CacheTiers(::Reserve(2))
        {
            CacheTiers.emplace_back(limit);
            RegularTier = &CacheTiers.back();

            CacheTiers.emplace_back(0);
            TryKeepInMemoryTier = &CacheTiers.back();
        }

        TPage* EvictNext() Y_WARN_UNUSED_RESULT {
            if (TPage* evictedPage = RegularTier->EvictNext(); evictedPage) {
                return evictedPage;
            } else {
                return TryKeepInMemoryTier->EvictNext();
            }
        }

        TIntrusiveList<TPage> Insert(TPage *page) Y_WARN_UNUSED_RESULT {
            ui32 tier = TPageTraits::GetTier(page);
            return CacheTiers[tier].Insert(page);
        }

        void Erase(TPage *page) {
            ui32 tier = TPageTraits::GetTier(page);
            CacheTiers[tier].Erase(page);
        }

        void UpdateLimit(ui64 limit, ui64 tryKeepInMemoryBytes) {
            ui64 tryKeepInMemoryLimit = Min(limit, tryKeepInMemoryBytes);
            RegularTier->UpdateLimit(limit - tryKeepInMemoryLimit);
            TryKeepInMemoryTier->UpdateLimit(tryKeepInMemoryLimit);
        }

        ui64 GetLimit() const {
            return RegularTier->GetLimit() + TryKeepInMemoryTier->GetLimit();
        }

        ui64 GetSize() const {
            return RegularTier->GetSize() + TryKeepInMemoryTier->GetSize();
        }

        TString Dump() const {
            TStringBuilder result;
            bool first = true;

            for (size_t i = 0; i < CacheTiers.size(); ++i) {
                if (first) {
                    first = false;
                } else {
                    result << "; ";
                }
                result << static_cast<NTable::NPage::ECacheMode>(i) << "Tier: " << CacheTiers[i].Dump();
            }
        
            return result;
        }

        struct TStats {
            ui64 RegularBytes;
            ui64 TryKeepInMemoryBytes;
            ui64 RegularLimit;
            ui64 TryKeepInMemoryLimit;
        };

        // for ut
        TStats GetStats() const {
            return {
                .RegularBytes = RegularTier->GetSize(),
                .TryKeepInMemoryBytes = TryKeepInMemoryTier->GetSize(),
                .RegularLimit = RegularTier->GetLimit(),
                .TryKeepInMemoryLimit = TryKeepInMemoryTier->GetLimit()
            };
        }

    private:
        TVector<TCache> CacheTiers;
        TCache* RegularTier;
        TCache* TryKeepInMemoryTier;
    };

} // namespace NKikimr::NSharedCache
