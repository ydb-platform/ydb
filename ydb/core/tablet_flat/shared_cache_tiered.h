#pragma once

#include "shared_cache_counters.h"
#include "shared_cache_switchable.h"
#include "shared_cache_tiers.h"

namespace NKikimr::NSharedCache {

    template <typename TPage, typename TPageTraits>
    class TTieredCache {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;
        using TCache = TSwitchableCache<TPage, TPageTraits>;

    public:
        template <typename TCacheBuilder>
        TTieredCache(ui64 limit, TCacheBuilder createCache, TReplacementPolicy policy, TSharedPageCacheCounters& cacheCounters)
            : CacheTiers(::Reserve(2))
        {
            CacheTiers.emplace_back(limit, createCache(), cacheCounters.ReplacementPolicySize(policy));
            RegularTier = &CacheTiers.back();

            CacheTiers.emplace_back(0, createCache(), cacheCounters.ReplacementPolicySize(policy));
            TryInMemoryTier = &CacheTiers.back();
        }

        template <typename TCacheBuilder>
        TIntrusiveList<TPage> Switch(TCacheBuilder createCache, TCounterPtr sizeCounter) Y_WARN_UNUSED_RESULT {
            TIntrusiveList<TPage> evictedList;

            for (auto& cacheTier : CacheTiers) {
                evictedList.Append(cacheTier.Switch(createCache(), sizeCounter));
            }

            return evictedList;
        }

        TIntrusiveList<TPage> EvictNext() Y_WARN_UNUSED_RESULT {
            if (auto evicted = RegularTier->EvictNext(); evicted) {
                return std::move(evicted);
            } else {
                return TryInMemoryTier->EvictNext();
            }
        }

        TIntrusiveList<TPage> Touch(TPage *page) Y_WARN_UNUSED_RESULT {
            ECacheTier tier = TPageTraits::GetTier(page);
            return CacheTiers[static_cast<size_t>(tier)].Touch(page);
        }

        void Erase(TPage *page) {
            ECacheTier tier = TPageTraits::GetTier(page);
            CacheTiers[static_cast<size_t>(tier)].Erase(page);
        }

        void UpdateLimit(ui64 limit, ui64 tryKeepInMemoryBytes) {
            ui64 tryKeepInMemoryLimit = Min(limit, tryKeepInMemoryBytes);
            RegularTier->UpdateLimit(limit - tryKeepInMemoryLimit);
            TryInMemoryTier->UpdateLimit(tryKeepInMemoryLimit);
        }

        ui64 GetSize() const {
            ui64 result = 0;
            for (const auto& cacheTier : CacheTiers) {
                result += cacheTier.GetSize();
            }
            return result;
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
                result << static_cast<ECacheTier>(i) << "Tier: " << CacheTiers[i].Dump();
            }
        
            return result;
        }

    private:
        TVector<TCache> CacheTiers;
        TCache* RegularTier;
        TCache* TryInMemoryTier;
    };

} // namespace NKikimr::NSharedCache
