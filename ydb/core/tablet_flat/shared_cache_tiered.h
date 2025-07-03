#pragma once

#include "shared_cache_counters.h"
#include "shared_cache_switchable.h"
#include "shared_cache_tiers.h"

namespace NKikimr::NSharedCache {

    template <typename TPage, typename TPageTraits>
    class TTieredCache {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

        struct TCacheTier {
            template <typename TCacheBuilder>
            TCacheTier(ui64 limit, TCacheBuilder createCache, ECacheTier tier, TReplacementPolicy policy, TSharedPageCacheCounters& cacheCounters)
                : Cache(TSwitchableCache<TPage, TPageTraits>(limit, createCache(), cacheCounters.ReplacementPolicySize(policy)))
                , LimitBytesCounter(cacheCounters.LimitBytesTier(tier))
            {
                LimitBytesCounter->Set(limit);
            }

            void UpdateLimit(ui64 limit) {
                Cache.UpdateLimit(limit);
                LimitBytesCounter->Set(limit);
            }

            TSwitchableCache<TPage, TPageTraits> Cache;
            TCounterPtr LimitBytesCounter;
        };

    public:
        template <typename TCacheBuilder>
        TTieredCache(ui64 limit, TCacheBuilder createCache, TReplacementPolicy policy, TSharedPageCacheCounters& cacheCounters)
            : CacheTiers(::Reserve(2))
        {
            CacheTiers.emplace_back(limit, createCache, ECacheTier::Regular, policy, cacheCounters);
            RegularTier = &CacheTiers.back();

            CacheTiers.emplace_back(0, createCache, ECacheTier::TryKeepInMemory, policy, cacheCounters);
            TryInMemoryTier = &CacheTiers.back();
        }

        template <typename TCacheBuilder>
        TIntrusiveList<TPage> Switch(TCacheBuilder createCache, TCounterPtr sizeCounter) Y_WARN_UNUSED_RESULT {
            TIntrusiveList<TPage> evictedList;

            for (auto& cacheTier : CacheTiers) {
                evictedList.Append(cacheTier.Cache.Switch(createCache(), sizeCounter));
            }

            return evictedList;
        }

        TIntrusiveList<TPage> EvictNext() Y_WARN_UNUSED_RESULT {
            if (auto evicted = RegularTier->Cache.EvictNext(); evicted) {
                return std::move(evicted);
            } else {
                return TryInMemoryTier->Cache.EvictNext();
            }
        }

        TIntrusiveList<TPage> Touch(TPage *page) Y_WARN_UNUSED_RESULT {
            ECacheTier tier = TPageTraits::GetTier(page);
            return CacheTiers[static_cast<size_t>(tier)].Cache.Touch(page);
        }

        void Erase(TPage *page) {
            ECacheTier tier = TPageTraits::GetTier(page);
            CacheTiers[static_cast<size_t>(tier)].Cache.Erase(page);
        }

        void UpdateLimit(ui64 limit, ui64 tryKeepInMemoryBytes) {
            ui64 tryKeepInMemoryLimit = Min(limit, tryKeepInMemoryBytes);
            RegularTier->UpdateLimit(limit - tryKeepInMemoryLimit);
            TryInMemoryTier->UpdateLimit(tryKeepInMemoryLimit);
        }

        ui64 GetSize() const {
            ui64 result = 0;
            for (const auto& cacheTier : CacheTiers) {
                result += cacheTier.Cache.GetSize();
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
                result << static_cast<ECacheTier>(i) << "Tier: " << CacheTiers[i].Cache.Dump();
            }
        
            return result;
        }

    private:
        TVector<TCacheTier> CacheTiers;
        TCacheTier* RegularTier;
        TCacheTier* TryInMemoryTier;
    };

} // namespace NKikimr::NSharedCache
