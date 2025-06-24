#pragma once

#include "shared_cache_counters.h"
#include "shared_cache_switchable.h"

namespace NKikimr::NSharedCache {

    template <typename TPage, typename TPageTraits>
    class TTieredCache {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

        static constexpr ui32 MaxTiersCount = 3;
        static_assert(MaxTiersCount < (1 << 2));
        static constexpr ui32 DefaultTier = 1;

        struct TCacheHolder {
            TSwitchableCache<TPage, TPageTraits> Cache;
            TCounterPtr ActivePages;
            TCounterPtr ActiveBytes;
        };

    public:
        template <typename TCacheBuilder>
        TTieredCache(ui64 limit, TCacheBuilder createCache, ui32 numberOfTiers, TReplacementPolicy policy, TSharedPageCacheCounters& cacheCounters)
            : CacheTiers(::Reserve(numberOfTiers))
        {
            Y_ENSURE(numberOfTiers > 0 && numberOfTiers <= MaxTiersCount);
            TCounterPtr sizeCounter = cacheCounters.ReplacementPolicySize(policy);
            for (ui32 tier = 1; tier <= numberOfTiers; ++tier) {
                ui64 tierLimit = tier == DefaultTier ? limit : 0;
                CacheTiers.emplace_back(
                    TSwitchableCache<TPage, TPageTraits>(tierLimit, createCache(), sizeCounter),
                    cacheCounters.ActivePagesTier(tier),
                    cacheCounters.ActiveBytesTier(tier)
                );
            }
        }

        // move if page in cache
        TIntrusiveList<TPage> Move(TPage *page, ui32 targetTier) {
            ui32 sourceTier = TPageTraits::GetTier(page);
            Y_ENSURE(sourceTier <= CacheTiers.size());
            Y_ENSURE(targetTier > 0 && targetTier <= CacheTiers.size());

            if (sourceTier == 0 || sourceTier == targetTier) {
                // nothing to move
                return {};
            }

            return DoMove(page, sourceTier, targetTier);
        }

        template <typename TCacheBuilder>
        TIntrusiveList<TPage> Switch(TCacheBuilder createCache, TCounterPtr sizeCounter) Y_WARN_UNUSED_RESULT {
            TIntrusiveList<TPage> evictedList;

            for (ui32 tier = 1; tier <= CacheTiers.size(); ++tier) {
                evictedList.Append(ProcessEvicted(GetTierCache(tier).Cache.Switch(createCache(), sizeCounter), tier));
            }

            return evictedList;
        }

        TIntrusiveList<TPage> EvictNext() {
            for (ui32 tier = 1; tier <= CacheTiers.size(); ++tier) {
                auto& cache = GetTierCache(tier);
                if (auto evicted = cache.Cache.EvictNext(); !evicted.Empty()) {
                    return ProcessEvicted(std::move(evicted), tier);
                }
            }
            return {};
        }

        TIntrusiveList<TPage> Touch(TPage *page, ui32 targetTier) {
            ui32 tier = TPageTraits::GetTier(page);
            Y_ENSURE(tier <= CacheTiers.size());
            Y_ENSURE(targetTier > 0 && targetTier <= CacheTiers.size());

            if (tier == targetTier) {
                return ProcessEvicted(GetTierCache(tier).Cache.Touch(page), tier);
            }

            return DoMove(page, tier, targetTier);
        }

        void Erase(TPage *page) {
            ui32 tier = TPageTraits::GetTier(page);
            Y_ENSURE(tier <= CacheTiers.size());
            if (tier > 0) {
                auto& cache = GetTierCache(tier);
                if (tier != DefaultTier) {
                    UpdateLimits(page, cache, GetTierCache(DefaultTier));
                }
                cache.ActivePages->Dec();
                cache.ActiveBytes->Sub(TPageTraits::GetSize(page));
                TPageTraits::SetTier(page, 0);
                cache.Cache.Erase(page);
            }
        }

        void UpdateLimit(ui64 limit) {
            for (ui32 tier = CacheTiers.size(); tier > DefaultTier; --tier) {
                auto& cache = GetTierCache(tier);
                ui64 currentTierLimit = Min(cache.Cache.GetSize(), limit);
                cache.Cache.UpdateLimit(currentTierLimit);
                limit -= currentTierLimit;
            }
            GetTierCache(DefaultTier).Cache.UpdateLimit(limit); // set all remaining limit for default tier
        }

        ui64 GetLimit() const {
            ui64 result = 0;
            for (const auto& cacheTier : CacheTiers) {
                result += cacheTier.Cache.GetLimit();
            }
            return result;
        }

        ui64 GetSize() const {
            ui64 result = 0;
            for (const auto& cacheTier : CacheTiers) {
                result += cacheTier.Cache.GetSize();
            }
            return result;
        }

        virtual TString Dump() const {
            TStringBuilder result;
            bool first = true;

            for (ui32 tierIndex = 0; tierIndex < CacheTiers.size(); ++tierIndex) {
                if (first) {
                    first = false;
                } else {
                    result << "; ";
                }
                result << "Tier " << (tierIndex + 1) << ": " << CacheTiers[tierIndex].Cache.Dump();
            }
        
            return result;
        }

    private:

        TIntrusiveList<TPage> DoMove(TPage *page, ui32 sourceTier, ui32 targetTier) {
            TIntrusiveList<TPage> evictedList;
            auto& targetCache = GetTierCache(targetTier);
            if (sourceTier == 0) {
                auto& sourceCache = GetTierCache(DefaultTier);
                UpdateLimits(page, sourceCache, targetCache);
                if (sourceCache.Cache.GetSize() > sourceCache.Cache.GetLimit()) {
                    evictedList.Append(ProcessEvicted(sourceCache.Cache.EvictNext(), DefaultTier));
                }
            } else {
                auto& sourceCache = GetTierCache(sourceTier);
                UpdateLimits(page, sourceCache, targetCache);
                sourceCache.ActivePages->Dec();
                sourceCache.ActiveBytes->Sub(TPageTraits::GetSize(page));
                sourceCache.Cache.Erase(page);
            }

            TPageTraits::SetTier(page, targetTier);
            targetCache.ActivePages->Inc();
            targetCache.ActiveBytes->Add(TPageTraits::GetSize(page));
            evictedList.Append(ProcessEvicted(targetCache.Cache.Touch(page), targetTier));
            return evictedList;
        }

        void UpdateLimits(TPage *page, TCacheHolder& sourceCache, TCacheHolder& targetCache) {
            TransferLimits(TPageTraits::GetSize(page), sourceCache, targetCache);
        }

        void TransferLimits(ui64 amount, TCacheHolder& sourceCache, TCacheHolder& targetCache) {
            auto sourceCacheLimit = sourceCache.Cache.GetLimit();
            auto limitDelta = Min(sourceCacheLimit, amount);
            sourceCache.Cache.UpdateLimit(sourceCacheLimit - limitDelta);
            targetCache.Cache.UpdateLimit(targetCache.Cache.GetLimit() + limitDelta);
        }

        TIntrusiveList<TPage> ProcessEvicted(TIntrusiveList<TPage>&& evictedList, ui32 tier) {
            auto& cache = GetTierCache(tier);
            if (tier != DefaultTier) {
                // shrink non-default cache limit to actual size 
                auto cacheSize = cache.Cache.GetSize();
                auto cacheLimit = cache.Cache.GetLimit();
                if (cacheLimit > cacheSize) {
                    TransferLimits(cacheLimit - cacheSize, cache, GetTierCache(DefaultTier));
                }
            }

            if (evictedList.Empty()) {
                return evictedList;
            }

            ui64 evictedPages = 0;
            ui64 evictedSize = 0;

            for (auto& page_ : evictedList) {
                TPage* page = &page_;
                TPageTraits::SetTier(page, 0);
                ++evictedPages;
                evictedSize += TPageTraits::GetSize(page);
            }

            cache.ActivePages->Sub(evictedPages);
            cache.ActiveBytes->Sub(evictedSize);

            return evictedList;
        }

        Y_FORCE_INLINE TCacheHolder& GetTierCache(ui32 tier) {
            return CacheTiers[tier - 1];
        }

    private:
        TVector<TCacheHolder> CacheTiers; // tierIndex == tier - 1
    };

} // namespace NKikimr::NSharedCache
