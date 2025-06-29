#pragma once

#include "shared_cache_counters.h"
#include "shared_cache_switchable.h"

namespace NKikimr::NSharedCache {

    template <typename TPage, typename TPageTraits>
    class TTieredCache : public ICacheCache<TPage> {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

        static constexpr ui32 MaxTier = 3;
        static_assert(MaxTier == ((1 << 2) - 1));
        static constexpr ui32 DefaultTier = 0;

        struct TCacheHolder {
            TSwitchableCache<TPage, TPageTraits> Cache;
            TCounterPtr ActivePages;
            TCounterPtr ActiveBytes;
            TCounterPtr DesiredSizeCounter;
            TCounterPtr LimitBytes;
            ui64 DesiredSize;
        };

    public:
        template <typename TCacheBuilder>
        TTieredCache(ui64 limit, TCacheBuilder createCache, ui32 numberOfTiers, TReplacementPolicy policy, TSharedPageCacheCounters& cacheCounters)
            : CacheTiers(::Reserve(numberOfTiers))
        {
            Y_ENSURE(numberOfTiers > 0 && numberOfTiers <= MaxTier);
            TCounterPtr sizeCounter = cacheCounters.ReplacementPolicySize(policy);
            for (ui32 tier = 0; tier < numberOfTiers; ++tier) {
                ui64 tierLimit = tier == DefaultTier ? limit : 0;
                CacheTiers.emplace_back(
                    TSwitchableCache<TPage, TPageTraits>(tierLimit, createCache(), sizeCounter),
                    cacheCounters.ActivePagesTier(tier),
                    cacheCounters.ActiveBytesTier(tier),
                    cacheCounters.DesiredSizeTier(tier),
                    cacheCounters.LimitBytesTier(tier),
                    0
                );
                CacheTiers.back().LimitBytes->Set(tierLimit);
            }
        }

        TIntrusiveList<TPage> TryMove(TPage *page, ui32 targetTier) Y_WARN_UNUSED_RESULT {
            ui32 sourceTier = TPageTraits::GetTier(page);
            Y_ENSURE(sourceTier <= MaxTier);
            Y_ENSURE(targetTier < MaxTier);

            if (sourceTier == MaxTier || sourceTier == targetTier) {
                // nothing to move
                return {};
            }

            return DoMove(page, sourceTier, targetTier);
        }

        template <typename TCacheBuilder>
        TIntrusiveList<TPage> Switch(TCacheBuilder createCache, TCounterPtr sizeCounter) Y_WARN_UNUSED_RESULT {
            TIntrusiveList<TPage> evictedList;

            for (ui32 tier = 0; tier < CacheTiers.size(); ++tier) {
                evictedList.Append(ProcessEvicted(CacheTiers[tier].Cache.Switch(createCache(), sizeCounter), tier));
            }

            return evictedList;
        }

        TIntrusiveList<TPage> EvictNext() override {
            for (ui32 tier = 0; tier < CacheTiers.size(); ++tier) {
                auto& cache = CacheTiers[tier];
                if (auto evicted = cache.Cache.EvictNext(); !evicted.Empty()) {
                    return ProcessEvicted(std::move(evicted), tier);
                }
            }
            return {};
        }

        TIntrusiveList<TPage> Touch(TPage *page) override {
            ui32 tier = TPageTraits::GetTier(page);
            Y_ENSURE(tier <= MaxTier);

            if (tier == MaxTier) {
                tier = DefaultTier;
                TPageTraits::SetTier(page, tier);
                CacheTiers[tier].ActivePages->Inc();
                CacheTiers[tier].ActiveBytes->Add(TPageTraits::GetSize(page));
            }
            return ProcessEvicted(CacheTiers[tier].Cache.Touch(page), tier);
        }

        TIntrusiveList<TPage> MoveTouch(TPage *page, ui32 targetTier) Y_WARN_UNUSED_RESULT {
            ui32 sourceTier = TPageTraits::GetTier(page);
            Y_ENSURE(sourceTier <= MaxTier);
            Y_ENSURE(targetTier < MaxTier);

            if (sourceTier == targetTier) {
                return ProcessEvicted(CacheTiers[sourceTier].Cache.Touch(page), sourceTier);
            }

            return DoMove(page, sourceTier, targetTier);
        }

        void Erase(TPage *page) override {
            ui32 tier = TPageTraits::GetTier(page);
            Y_ENSURE(tier <= MaxTier);
            if (tier < MaxTier) {
                auto& cache = CacheTiers[tier];
                if (tier != DefaultTier) {
                    UpdateLimits(page, cache, CacheTiers[DefaultTier]);
                }
                cache.ActivePages->Dec();
                cache.ActiveBytes->Sub(TPageTraits::GetSize(page));
                TPageTraits::SetTier(page, MaxTier);
                cache.Cache.Erase(page);
            }
        }

        void UpdateLimit(ui64 limit) override {
            for (ui32 tier = CacheTiers.size() - 1; tier > DefaultTier; --tier) {
                auto& cache = CacheTiers[tier];
                ui64 currentTierLimit = Min(Max(cache.Cache.GetSize(), cache.DesiredSize), limit);
                cache.Cache.UpdateLimit(currentTierLimit);
                cache.LimitBytes->Set(currentTierLimit);
                limit -= currentTierLimit;
            }
            CacheTiers[DefaultTier].Cache.UpdateLimit(limit); // set all remaining limit for default zero tier
            CacheTiers[DefaultTier].LimitBytes->Set(limit);
        }

        void AddDesiredSize(ui32 tier, ui64 limit) {
            Y_ENSURE(tier > DefaultTier && tier < MaxTier);
            CacheTiers[tier].DesiredSize += limit;
            CacheTiers[tier].DesiredSizeCounter->Set(CacheTiers[tier].DesiredSize);
        }

        void SubDesiredSize(ui32 tier, ui64 limit) {
            Y_ENSURE(tier > DefaultTier && tier < MaxTier);
            Y_ENSURE(CacheTiers[tier].DesiredSize >= limit);
            CacheTiers[tier].DesiredSize -= limit;
            CacheTiers[tier].DesiredSizeCounter->Set(CacheTiers[tier].DesiredSize);
        }

        ui64 GetLimit() const override {
            ui64 result = 0;
            for (const auto& cacheTier : CacheTiers) {
                result += cacheTier.Cache.GetLimit();
            }
            return result;
        }

        ui64 GetSize() const override {
            ui64 result = 0;
            for (const auto& cacheTier : CacheTiers) {
                result += cacheTier.Cache.GetSize();
            }
            return result;
        }

        TString Dump() const override {
            TStringBuilder result;
            bool first = true;

            for (ui32 tier = 0; tier < CacheTiers.size(); ++tier) {
                if (first) {
                    first = false;
                } else {
                    result << "; ";
                }
                result << "Tier " << tier << ": " << CacheTiers[tier].Cache.Dump();
            }
        
            return result;
        }

    private:

        TIntrusiveList<TPage> DoMove(TPage *page, ui32 sourceTier, ui32 targetTier) Y_WARN_UNUSED_RESULT {
            TIntrusiveList<TPage> evictedList;
            auto& targetCache = CacheTiers[targetTier];
            if (sourceTier < MaxTier) {
                auto& sourceCache = CacheTiers[sourceTier];
                UpdateLimits(page, sourceCache, targetCache);
                sourceCache.ActivePages->Dec();
                sourceCache.ActiveBytes->Sub(TPageTraits::GetSize(page));
                sourceCache.Cache.Erase(page);
            } else if (targetTier != DefaultTier) {
                auto& sourceCache = CacheTiers[DefaultTier];
                ui64 pageSize = TPageTraits::GetSize(page);
                ui64 targetSize = targetCache.Cache.GetSize() + pageSize;
                if (targetSize > targetCache.Cache.GetLimit()) {
                    TransferLimits(targetSize - targetCache.Cache.GetLimit(), sourceCache, targetCache);
                }
                if (sourceCache.Cache.GetSize() > sourceCache.Cache.GetLimit()) {
                    evictedList.Append(ProcessEvicted(sourceCache.Cache.EvictNext(), DefaultTier));
                }
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
            sourceCacheLimit -= limitDelta;
            auto targetCacheLimit = targetCache.Cache.GetLimit() + limitDelta;

            sourceCache.Cache.UpdateLimit(sourceCacheLimit);
            sourceCache.LimitBytes->Set(sourceCacheLimit);

            targetCache.Cache.UpdateLimit(targetCacheLimit);
            targetCache.LimitBytes->Set(targetCacheLimit);
        }

        TIntrusiveList<TPage> ProcessEvicted(TIntrusiveList<TPage>&& evictedList, ui32 tier) Y_WARN_UNUSED_RESULT {
            auto& cache = CacheTiers[tier];

            if (evictedList.Empty()) {
                return evictedList;
            }

            ui64 evictedPages = 0;
            ui64 evictedSize = 0;

            for (auto& page_ : evictedList) {
                TPage* page = &page_;
                TPageTraits::SetTier(page, MaxTier);
                ++evictedPages;
                evictedSize += TPageTraits::GetSize(page);
            }

            cache.ActivePages->Sub(evictedPages);
            cache.ActiveBytes->Sub(evictedSize);

            return evictedList;
        }

    private:
        TVector<TCacheHolder> CacheTiers;
    };

} // namespace NKikimr::NSharedCache
