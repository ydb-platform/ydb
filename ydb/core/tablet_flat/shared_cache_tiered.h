#pragma once

#include "shared_cache_counters.h"
#include "shared_cache_switchable.h"
#include "shared_cache_tiers.h"

namespace NKikimr::NSharedCache {

    template <typename TPage, typename TPageTraits>
    class TTieredCache {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

        class TCacheTier {
        public:
            template <typename TCacheBuilder>
            TCacheTier(ui64 limit, TCacheBuilder createCache, ECacheTier tier, TReplacementPolicy policy, TSharedPageCacheCounters& cacheCounters)
                : Cache(TSwitchableCache<TPage, TPageTraits>(limit, createCache(), cacheCounters.ReplacementPolicySize(policy)))
                , Tier(tier)
                , ActivePagesCounter(cacheCounters.ActivePagesTier(tier))
                , ActiveBytesCounter(cacheCounters.ActiveBytesTier(tier))
                , LimitBytesCounter(cacheCounters.LimitBytesTier(tier))
            {
                LimitBytesCounter->Set(limit);
            }

            TIntrusiveList<TPage> Switch(THolder<ICacheCache<TPage>>&& cache, TCounterPtr sizeCounter) Y_WARN_UNUSED_RESULT {
                return ProcessEvicted(Cache.Switch(std::move(cache), std::move(sizeCounter)));
            }

            TIntrusiveList<TPage> EvictNext() Y_WARN_UNUSED_RESULT {
                return ProcessEvicted(Cache.EvictNext());
            }

            TIntrusiveList<TPage> Touch(TPage *page) Y_WARN_UNUSED_RESULT {
                ECacheTier tier = TPageTraits::GetTier(page);
                if (tier == ECacheTier::None) {
                    tier = Tier;
                    TPageTraits::SetTier(page, tier);
                    ActivePagesCounter->Inc();
                    ActiveBytesCounter->Add(TPageTraits::GetSize(page));
                }
                return ProcessEvicted(Cache.Touch(page));
            }

            void Erase(TPage *page) {
                ActivePagesCounter->Dec();
                ActiveBytesCounter->Sub(TPageTraits::GetSize(page));
                TPageTraits::SetTier(page, ECacheTier::None);
                Cache.Erase(page);
            }

            void UpdateLimit(ui64 limit) {
                Cache.UpdateLimit(limit);
                LimitBytesCounter->Set(limit);
            }

            ui64 GetSize() const {
                return Cache.GetSize();
            }

            TString Dump() const {
                return TStringBuilder() << Tier << "Tier: " << Cache.Dump();
            }

        private:

            TIntrusiveList<TPage> ProcessEvicted(TIntrusiveList<TPage>&& evictedList) Y_WARN_UNUSED_RESULT {
                if (evictedList.Empty()) {
                    return evictedList;
                }

                ui64 evictedPages = 0;
                ui64 evictedSize = 0;

                for (auto& page_ : evictedList) {
                    TPage* page = &page_;
                    TPageTraits::SetTier(page, ECacheTier::None);
                    ++evictedPages;
                    evictedSize += TPageTraits::GetSize(page);
                }

                ActivePagesCounter->Sub(evictedPages);
                ActiveBytesCounter->Sub(evictedSize);

                return evictedList;
            }

        private:
            TSwitchableCache<TPage, TPageTraits> Cache;
            ECacheTier Tier;
            TCounterPtr ActivePagesCounter;
            TCounterPtr ActiveBytesCounter;
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

        TIntrusiveList<TPage> TryMove(TPage *page, ECacheTier targetTier) Y_WARN_UNUSED_RESULT {
            ECacheTier sourceTier = TPageTraits::GetTier(page);
            Y_ENSURE(targetTier != ECacheTier::None);

            if (sourceTier == ECacheTier::None || sourceTier == targetTier) {
                // nothing to move
                return {};
            }

            CacheTiers[static_cast<size_t>(sourceTier)].Erase(page);
            return CacheTiers[static_cast<size_t>(targetTier)].Touch(page);
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
            if (tier == ECacheTier::None) {
                return RegularTier->Touch(page);
            }
            return CacheTiers[static_cast<size_t>(tier)].Touch(page);
        }

        TIntrusiveList<TPage> MoveTouch(TPage *page, ECacheTier targetTier) Y_WARN_UNUSED_RESULT {
            ECacheTier sourceTier = TPageTraits::GetTier(page);
            Y_ENSURE(targetTier != ECacheTier::None);

            if (sourceTier != ECacheTier::None && sourceTier != targetTier) {
                CacheTiers[static_cast<size_t>(sourceTier)].Erase(page);
            }

            return CacheTiers[static_cast<size_t>(targetTier)].Touch(page);
        }

        void Erase(TPage *page) {
            ECacheTier tier = TPageTraits::GetTier(page);
            if (tier == ECacheTier::None) {
                return;
            }

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

            for (const auto& cacheTier : CacheTiers) {
                if (first) {
                    first = false;
                } else {
                    result << "; ";
                }
                result << cacheTier.Dump();
            }
        
            return result;
        }

    private:
        TVector<TCacheTier> CacheTiers;
        TCacheTier* RegularTier;
        TCacheTier* TryInMemoryTier;
    };

} // namespace NKikimr::NSharedCache
