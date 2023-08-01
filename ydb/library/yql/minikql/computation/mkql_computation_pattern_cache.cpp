#include "mkql_computation_pattern_cache.h"

namespace NKikimr::NMiniKQL {

TComputationPatternLRUCache::TComputationPatternLRUCache(TComputationPatternLRUCache::Config configuration, NMonitoring::TDynamicCounterPtr counters)
    : Cache(CacheMaxElementsSize)
    , Configuration(configuration)
    , Hits(counters->GetCounter("PatternCache/Hits", true))
    , Waits(counters->GetCounter("PatternCache/Waits", true))
    , Misses(counters->GetCounter("PatternCache/Misses", true))
    , NotSuitablePattern(counters->GetCounter("PatternCache/NotSuitablePattern", true))
    , SizeItems(counters->GetCounter("PatternCache/SizeItems", false))
    , SizeBytes(counters->GetCounter("PatternCache/SizeBytes", false))
    , MaxSizeBytesCounter(counters->GetCounter("PatternCache/MaxSizeBytes", false))
{
    *MaxSizeBytesCounter = Configuration.MaxSizeBytes;
}

std::shared_ptr<TPatternCacheEntry> TComputationPatternLRUCache::Find(const TString& serialized) {
    std::lock_guard<std::mutex> lock(Mutex);
    if (auto it = Cache.Find(serialized); it != Cache.End()) {
        ++*Hits;
        return *it;
    }

    ++*Misses;
    return {};
}

TComputationPatternLRUCache::TTicket TComputationPatternLRUCache::FindOrSubscribe(const TString& serialized) {
    std::lock_guard lock(Mutex);
    if (auto it = Cache.Find(serialized); it != Cache.End()) {
        ++*Hits;
        AccessPattern(serialized, *it);
        return TTicket(serialized, false, NThreading::MakeFuture<std::shared_ptr<TPatternCacheEntry>>(*it), nullptr);
    }

    auto [notifyIt, isNew] = Notify.emplace(serialized, Nothing());
    if (isNew) {
        ++*Misses;
        return TTicket(serialized, true, {}, this);
    }

    ++*Waits;
    auto promise = NThreading::NewPromise<std::shared_ptr<TPatternCacheEntry>>();
    auto& subscribers = notifyIt->second;
    if (!subscribers) {
        subscribers.ConstructInPlace();
    }

    subscribers->push_back(promise);
    return TTicket(serialized, false, promise, nullptr);
}

void TComputationPatternLRUCache::NotifyMissing(const TString& serialized) {
    TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>> subscribers;
    {
        std::lock_guard<std::mutex> lock(Mutex);
        auto notifyIt = Notify.find(serialized);
        if (notifyIt != Notify.end()) {
            subscribers.swap(notifyIt->second);
            Notify.erase(notifyIt);
        }
    }

    if (subscribers) {
        for (auto& subscriber : *subscribers) {
            subscriber.SetValue(nullptr);
        }
    }
}

void TComputationPatternLRUCache::EmplacePattern(const TString& serialized, std::shared_ptr<TPatternCacheEntry> patternWithEnv) {
    Y_VERIFY_DEBUG(patternWithEnv && patternWithEnv->Pattern);
    TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>> subscribers;
    {
        std::lock_guard<std::mutex> lock(Mutex);
        // normally remove only one old cache entry by iteration to prevent bursts
        if (CurrentSizeBytes > Configuration.MaxSizeBytes) {
            RemoveOldest();
        }
        // to prevent huge memory overusage remove as much as needed
        while (CurrentSizeBytes > 2 * Configuration.MaxSizeBytes) {
            RemoveOldest();
        }

        patternWithEnv->UpdateSizeForCache();
        CurrentSizeBytes += patternWithEnv->SizeForCache;

        Cache.Insert(serialized, patternWithEnv);
        auto notifyIt = Notify.find(serialized);
        if (notifyIt != Notify.end()) {
            subscribers.swap(notifyIt->second);
            Notify.erase(notifyIt);
        }

        *SizeItems = Cache.Size();
        *SizeBytes = CurrentSizeBytes;
    }

    if (subscribers) {
        for (auto& subscriber : *subscribers) {
            subscriber.SetValue(patternWithEnv);
        }
    }
}

void TComputationPatternLRUCache::CleanCache() {
    *SizeItems = 0;
    *SizeBytes = 0;
    *MaxSizeBytesCounter = 0;
    std::lock_guard lock(Mutex);
    CurrentSizeBytes = 0;
    PatternsToCompile.clear();
    Cache.Clear();
}

void TComputationPatternLRUCache::RemoveOldest() {
    auto oldest = Cache.FindOldest();
    Y_VERIFY_DEBUG(oldest != Cache.End());
    CurrentSizeBytes -= oldest.Value()->SizeForCache;
    PatternsToCompile.erase(oldest.Key());
    Cache.Erase(oldest);
}

void TComputationPatternLRUCache::AccessPattern(const TString & serializedProgram, std::shared_ptr<TPatternCacheEntry> & entry) {
    if (!Configuration.PatternAccessTimesBeforeTryToCompile) {
        return;
    }

    if (entry->Pattern->IsCompiled()) {
        return;
    }

    size_t PatternAccessTimes = entry->AccessTimes.fetch_add(1) + 1;
    if (PatternAccessTimes == *Configuration.PatternAccessTimesBeforeTryToCompile ||
        (*Configuration.PatternAccessTimesBeforeTryToCompile == 0 && PatternAccessTimes == 1)) {
        PatternsToCompile.emplace(serializedProgram, entry);
    }
}


} // namespace NKikimr::NMiniKQL
