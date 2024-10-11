#include "mkql_computation_pattern_cache_impl.h"

namespace NKikimr::NMiniKQL {

TComputationPatternLRUCache::TComputationPatternLRUCache(const TComputationPatternLRUCache::Config& configuration, NMonitoring::TDynamicCounterPtr counters)
    : Cache(std::make_unique<TLRUPatternCacheImpl>(CacheMaxElementsSize, configuration.MaxSizeBytes, CacheMaxElementsSize, configuration.MaxCompiledSizeBytes))
    , Configuration(configuration)
    , Hits(counters->GetCounter("PatternCache/Hits", true))
    , HitsCompiled(counters->GetCounter("PatternCache/HitsCompiled", true))
    , Waits(counters->GetCounter("PatternCache/Waits", true))
    , Misses(counters->GetCounter("PatternCache/Misses", true))
    , NotSuitablePattern(counters->GetCounter("PatternCache/NotSuitablePattern", true))
    , SizeItems(counters->GetCounter("PatternCache/SizeItems", false))
    , SizeCompiledItems(counters->GetCounter("PatternCache/SizeCompiledItems", false))
    , SizeBytes(counters->GetCounter("PatternCache/SizeBytes", false))
    , SizeCompiledBytes(counters->GetCounter("PatternCache/SizeCompiledBytes", false))
    , MaxSizeBytesCounter(counters->GetCounter("PatternCache/MaxSizeBytes", false))
    , MaxCompiledSizeBytesCounter(counters->GetCounter("PatternCache/MaxCompiledSizeBytes", false))
{
    *MaxSizeBytesCounter = Configuration.MaxSizeBytes;
    *MaxCompiledSizeBytesCounter = Configuration.MaxCompiledSizeBytes;
}

TComputationPatternLRUCache::~TComputationPatternLRUCache() {
    CleanCache();
}

std::shared_ptr<TPatternCacheEntry> TComputationPatternLRUCache::Find(const TString& serializedProgram) {
    std::lock_guard<std::mutex> lock(Mutex);
    if (auto it = Cache->Find(serializedProgram)) {
        ++*Hits;

        if (it->Pattern->IsCompiled())
            ++*HitsCompiled;

        return it;
    }

    ++*Misses;
    return {};
}

TComputationPatternLRUCache::TTicket TComputationPatternLRUCache::FindOrSubscribe(const TString& serializedProgram) {
    std::lock_guard lock(Mutex);
    if (auto it = Cache->Find(serializedProgram)) {
        ++*Hits;
        AccessPattern(serializedProgram, it);
        return TTicket(serializedProgram, false, NThreading::MakeFuture<std::shared_ptr<TPatternCacheEntry>>(it), nullptr);
    }

    auto [notifyIt, isNew] = Notify.emplace(serializedProgram, Nothing());
    if (isNew) {
        ++*Misses;
        return TTicket(serializedProgram, true, {}, this);
    }

    ++*Waits;
    auto promise = NThreading::NewPromise<std::shared_ptr<TPatternCacheEntry>>();
    auto& subscribers = notifyIt->second;
    if (!subscribers) {
        subscribers.ConstructInPlace();
    }

    subscribers->push_back(promise);
    return TTicket(serializedProgram, false, promise, nullptr);
}

void TComputationPatternLRUCache::EmplacePattern(const TString& serializedProgram, std::shared_ptr<TPatternCacheEntry> patternWithEnv) {
    Y_DEBUG_ABORT_UNLESS(patternWithEnv && patternWithEnv->Pattern);
    TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>> subscribers;

    {
        std::lock_guard<std::mutex> lock(Mutex);
        Cache->InsertOrUpdate(serializedProgram, patternWithEnv);

        auto notifyIt = Notify.find(serializedProgram);
        if (notifyIt != Notify.end()) {
            subscribers.swap(notifyIt->second);
            Notify.erase(notifyIt);
        }

        *SizeItems = Cache->PatternsSize();
        *SizeBytes = Cache->PatternsSizeInBytes();
        *SizeCompiledItems = Cache->CompiledPatternsSize();
        *SizeCompiledBytes = Cache->CompiledPatternsSizeInBytes();
    }

    if (subscribers) {
        for (auto& subscriber : *subscribers) {
            subscriber.SetValue(patternWithEnv);
        }
    }
}

void TComputationPatternLRUCache::NotifyPatternCompiled(const TString& serializedProgram) {
    std::lock_guard lock(Mutex);
    Cache->NotifyPatternCompiled(serializedProgram);
}

size_t TComputationPatternLRUCache::GetSize() const {
    std::lock_guard lock(Mutex);
    return Cache->PatternsSize();
}

void TComputationPatternLRUCache::CleanCache() {
    *SizeItems = 0;
    *SizeBytes = 0;
    *MaxSizeBytesCounter = 0;
    std::lock_guard lock(Mutex);
    PatternsToCompile.clear();
    Cache->Clear();
}

void TComputationPatternLRUCache::AccessPattern(const TString & serializedProgram, std::shared_ptr<TPatternCacheEntry> & entry) {
    if (!Configuration.PatternAccessTimesBeforeTryToCompile || entry->Pattern->IsCompiled()) {
        return;
    }

    size_t PatternAccessTimes = entry->AccessTimes.fetch_add(1) + 1;
    if (PatternAccessTimes == *Configuration.PatternAccessTimesBeforeTryToCompile ||
        (*Configuration.PatternAccessTimesBeforeTryToCompile == 0 && PatternAccessTimes == 1)) {
        PatternsToCompile.emplace(serializedProgram, entry);
    }
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

} // namespace NKikimr::NMiniKQL
