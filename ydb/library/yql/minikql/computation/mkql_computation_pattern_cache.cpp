#include "mkql_computation_pattern_cache.h"

#include <util/generic/intrlist.h>

namespace NKikimr::NMiniKQL {

class TComputationPatternLRUCache::TLRUPatternCacheImpl
{
public:
    TLRUPatternCacheImpl(size_t maxPatternsSize,
        size_t maxPatternsSizeBytes,
        size_t maxCompiledPatternsSize,
        size_t maxCompiledPatternsSizeBytes)
        : MaxPatternsSize(maxPatternsSize)
        , MaxPatternsSizeBytes(maxPatternsSizeBytes)
        , MaxCompiledPatternsSize(maxCompiledPatternsSize)
        , MaxCompiledPatternsSizeBytes(maxCompiledPatternsSizeBytes)
    {}

    size_t PatternsSize() const {
        return SerializedProgramToPatternCacheHolder.size();
    }

    size_t PatternsSizeInBytes() const {
        return CurrentPatternsSizeBytes;
    }

    size_t CompiledPatternsSize() const {
        return CurrentCompiledPatternsSize;
    }

    size_t PatternsCompiledCodeSizeInBytes() const {
        return CurrentPatternsCompiledCodeSizeInBytes;
    }

    TPatternCacheEntryPtr Find(const TString& serializedProgram) {
        auto it = SerializedProgramToPatternCacheHolder.find(serializedProgram);
        if (it == SerializedProgramToPatternCacheHolder.end()) {
            return {};
        }

        PromoteEntry(&it->second);

        return it->second.Entry;
    }

    void Insert(const TString& serializedProgram, TPatternCacheEntryPtr& entry) {
        auto [it, inserted] = SerializedProgramToPatternCacheHolder.emplace(std::piecewise_construct,
            std::forward_as_tuple(serializedProgram),
            std::forward_as_tuple(serializedProgram, entry));

        if (!inserted) {
            RemoveEntryFromLists(&it->second);
            entry = it->second.Entry;
        } else {
            entry->UpdateSizeForCache();
        }

        /// New item is inserted, insert it in the back of both LRU lists and recalculate sizes
        CurrentPatternsSizeBytes += entry->SizeForCache;
        LRUPatternList.PushBack(&it->second);

        if (entry->Pattern->IsCompiled()) {
            ++CurrentCompiledPatternsSize;
            CurrentPatternsCompiledCodeSizeInBytes += entry->Pattern->CompiledCodeSize();
            LRUCompiledPatternList.PushBack(&it->second);
        }

        entry->IsInCache.store(true);
        ClearIfNeeded();
    }

    void NotifyPatternCompiled(const TString& serializedProgram) {
        auto it = SerializedProgramToPatternCacheHolder.find(serializedProgram);
        if (it == SerializedProgramToPatternCacheHolder.end()) {
            return;
        }

        const auto& entry = it->second.Entry;

        if (!entry->Pattern->IsCompiled()) {
            // This is possible if the old entry got removed from cache while being compiled - and the new entry got in.
            // TODO: add metrics for this inefficient cache usage.
            // TODO: make this scenario more consistent - don't waste compilation result.
            return;
        }

        if (it->second.LinkedInCompiledPatternLRUList()) {
            return;
        }

        PromoteEntry(&it->second);

        ++CurrentCompiledPatternsSize;
        CurrentPatternsCompiledCodeSizeInBytes += entry->Pattern->CompiledCodeSize();
        LRUCompiledPatternList.PushBack(&it->second);

        ClearIfNeeded();
    }

    void Clear() {
        CurrentPatternsSizeBytes = 0;
        CurrentCompiledPatternsSize = 0;
        CurrentPatternsCompiledCodeSizeInBytes = 0;

        SerializedProgramToPatternCacheHolder.clear();
        for (auto & holder : LRUPatternList) {
            holder.Entry->IsInCache.store(false);
        }

        LRUPatternList.Clear();
        LRUCompiledPatternList.Clear();
    }
private:
    struct TPatternLRUListTag {};
    struct TCompiledPatternLRUListTag {};

    /** Cache holder is used to store serialized program and pattern cache entry in intrusive LRU lists.
      * Most recently accessed items are in back of the lists, least recently accessed items are in front of the lists.
      */
    struct TPatternCacheHolder : public TIntrusiveListItem<TPatternCacheHolder, TPatternLRUListTag>, TIntrusiveListItem<TPatternCacheHolder, TCompiledPatternLRUListTag> {
        TPatternCacheHolder(TString serializedProgram, TPatternCacheEntryPtr entry)
            : SerializedProgram(std::move(serializedProgram))
            , Entry(std::move(entry))
        {}

        bool LinkedInPatternLRUList() const {
            return !TIntrusiveListItem<TPatternCacheHolder, TPatternLRUListTag>::Empty();
        }

        bool LinkedInCompiledPatternLRUList() const {
            return !TIntrusiveListItem<TPatternCacheHolder, TCompiledPatternLRUListTag>::Empty();
        }

        const TString SerializedProgram;
        TPatternCacheEntryPtr Entry;
    };

    void PromoteEntry(TPatternCacheHolder* holder) {
        Y_ASSERT(holder->LinkedInPatternLRUList());
        LRUPatternList.Remove(holder);
        LRUPatternList.PushBack(holder);

        if (!holder->LinkedInCompiledPatternLRUList()) {
            return;
        }

        LRUCompiledPatternList.Remove(holder);
        LRUCompiledPatternList.PushBack(holder);
    }

    void RemoveEntryFromLists(TPatternCacheHolder* holder) {
        Y_ASSERT(holder->LinkedInPatternLRUList());
        LRUPatternList.Remove(holder);

        Y_ASSERT(holder->Entry->SizeForCache <= CurrentPatternsSizeBytes);
        CurrentPatternsSizeBytes -= holder->Entry->SizeForCache;

        if (!holder->LinkedInCompiledPatternLRUList()) {
            return;
        }

        Y_ASSERT(CurrentCompiledPatternsSize > 0);
        --CurrentCompiledPatternsSize;

        size_t patternCompiledCodeSize = holder->Entry->Pattern->CompiledCodeSize();
        Y_ASSERT(patternCompiledCodeSize <= CurrentPatternsCompiledCodeSizeInBytes);
        CurrentPatternsCompiledCodeSizeInBytes -= patternCompiledCodeSize;

        LRUCompiledPatternList.Remove(holder);

        holder->Entry->IsInCache.store(false);
    }

    void ClearIfNeeded() {
        /// Remove from pattern LRU list and compiled pattern LRU list
        while (SerializedProgramToPatternCacheHolder.size() > MaxPatternsSize || CurrentPatternsSizeBytes > MaxPatternsSizeBytes) {
            TPatternCacheHolder* holder = LRUPatternList.Front();
            RemoveEntryFromLists(holder);
            SerializedProgramToPatternCacheHolder.erase(holder->SerializedProgram);
        }

        /// Only remove from compiled pattern LRU list
        while (CurrentCompiledPatternsSize > MaxCompiledPatternsSize || CurrentPatternsCompiledCodeSizeInBytes > MaxCompiledPatternsSizeBytes) {
            TPatternCacheHolder* holder = LRUCompiledPatternList.PopFront();

            Y_ASSERT(CurrentCompiledPatternsSize > 0);
            --CurrentCompiledPatternsSize;

            auto & pattern = holder->Entry->Pattern;
            size_t patternCompiledSize = pattern->CompiledCodeSize();
            Y_ASSERT(patternCompiledSize <= CurrentPatternsCompiledCodeSizeInBytes);
            CurrentPatternsCompiledCodeSizeInBytes -= patternCompiledSize;

            pattern->RemoveCompiledCode();
            holder->Entry->AccessTimes.store(0);
        }
    }

    const size_t MaxPatternsSize;
    const size_t MaxPatternsSizeBytes;
    const size_t MaxCompiledPatternsSize;
    const size_t MaxCompiledPatternsSizeBytes;

    size_t CurrentPatternsSizeBytes = 0;
    size_t CurrentCompiledPatternsSize = 0;
    size_t CurrentPatternsCompiledCodeSizeInBytes = 0;

    THashMap<TString, TPatternCacheHolder> SerializedProgramToPatternCacheHolder;
    TIntrusiveList<TPatternCacheHolder, TPatternLRUListTag> LRUPatternList;
    TIntrusiveList<TPatternCacheHolder, TCompiledPatternLRUListTag> LRUCompiledPatternList;
};

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

TPatternCacheEntryPtr TComputationPatternLRUCache::Find(const TString& serializedProgram) {
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

TPatternCacheEntryFuture TComputationPatternLRUCache::FindOrSubscribe(const TString& serializedProgram) {
    std::lock_guard lock(Mutex);
    if (auto it = Cache->Find(serializedProgram)) {
        ++*Hits;
        AccessPattern(serializedProgram, it);
        return NThreading::MakeFuture<TPatternCacheEntryPtr>(it);
    }

    auto [notifyIt, isNew] = Notify.emplace(std::piecewise_construct, std::forward_as_tuple(serializedProgram), std::forward_as_tuple());
    if (isNew) {
        ++*Misses;
        // First future is empty - so the subscriber can initiate the entry creation.
        return {};
    }

    ++*Waits;
    auto promise = NThreading::NewPromise<TPatternCacheEntryPtr>();
    auto& subscribers = notifyIt->second;
    subscribers.push_back(promise);

    // Second and next futures are not empty - so subscribers can wait while first one creates the entry.
    return promise;
}

void TComputationPatternLRUCache::EmplacePattern(const TString& serializedProgram, TPatternCacheEntryPtr& patternWithEnv) {
    Y_DEBUG_ABORT_UNLESS(patternWithEnv && patternWithEnv->Pattern);
    TVector<NThreading::TPromise<TPatternCacheEntryPtr>> subscribers;

    {
        std::lock_guard lock(Mutex);
        Cache->Insert(serializedProgram, patternWithEnv);

        auto notifyIt = Notify.find(serializedProgram);
        if (notifyIt != Notify.end()) {
            subscribers.swap(notifyIt->second);
            Notify.erase(notifyIt);
        }

        *SizeItems = Cache->PatternsSize();
        *SizeBytes = Cache->PatternsSizeInBytes();
        *SizeCompiledItems = Cache->CompiledPatternsSize();
        *SizeCompiledBytes = Cache->PatternsCompiledCodeSizeInBytes();
    }

    for (auto& subscriber : subscribers) {
        subscriber.SetValue(patternWithEnv);
    }
}

void TComputationPatternLRUCache::NotifyPatternCompiled(const TString& serializedProgram) {
    std::lock_guard lock(Mutex);
    Cache->NotifyPatternCompiled(serializedProgram);
}

void TComputationPatternLRUCache::NotifyPatternMissing(const TString& serializedProgram) {
    TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>> subscribers;
    {
        std::lock_guard lock(Mutex);

        auto notifyIt = Notify.find(serializedProgram);
        if (notifyIt != Notify.end()) {
            subscribers.swap(notifyIt->second);
            Notify.erase(notifyIt);
        }
    }

    for (auto& subscriber : subscribers) {
        // It's part of API - to set nullptr as broken promise.
        subscriber.SetValue(nullptr);
    }
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

void TComputationPatternLRUCache::AccessPattern(const TString& serializedProgram, TPatternCacheEntryPtr entry) {
    if (!Configuration.PatternAccessTimesBeforeTryToCompile || entry->Pattern->IsCompiled()) {
        return;
    }

    size_t PatternAccessTimes = entry->AccessTimes.fetch_add(1) + 1;
    if (PatternAccessTimes == *Configuration.PatternAccessTimesBeforeTryToCompile ||
        (*Configuration.PatternAccessTimesBeforeTryToCompile == 0 && PatternAccessTimes == 1)) {
        PatternsToCompile.emplace(serializedProgram, entry);
    }
}

} // namespace NKikimr::NMiniKQL
