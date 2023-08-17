#include "mkql_computation_pattern_cache.h"

#include <util/generic/intrlist.h>

namespace NKikimr::NMiniKQL {

class TComputationPatternLRUCache::LRUPatternCacheImpl
{
public:
    LRUPatternCacheImpl(size_t maxPatternsSize,
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

    std::shared_ptr<TPatternCacheEntry>* Find(const TString& serializedProgram) {
        auto it = SerializedProgramToPatternCacheHolder.find(serializedProgram);
        if (it == SerializedProgramToPatternCacheHolder.end()) {
            return nullptr;
        }

        PromoteEntry(&it->second);

        return &it->second.Entry;
    }

    void Insert(const TString& serializedProgram, std::shared_ptr<TPatternCacheEntry>& entry) {
        auto [it, inserted] = SerializedProgramToPatternCacheHolder.emplace(std::piecewise_construct,
            std::forward_as_tuple(serializedProgram),
            std::forward_as_tuple(serializedProgram, entry));

        if (!inserted) {
            RemoveEntryFromLists(&it->second);
        }

        entry->UpdateSizeForCache();

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

    void NotifyPatternCompiled(const TString & serializedProgram, std::shared_ptr<TPatternCacheEntry>& entry) {
        auto it = SerializedProgramToPatternCacheHolder.find(serializedProgram);
        if (it == SerializedProgramToPatternCacheHolder.end()) {
            return;
        }

        Y_ASSERT(entry->Pattern->IsCompiled());

        Y_ASSERT(!it->second.LinkedInCompiledPatternLRUList());
        PromoteEntry(&it->second);

        ++CurrentCompiledPatternsSize;
        CurrentPatternsCompiledCodeSizeInBytes += entry->Pattern->CompiledCodeSize();
        LRUCompiledPatternList.PushBack(&it->second);

        ClearIfNeeded();
    }

    void Clear() {
        SerializedProgramToPatternCacheHolder.clear();
        for (auto & holder : LRUPatternList) {
            holder.Entry->IsInCache.store(false);
        }

        LRUPatternList.Clear();
        LRUCompiledPatternList.Clear();
    }
private:
    struct PatternLRUListTag {};
    struct CompiledPatternLRUListTag {};

    /** Cache holder is used to store serialized program and pattern cache entry in intrusive LRU lists.
      * Most recently accessed items are in back of the lists, least recently accessed items are in front of the lists.
      */
    struct PatternCacheHolder : public TIntrusiveListItem<PatternCacheHolder, PatternLRUListTag>, TIntrusiveListItem<PatternCacheHolder, CompiledPatternLRUListTag> {
        PatternCacheHolder(TString serializedProgram, std::shared_ptr<TPatternCacheEntry> entry)
            : SerializedProgram(std::move(serializedProgram))
            , Entry(std::move(entry))
        {}

        bool LinkedInPatternLRUList() const {
            return !TIntrusiveListItem<PatternCacheHolder, PatternLRUListTag>::Empty();
        }

        bool LinkedInCompiledPatternLRUList() const {
            return !TIntrusiveListItem<PatternCacheHolder, CompiledPatternLRUListTag>::Empty();
        }

        TString SerializedProgram;
        std::shared_ptr<TPatternCacheEntry> Entry;
    };

    void PromoteEntry(PatternCacheHolder* holder) {
        Y_ASSERT(holder->LinkedInPatternLRUList());
        LRUPatternList.Remove(holder);
        LRUPatternList.PushBack(holder);

        if (!holder->LinkedInCompiledPatternLRUList()) {
            return;
        }

        LRUCompiledPatternList.Remove(holder);
        LRUCompiledPatternList.PushBack(holder);
    }

    void RemoveEntryFromLists(PatternCacheHolder* holder) {
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
            PatternCacheHolder* holder = LRUPatternList.Front();
            RemoveEntryFromLists(holder);
            SerializedProgramToPatternCacheHolder.erase(holder->SerializedProgram);
        }

        /// Only remove from compiled pattern LRU list
        while (CurrentCompiledPatternsSize > MaxCompiledPatternsSize || CurrentPatternsCompiledCodeSizeInBytes > MaxCompiledPatternsSizeBytes) {
            PatternCacheHolder * holder = LRUCompiledPatternList.PopFront();

            Y_ASSERT(CurrentCompiledPatternsSize > 0);
            --CurrentCompiledPatternsSize;

            size_t patternCompiledSize = holder->Entry->Pattern->CompiledCodeSize();
            Y_ASSERT(patternCompiledSize <= CurrentPatternsCompiledCodeSizeInBytes);
            CurrentPatternsCompiledCodeSizeInBytes -= patternCompiledSize;
        }
    }

    const size_t MaxPatternsSize = 0;
    const size_t MaxPatternsSizeBytes = 0;
    const size_t MaxCompiledPatternsSize = 0;
    const size_t MaxCompiledPatternsSizeBytes = 0;

    size_t CurrentPatternsSizeBytes = 0;
    size_t CurrentCompiledPatternsSize = 0;
    size_t CurrentPatternsCompiledCodeSizeInBytes = 0;

    THashMap<TString, PatternCacheHolder> SerializedProgramToPatternCacheHolder;
    TIntrusiveList<PatternCacheHolder, PatternLRUListTag> LRUPatternList;
    TIntrusiveList<PatternCacheHolder, CompiledPatternLRUListTag> LRUCompiledPatternList;
};

TComputationPatternLRUCache::TComputationPatternLRUCache(TComputationPatternLRUCache::Config configuration, NMonitoring::TDynamicCounterPtr counters)
    : Cache(std::make_unique<LRUPatternCacheImpl>(CacheMaxElementsSize, configuration.MaxSizeBytes, CacheMaxElementsSize, configuration.MaxCompiledSizeBytes))
    , Configuration(configuration)
    , Hits(counters->GetCounter("PatternCache/Hits", true))
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
        return *it;
    }

    ++*Misses;
    return {};
}

TComputationPatternLRUCache::TTicket TComputationPatternLRUCache::FindOrSubscribe(const TString& serializedProgram) {
    std::lock_guard lock(Mutex);
    if (auto it = Cache->Find(serializedProgram)) {
        ++*Hits;
        AccessPattern(serializedProgram, *it);
        return TTicket(serializedProgram, false, NThreading::MakeFuture<std::shared_ptr<TPatternCacheEntry>>(*it), nullptr);
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
    Y_VERIFY_DEBUG(patternWithEnv && patternWithEnv->Pattern);
    TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>> subscribers;

    {
        std::lock_guard<std::mutex> lock(Mutex);
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

    if (subscribers) {
        for (auto& subscriber : *subscribers) {
            subscriber.SetValue(patternWithEnv);
        }
    }
}

void TComputationPatternLRUCache::NotifyPatternCompiled(const TString& serializedProgram, std::shared_ptr<TPatternCacheEntry> patternWithEnv) {
    std::lock_guard lock(Mutex);
    Cache->NotifyPatternCompiled(serializedProgram, patternWithEnv);
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
