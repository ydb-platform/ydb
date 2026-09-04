#include "mkql_computation_pattern_cache.h"

#include <util/generic/intrlist.h>

namespace NKikimr::NMiniKQL {

class TComputationPatternLRUCache::TLRUPatternCacheImpl {
public:
    TLRUPatternCacheImpl(size_t maxPatternsSize,
                         size_t maxPatternsSizeBytes,
                         size_t maxCompiledPatternsSize,
                         size_t maxCompiledPatternsSizeBytes)
        : MaxPatternsSize_(maxPatternsSize)
        , MaxPatternsSizeBytes_(maxPatternsSizeBytes)
        , MaxCompiledPatternsSize_(maxCompiledPatternsSize)
        , MaxCompiledPatternsSizeBytes_(maxCompiledPatternsSizeBytes)
    {
    }

    size_t PatternsSize() const {
        return ProgramKeyToPatternCacheHolder_.size();
    }

    size_t PatternsSizeInBytes() const {
        return CurrentPatternsSizeBytes_;
    }

    size_t CompiledPatternsSize() const {
        return CurrentCompiledPatternsSize_;
    }

    size_t PatternsCompiledCodeSizeInBytes() const {
        return CurrentPatternsCompiledCodeSizeInBytes_;
    }

    std::shared_ptr<TPatternCacheEntry> Find(const TProgramKey& key) {
        auto it = ProgramKeyToPatternCacheHolder_.find(key);
        if (it == ProgramKeyToPatternCacheHolder_.end()) {
            return {};
        }

        PromoteEntry(&it->second);

        return it->second.Entry;
    }

    void Insert(const TProgramKey& key, TPatternCacheEntryPtr entry) {
        auto [it, inserted] = ProgramKeyToPatternCacheHolder_.emplace(std::piecewise_construct,
                                                                      std::forward_as_tuple(key),
                                                                      std::forward_as_tuple(key, entry));

        if (!inserted) {
            RemoveEntryFromLists(&it->second);
        } else {
            it->second.Entry->UpdateSizeForCache();
        }

        /// New item is inserted, insert it in the back of both LRU lists and recalculate sizes
        CurrentPatternsSizeBytes_ += it->second.Entry->SizeForCache;
        LruPatternList_.PushBack(&it->second);

        if (it->second.Entry->Pattern->IsCompiled()) {
            ++CurrentCompiledPatternsSize_;
            CurrentPatternsCompiledCodeSizeInBytes_ += it->second.Entry->Pattern->CompiledCodeSize();
            LruCompiledPatternList_.PushBack(&it->second);
        }

        it->second.Entry->IsInCache.store(true);
        ClearIfNeeded();
    }

    void NotifyPatternCompiled(const TProgramKey& key) {
        auto it = ProgramKeyToPatternCacheHolder_.find(key);
        if (it == ProgramKeyToPatternCacheHolder_.end()) {
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

        ++CurrentCompiledPatternsSize_;
        CurrentPatternsCompiledCodeSizeInBytes_ += entry->Pattern->CompiledCodeSize();
        LruCompiledPatternList_.PushBack(&it->second);

        ClearIfNeeded();
    }

    void Clear() {
        CurrentPatternsSizeBytes_ = 0;
        CurrentCompiledPatternsSize_ = 0;
        CurrentPatternsCompiledCodeSizeInBytes_ = 0;

        ProgramKeyToPatternCacheHolder_.clear();
        for (auto& holder : LruPatternList_) {
            holder.Entry->IsInCache.store(false);
        }

        LruPatternList_.Clear();
        LruCompiledPatternList_.Clear();
    }

    void UpdateMaxSizes(size_t maxPatternsSizeBytes, size_t maxCompiledPatternsSizeBytes) {
        MaxPatternsSizeBytes_ = maxPatternsSizeBytes;
        MaxCompiledPatternsSizeBytes_ = maxCompiledPatternsSizeBytes;
        ClearIfNeeded();
    }

private:
    struct TPatternLRUListTag {};
    struct TCompiledPatternLRUListTag {};

    /** Cache holder is used to store the program key and pattern cache entry in intrusive LRU lists.
     * Most recently accessed items are in back of the lists, least recently accessed items are in front of the lists.
     */
    struct TPatternCacheHolder: public TIntrusiveListItem<TPatternCacheHolder, TPatternLRUListTag>,
                                TIntrusiveListItem<TPatternCacheHolder, TCompiledPatternLRUListTag> {
        TPatternCacheHolder(TProgramKey key, std::shared_ptr<TPatternCacheEntry> entry)
            : Key(std::move(key))
            , Entry(std::move(entry))
        {
        }

        bool LinkedInPatternLRUList() const {
            return !TIntrusiveListItem<TPatternCacheHolder, TPatternLRUListTag>::Empty();
        }

        bool LinkedInCompiledPatternLRUList() const {
            return !TIntrusiveListItem<TPatternCacheHolder, TCompiledPatternLRUListTag>::Empty();
        }

        const TProgramKey Key;
        TPatternCacheEntryPtr Entry;
    };

    void PromoteEntry(TPatternCacheHolder* holder) {
        Y_ASSERT(holder->LinkedInPatternLRUList());
        LruPatternList_.Remove(holder);
        LruPatternList_.PushBack(holder);

        if (!holder->LinkedInCompiledPatternLRUList()) {
            return;
        }

        LruCompiledPatternList_.Remove(holder);
        LruCompiledPatternList_.PushBack(holder);
    }

    void RemoveEntryFromLists(TPatternCacheHolder* holder) {
        Y_ASSERT(holder->LinkedInPatternLRUList());
        LruPatternList_.Remove(holder);

        Y_ASSERT(holder->Entry->SizeForCache <= CurrentPatternsSizeBytes_);
        CurrentPatternsSizeBytes_ -= holder->Entry->SizeForCache;

        if (!holder->LinkedInCompiledPatternLRUList()) {
            return;
        }

        Y_ASSERT(CurrentCompiledPatternsSize_ > 0);
        --CurrentCompiledPatternsSize_;

        size_t patternCompiledCodeSize = holder->Entry->Pattern->CompiledCodeSize();
        Y_ASSERT(patternCompiledCodeSize <= CurrentPatternsCompiledCodeSizeInBytes_);
        CurrentPatternsCompiledCodeSizeInBytes_ -= patternCompiledCodeSize;

        LruCompiledPatternList_.Remove(holder);

        holder->Entry->IsInCache.store(false);
    }

    void ClearIfNeeded() {
        /// Remove from pattern LRU list and compiled pattern LRU list
        while (ProgramKeyToPatternCacheHolder_.size() > MaxPatternsSize_ ||
               CurrentPatternsSizeBytes_ > MaxPatternsSizeBytes_) {
            TPatternCacheHolder* holder = LruPatternList_.Front();
            RemoveEntryFromLists(holder);
            ProgramKeyToPatternCacheHolder_.erase(holder->Key);
        }

        /// Only remove from compiled pattern LRU list
        while (CurrentCompiledPatternsSize_ > MaxCompiledPatternsSize_ ||
               CurrentPatternsCompiledCodeSizeInBytes_ > MaxCompiledPatternsSizeBytes_) {
            TPatternCacheHolder* holder = LruCompiledPatternList_.PopFront();

            Y_ASSERT(CurrentCompiledPatternsSize_ > 0);
            --CurrentCompiledPatternsSize_;

            auto& pattern = holder->Entry->Pattern;
            size_t patternCompiledSize = pattern->CompiledCodeSize();
            Y_ASSERT(patternCompiledSize <= CurrentPatternsCompiledCodeSizeInBytes_);
            CurrentPatternsCompiledCodeSizeInBytes_ -= patternCompiledSize;

            pattern->RemoveCompiledCode();
            holder->Entry->AccessTimes.store(0);
        }
    }

    const size_t MaxPatternsSize_;
    size_t MaxPatternsSizeBytes_;
    const size_t MaxCompiledPatternsSize_;
    size_t MaxCompiledPatternsSizeBytes_;

    size_t CurrentPatternsSizeBytes_ = 0;
    size_t CurrentCompiledPatternsSize_ = 0;
    size_t CurrentPatternsCompiledCodeSizeInBytes_ = 0;

    THashMap<TProgramKey, TPatternCacheHolder> ProgramKeyToPatternCacheHolder_;
    TIntrusiveList<TPatternCacheHolder, TPatternLRUListTag> LruPatternList_;
    TIntrusiveList<TPatternCacheHolder, TCompiledPatternLRUListTag> LruCompiledPatternList_;
};

TComputationPatternLRUCache::TComputationPatternLRUCache(
    const TComputationPatternLRUCache::TConfig& configuration,
    NMonitoring::TDynamicCounterPtr counters)
    : Cache_(std::make_unique<TLRUPatternCacheImpl>(
          CacheMaxElementsSize, configuration.MaxSizeBytes, CacheMaxElementsSize, configuration.MaxCompiledSizeBytes))
    , Configuration_(configuration)
    , Hits_(counters->GetCounter("PatternCache/Hits", /*derivative=*/true))
    , HitsCompiled_(counters->GetCounter("PatternCache/HitsCompiled", /*derivative=*/true))
    , Waits_(counters->GetCounter("PatternCache/Waits", /*derivative=*/true))
    , Misses_(counters->GetCounter("PatternCache/Misses", /*derivative=*/true))
    , NotSuitablePattern_(counters->GetCounter("PatternCache/NotSuitablePattern", /*derivative=*/true))
    , SizeItems_(counters->GetCounter("PatternCache/SizeItems", /*derivative=*/false))
    , SizeCompiledItems_(counters->GetCounter("PatternCache/SizeCompiledItems", /*derivative=*/false))
    , SizeBytes_(counters->GetCounter("PatternCache/SizeBytes", /*derivative=*/false))
    , SizeCompiledBytes_(counters->GetCounter("PatternCache/SizeCompiledBytes", /*derivative=*/false))
    , MaxSizeBytesCounter_(counters->GetCounter("PatternCache/MaxSizeBytes", /*derivative=*/false))
    , MaxCompiledSizeBytesCounter_(counters->GetCounter("PatternCache/MaxCompiledSizeBytes", /*derivative=*/false))
{
    *MaxSizeBytesCounter_ = Configuration_.MaxSizeBytes;
    *MaxCompiledSizeBytesCounter_ = Configuration_.MaxCompiledSizeBytes;
}

TComputationPatternLRUCache::~TComputationPatternLRUCache() {
    CleanCache();
}

TPatternCacheEntryPtr TComputationPatternLRUCache::Find(const TProgramKey& key) {
    std::lock_guard<std::mutex> lock(Mutex_);
    if (auto it = Cache_->Find(key)) {
        ++*Hits_;

        if (it->Pattern->IsCompiled()) {
            ++*HitsCompiled_;
        }

        return it;
    }

    ++*Misses_;
    return {};
}

TPatternCacheEntryFuture TComputationPatternLRUCache::FindOrSubscribe(const TProgramKey& key) {
    std::lock_guard lock(Mutex_);
    if (auto it = Cache_->Find(key)) {
        ++*Hits_;
        AccessPattern(key, it);
        return NThreading::MakeFuture<TPatternCacheEntryPtr>(it);
    }

    auto [notifyIt, isNew] = Notify_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(key),
        std::forward_as_tuple());
    if (isNew) {
        ++*Misses_;
        // First future is empty - so the subscriber can initiate the entry creation.
        return {};
    }

    ++*Waits_;
    auto promise = NThreading::NewPromise<TPatternCacheEntryPtr>();
    auto& subscribers = notifyIt->second;
    subscribers.push_back(promise);

    // Second and next futures are not empty - so subscribers can wait while first one creates the entry.
    return promise;
}

void TComputationPatternLRUCache::EmplacePattern(const TProgramKey& key, TPatternCacheEntryPtr patternWithEnv) {
    Y_DEBUG_ABORT_UNLESS(patternWithEnv && patternWithEnv->Pattern);
    TVector<NThreading::TPromise<TPatternCacheEntryPtr>> subscribers;

    {
        std::lock_guard lock(Mutex_);
        Cache_->Insert(key, patternWithEnv);

        auto notifyIt = Notify_.find(key);
        if (notifyIt != Notify_.end()) {
            subscribers.swap(notifyIt->second);
            Notify_.erase(notifyIt);
        }

        UpdatePatternCurrentUsageInfo();
    }

    for (auto& subscriber : subscribers) {
        subscriber.SetValue(patternWithEnv);
    }
}

void TComputationPatternLRUCache::UpdatePatternCurrentUsageInfo() {
    *SizeItems_ = Cache_->PatternsSize();
    *SizeBytes_ = Cache_->PatternsSizeInBytes();
    *SizeCompiledItems_ = Cache_->CompiledPatternsSize();
    *SizeCompiledBytes_ = Cache_->PatternsCompiledCodeSizeInBytes();
}

void TComputationPatternLRUCache::NotifyPatternCompiled(const TProgramKey& key) {
    std::lock_guard lock(Mutex_);
    Cache_->NotifyPatternCompiled(key);
}

void TComputationPatternLRUCache::NotifyPatternMissing(const TProgramKey& key) {
    TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>> subscribers;
    {
        std::lock_guard lock(Mutex_);

        auto notifyIt = Notify_.find(key);
        if (notifyIt != Notify_.end()) {
            subscribers.swap(notifyIt->second);
            Notify_.erase(notifyIt);
        }
    }

    for (auto& subscriber : subscribers) {
        // It's part of API - to set nullptr as broken promise.
        subscriber.SetValue(nullptr);
    }
}

size_t TComputationPatternLRUCache::GetSize() const {
    std::lock_guard lock(Mutex_);
    return Cache_->PatternsSize();
}

void TComputationPatternLRUCache::CleanCache() {
    std::lock_guard lock(Mutex_);
    PatternsToCompile_.clear();
    Cache_->Clear();
    UpdatePatternCurrentUsageInfo();
    Y_DEBUG_ABORT_UNLESS(*SizeItems_ == 0, "Cache is expected to be empty after the CleanCache call");
    Y_DEBUG_ABORT_UNLESS(*SizeBytes_ == 0, "Cache is expected to be empty after the CleanCache call");
    Y_DEBUG_ABORT_UNLESS(*SizeCompiledItems_ == 0, "Cache is expected to be empty after the CleanCache call");
    Y_DEBUG_ABORT_UNLESS(*SizeCompiledBytes_ == 0, "Cache is expected to be empty after the CleanCache call");
}

void TComputationPatternLRUCache::UpdateConfiguration(const TConfig& configuration) {
    std::lock_guard lock(Mutex_);
    Y_ABORT_UNLESS(Configuration_.PatternAccessTimesBeforeTryToCompile ==
                       configuration.PatternAccessTimesBeforeTryToCompile,
                   "PatternAccessTimesBeforeTryToCompile cannot be changed in-place");

    Configuration_.MaxSizeBytes = configuration.MaxSizeBytes;
    Configuration_.MaxCompiledSizeBytes = configuration.MaxCompiledSizeBytes;

    Cache_->UpdateMaxSizes(configuration.MaxSizeBytes, configuration.MaxCompiledSizeBytes);

    *MaxSizeBytesCounter_ = configuration.MaxSizeBytes;
    *MaxCompiledSizeBytesCounter_ = configuration.MaxCompiledSizeBytes;
    UpdatePatternCurrentUsageInfo();
}

void TComputationPatternLRUCache::AccessPattern(const TProgramKey& key, TPatternCacheEntryPtr entry) {
    if (!Configuration_.PatternAccessTimesBeforeTryToCompile || entry->Pattern->IsCompiled()) {
        return;
    }

    size_t PatternAccessTimes = entry->AccessTimes.fetch_add(1) + 1;
    if (PatternAccessTimes == *Configuration_.PatternAccessTimesBeforeTryToCompile ||
        (*Configuration_.PatternAccessTimesBeforeTryToCompile == 0 && PatternAccessTimes == 1)) {
        PatternsToCompile_.emplace(key, entry);
    }
}

} // namespace NKikimr::NMiniKQL
