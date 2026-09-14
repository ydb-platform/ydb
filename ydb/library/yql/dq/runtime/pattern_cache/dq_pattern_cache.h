#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_pattern_cache.h>
#include <yql/essentials/minikql/computation/mkql_computation_pattern_cache_program_key.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <library/cpp/threading/future/future.h>

#include <memory>
#include <mutex>

namespace NYql::NDq {

using NKikimr::NMiniKQL::TPatternCacheEntry;
using NKikimr::NMiniKQL::TPatternCacheEntryPtr;
using NKikimr::NMiniKQL::TPatternCacheEntryFuture;
using NKikimr::NMiniKQL::TProgramKey;

class TComputationPatternCache {
public:
    struct TConfig {
        TConfig(size_t maxSizeBytes, size_t maxCompiledSizeBytes)
            : MaxSizeBytes(maxSizeBytes)
            , MaxCompiledSizeBytes(maxCompiledSizeBytes)
        {
        }

        TConfig(size_t maxSizeBytes, size_t maxCompiledSizeBytes, size_t patternAccessTimesBeforeTryToCompile)
            : MaxSizeBytes(maxSizeBytes)
            , MaxCompiledSizeBytes(maxCompiledSizeBytes)
            , PatternAccessTimesBeforeTryToCompile(patternAccessTimesBeforeTryToCompile)
        {
        }

        size_t MaxSizeBytes;
        size_t MaxCompiledSizeBytes;
        const std::optional<size_t> PatternAccessTimesBeforeTryToCompile;

        bool operator==(const TConfig& rhs) {
            return std::tie(MaxSizeBytes, MaxCompiledSizeBytes, PatternAccessTimesBeforeTryToCompile) ==
                   std::tie(rhs.MaxSizeBytes, rhs.MaxCompiledSizeBytes, rhs.PatternAccessTimesBeforeTryToCompile);
        }

        bool operator!=(const TConfig& rhs) {
            return !(*this == rhs);
        }
    };

    explicit TComputationPatternCache(const TConfig& configuration,
                                      NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>());
    ~TComputationPatternCache();

    static TPatternCacheEntryPtr CreateCacheEntry(bool useAlloc = true) {
        return std::make_shared<TPatternCacheEntry>(useAlloc);
    }

    TPatternCacheEntryPtr Find(const TProgramKey& key);
    std::optional<TPatternCacheEntryFuture> FindOrSubscribe(const TProgramKey& key);

    void EmplacePattern(const TProgramKey& key, TPatternCacheEntryPtr patternWithEnv);

    void NotifyPatternCompiled(const TProgramKey& key);
    void NotifyPatternMissing(const TProgramKey& key);

    size_t GetSize() const;

    void CleanCache();

    // Adjusts the size limits in-place, preserving cached entries.
    // PatternAccessTimesBeforeTryToCompile must match the existing configuration;
    // a different value requires recreating the cache.
    void UpdateConfiguration(const TConfig& configuration);

    void UpdatePatternCurrentUsageInfo();

    TConfig GetConfiguration() const {
        std::lock_guard lock(Mutex_);
        return Configuration_;
    }

    size_t GetMaxSizeBytes() const {
        std::lock_guard lock(Mutex_);
        return Configuration_.MaxSizeBytes;
    }

    i64 GetCacheHits() const {
        return *Hits_;
    }

    void IncNotSuitablePattern() {
        ++*NotSuitablePattern_;
    }

    size_t GetPatternsToCompileSize() const {
        std::lock_guard lock(Mutex_);
        return PatternsToCompile_.size();
    }

    void GetPatternsToCompile(THashMap<TProgramKey, TPatternCacheEntryPtr>& result) {
        std::lock_guard lock(Mutex_);
        result.swap(PatternsToCompile_);
    }

private:
    class TLRUPatternCacheImpl;

    static constexpr size_t CacheMaxElementsSize = 10000;

    void AccessPattern(const TProgramKey& key, TPatternCacheEntryPtr entry);

    mutable std::mutex Mutex_;
    THashMap<TProgramKey, TVector<NThreading::TPromise<TPatternCacheEntryPtr>>> Notify_; // protected by Mutex
    std::unique_ptr<TLRUPatternCacheImpl> Cache_;                                        // protected by Mutex
    THashMap<TProgramKey, TPatternCacheEntryPtr> PatternsToCompile_;                     // protected by Mutex

    TConfig Configuration_;

    NMonitoring::TDynamicCounters::TCounterPtr Hits_;
    NMonitoring::TDynamicCounters::TCounterPtr HitsCompiled_;
    NMonitoring::TDynamicCounters::TCounterPtr Waits_;
    NMonitoring::TDynamicCounters::TCounterPtr Misses_;
    NMonitoring::TDynamicCounters::TCounterPtr NotSuitablePattern_;
    NMonitoring::TDynamicCounters::TCounterPtr SizeItems_;
    NMonitoring::TDynamicCounters::TCounterPtr SizeCompiledItems_;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBytes_;
    NMonitoring::TDynamicCounters::TCounterPtr SizeCompiledBytes_;
    NMonitoring::TDynamicCounters::TCounterPtr MaxSizeBytesCounter_;
    NMonitoring::TDynamicCounters::TCounterPtr MaxCompiledSizeBytesCounter_;
};

} // namespace NYql::NDq
