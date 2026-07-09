#pragma once

#include "mkql_computation_node.h"
#include "mkql_computation_pattern_cache_program_key.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <library/cpp/threading/future/future.h>

#include <memory>
#include <mutex>

namespace NKikimr::NMiniKQL {

struct TPatternCacheEntry {
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    bool UseAlloc;

    TRuntimeNode ProgramNode;

    ui32 ProgramInputsCount;
    TRuntimeNode ProgramParams;
    TVector<TString> InputItemTypesRaw;
    TVector<TType*> InputItemTypes;
    TVector<TString> OutputItemTypesRaw;
    TVector<TType*> OutputItemTypes;
    TVector<TNode*> EntryPoints; // last entry node stands for parameters

    TStructType* ParamsStruct;
    IComputationPattern::TPtr Pattern;
    size_t SizeForCache = 0;             // set only by cache to lock the size, which can slightly vary when pattern is used
    std::atomic<size_t> AccessTimes = 0; // set only by cache
    std::atomic<bool> IsInCache = false; // set only by cache

    void UpdateSizeForCache() {
        Y_DEBUG_ABORT_UNLESS(!SizeForCache);
        SizeForCache = Alloc.GetAllocated();
    }

    explicit TPatternCacheEntry(bool useAlloc = true)
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , UseAlloc(useAlloc)
    {
        // Release Alloc since it was implicitly acquired in Alloc's ctor
        Alloc.Release();
    }

    ~TPatternCacheEntry() {
        if (UseAlloc) {
            // If alloc was used it should be acquired so dtors of all member fields will use it to free memory
            // Release of Alloc will be called implicitly in Alloc's dtor
            Alloc.Acquire();
        }
    }
};

using TPatternCacheEntryPtr = std::shared_ptr<TPatternCacheEntry>;
using TPatternCacheEntryFuture = NThreading::TFuture<TPatternCacheEntryPtr>;

class TComputationPatternLRUCache {
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

    // TODO(YQL-20086): Migrate YDB to TConfig
    using Config = TConfig;

    explicit TComputationPatternLRUCache(const TConfig& configuration,
                                         NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>());
    ~TComputationPatternLRUCache();

    static TPatternCacheEntryPtr CreateCacheEntry(bool useAlloc = true) {
        return std::make_shared<TPatternCacheEntry>(useAlloc);
    }

    TPatternCacheEntryPtr Find(const TProgramKey& key);
    TPatternCacheEntryFuture FindOrSubscribe(const TProgramKey& key);

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

} // namespace NKikimr::NMiniKQL
