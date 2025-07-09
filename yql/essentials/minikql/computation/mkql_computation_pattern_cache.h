#pragma once

#include "mkql_computation_node.h"

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
    size_t SizeForCache = 0; // set only by cache to lock the size, which can slightly vary when pattern is used
    std::atomic<size_t> AccessTimes = 0; // set only by cache
    std::atomic<bool> IsInCache = false; // set only by cache

    void UpdateSizeForCache() {
        Y_DEBUG_ABORT_UNLESS(!SizeForCache);
        SizeForCache = Alloc.GetAllocated();
    }

    TPatternCacheEntry(bool useAlloc = true)
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
    struct Config {
        Config(size_t maxSizeBytes, size_t maxCompiledSizeBytes)
            : MaxSizeBytes(maxSizeBytes)
            , MaxCompiledSizeBytes(maxCompiledSizeBytes)
        {}

        Config(size_t maxSizeBytes, size_t maxCompiledSizeBytes, size_t patternAccessTimesBeforeTryToCompile)
            : MaxSizeBytes(maxSizeBytes)
            , MaxCompiledSizeBytes(maxCompiledSizeBytes)
            , PatternAccessTimesBeforeTryToCompile(patternAccessTimesBeforeTryToCompile)
        {}

        const size_t MaxSizeBytes;
        const size_t MaxCompiledSizeBytes;
        const std::optional<size_t> PatternAccessTimesBeforeTryToCompile;

        bool operator==(const Config & rhs) {
            return std::tie(MaxSizeBytes, MaxCompiledSizeBytes, PatternAccessTimesBeforeTryToCompile) ==
                std::tie(rhs.MaxSizeBytes, rhs.MaxCompiledSizeBytes, rhs.PatternAccessTimesBeforeTryToCompile);
        }

        bool operator!=(const Config & rhs) {
            return !(*this == rhs);
        }
    };

    TComputationPatternLRUCache(const Config& configuration, NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>());
    ~TComputationPatternLRUCache();

    static TPatternCacheEntryPtr CreateCacheEntry(bool useAlloc = true) {
        return std::make_shared<TPatternCacheEntry>(useAlloc);
    }

    TPatternCacheEntryPtr Find(const TString& serializedProgram);
    TPatternCacheEntryFuture FindOrSubscribe(const TString& serializedProgram);

    void EmplacePattern(const TString& serializedProgram, TPatternCacheEntryPtr patternWithEnv);

    void NotifyPatternCompiled(const TString& serializedProgram);
    void NotifyPatternMissing(const TString& serializedProgram);

    size_t GetSize() const;

    void CleanCache();

    Config GetConfiguration() const {
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

    void GetPatternsToCompile(THashMap<TString, TPatternCacheEntryPtr> & result) {
        std::lock_guard lock(Mutex_);
        result.swap(PatternsToCompile_);
    }

private:
    class TLRUPatternCacheImpl;

    static constexpr size_t CacheMaxElementsSize = 10000;

    void AccessPattern(const TString& serializedProgram, TPatternCacheEntryPtr entry);

    mutable std::mutex Mutex_;
    THashMap<TString, TVector<NThreading::TPromise<TPatternCacheEntryPtr>>> Notify_; // protected by Mutex
    std::unique_ptr<TLRUPatternCacheImpl> Cache_;                                    // protected by Mutex
    THashMap<TString, TPatternCacheEntryPtr> PatternsToCompile_;                     // protected by Mutex

    const Config Configuration_;

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
