#pragma once

#include "mkql_computation_node.h"

#include <ydb/library/yql/minikql/mkql_node.h>
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

class TComputationPatternLRUCache {
public:
    class TTicket : private TNonCopyable {
    public:
        TTicket(const TString& serialized, bool isOwned, const NThreading::TFuture<std::shared_ptr<TPatternCacheEntry>>& future, TComputationPatternLRUCache* cache)
            : Serialized(serialized)
            , IsOwned(isOwned)
            , Future(future)
            , Cache(cache)
        {}

        ~TTicket() {
            if (Cache) {
                Cache->NotifyMissing(Serialized);
            }
        }

        bool HasFuture() const {
            return !IsOwned;
        }

        std::shared_ptr<TPatternCacheEntry> GetValueSync() const {
            Y_ABORT_UNLESS(HasFuture());
            return Future.GetValueSync();
        }

        void Close() {
            Cache = nullptr;
        }

    private:
        const TString Serialized;
        const bool IsOwned;
        const NThreading::TFuture<std::shared_ptr<TPatternCacheEntry>> Future;
        TComputationPatternLRUCache* Cache;
    };

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

    static std::shared_ptr<TPatternCacheEntry> CreateCacheEntry(bool useAlloc = true) {
        return std::make_shared<TPatternCacheEntry>(useAlloc);
    }

    std::shared_ptr<TPatternCacheEntry> Find(const TString& serializedProgram);

    TTicket FindOrSubscribe(const TString& serializedProgram);

    void EmplacePattern(const TString& serializedProgram, std::shared_ptr<TPatternCacheEntry> patternWithEnv);

    void NotifyPatternCompiled(const TString& serializedProgram, std::shared_ptr<TPatternCacheEntry> patternWithEnv);

    size_t GetSize() const;

    void CleanCache();

    Config GetConfiguration() const {
        std::lock_guard lock(Mutex);
        return Configuration;
    }

    size_t GetMaxSizeBytes() const {
        std::lock_guard lock(Mutex);
        return Configuration.MaxSizeBytes;
    }

    i64 GetCacheHits() const {
        return *Hits;
    }

    void IncNotSuitablePattern() {
        ++*NotSuitablePattern;
    }

    size_t GetPatternsToCompileSize() const {
        std::lock_guard lock(Mutex);
        return PatternsToCompile.size();
    }

    void GetPatternsToCompile(THashMap<TString, std::shared_ptr<TPatternCacheEntry>> & result) {
        std::lock_guard lock(Mutex);
        result.swap(PatternsToCompile);
    }

private:
    void AccessPattern(const TString & serializedProgram, std::shared_ptr<TPatternCacheEntry> & entry);

    void NotifyMissing(const TString& serialized);

    static constexpr size_t CacheMaxElementsSize = 10000;

    friend class TTicket;

    mutable std::mutex Mutex;
    THashMap<TString, TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>>> Notify;

    class TLRUPatternCacheImpl;
    std::unique_ptr<TLRUPatternCacheImpl> Cache;

    THashMap<TString, std::shared_ptr<TPatternCacheEntry>> PatternsToCompile;

    const Config Configuration;

    NMonitoring::TDynamicCounters::TCounterPtr Hits;
    NMonitoring::TDynamicCounters::TCounterPtr HitsCompiled;
    NMonitoring::TDynamicCounters::TCounterPtr Waits;
    NMonitoring::TDynamicCounters::TCounterPtr Misses;
    NMonitoring::TDynamicCounters::TCounterPtr NotSuitablePattern;
    NMonitoring::TDynamicCounters::TCounterPtr SizeItems;
    NMonitoring::TDynamicCounters::TCounterPtr SizeCompiledItems;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SizeCompiledBytes;
    NMonitoring::TDynamicCounters::TCounterPtr MaxSizeBytesCounter;
    NMonitoring::TDynamicCounters::TCounterPtr MaxCompiledSizeBytesCounter;
};

} // namespace NKikimr::NMiniKQL
