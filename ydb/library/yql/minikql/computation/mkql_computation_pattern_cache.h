#pragma once

#include "mkql_computation_node.h"

#include <ydb/library/yql/minikql/mkql_node.h>
#include <library/cpp/threading/future/future.h>

#include <memory>

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
        Y_VERIFY_DEBUG(!SizeForCache);
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
            Y_VERIFY(HasFuture());
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
        Config(size_t maxSizeBytes)
            : MaxSizeBytes(maxSizeBytes)
        {}

        Config(size_t maxSizeBytes, size_t patternAccessTimesBeforeTryToCompile)
            : MaxSizeBytes(maxSizeBytes)
            , PatternAccessTimesBeforeTryToCompile(patternAccessTimesBeforeTryToCompile)
        {}

        size_t MaxSizeBytes = 0;
        std::optional<size_t> PatternAccessTimesBeforeTryToCompile;

        bool operator==(const Config & rhs) {
            return std::tie(MaxSizeBytes, PatternAccessTimesBeforeTryToCompile) ==
                std::tie(rhs.MaxSizeBytes, rhs.PatternAccessTimesBeforeTryToCompile);
        }

        bool operator!=(const Config & rhs) {
            return !(*this == rhs);
        }
    };

    TComputationPatternLRUCache(Config configuration, NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>());

    ~TComputationPatternLRUCache() {
        CleanCache();
    }

    static std::shared_ptr<TPatternCacheEntry> CreateCacheEntry(bool useAlloc = true) {
        return std::make_shared<TPatternCacheEntry>(useAlloc);
    }

    std::shared_ptr<TPatternCacheEntry> Find(const TString& serialized);

    TTicket FindOrSubscribe(const TString& serialized);

    void NotifyMissing(const TString& serialized);

    void EmplacePattern(const TString& serialized, std::shared_ptr<TPatternCacheEntry> patternWithEnv);

    void CleanCache();

    size_t GetSize() const {
        std::lock_guard lock(Mutex);
        return Cache.Size();
    }

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
    void RemoveOldest();

    void AccessPattern(const TString & serializedProgram, std::shared_ptr<TPatternCacheEntry> & entry);

    static constexpr size_t CacheMaxElementsSize = 10000;

    mutable std::mutex Mutex;
    THashMap<TString, TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>>> Notify;
    TLRUCache<TString, std::shared_ptr<TPatternCacheEntry>> Cache;
    size_t CurrentSizeBytes = 0;
    THashMap<TString, std::shared_ptr<TPatternCacheEntry>> PatternsToCompile;

    const Config Configuration;

    NMonitoring::TDynamicCounters::TCounterPtr Hits;
    NMonitoring::TDynamicCounters::TCounterPtr Waits;
    NMonitoring::TDynamicCounters::TCounterPtr Misses;
    NMonitoring::TDynamicCounters::TCounterPtr NotSuitablePattern;
    NMonitoring::TDynamicCounters::TCounterPtr SizeItems;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBytes;
    NMonitoring::TDynamicCounters::TCounterPtr MaxSizeBytesCounter;
};

} // namespace NKikimr::NMiniKQL
