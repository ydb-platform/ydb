#pragma once

#include "mkql_computation_node.h"

#include <ydb/library/yql/minikql/mkql_node.h>

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
    mutable std::mutex Mutex;

    TLRUCache<TString, std::shared_ptr<TPatternCacheEntry>> Cache;
    size_t CurrentSizeBytes = 0;
    const size_t MaxSizeBytes = 0;
public:
    NMonitoring::TDynamicCounters::TCounterPtr Hits;
    NMonitoring::TDynamicCounters::TCounterPtr Misses;
    NMonitoring::TDynamicCounters::TCounterPtr SizeItems;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBytes;

public:
    TComputationPatternLRUCache(size_t sizeBytes, NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>())
        : Cache(10000)
        , MaxSizeBytes(sizeBytes)
        , Hits(counters->GetCounter("PatternCache/Hits", true))
        , Misses(counters->GetCounter("PatternCache/Misses", true))
        , SizeItems(counters->GetCounter("PatternCache/SizeItems", false))
        , SizeBytes(counters->GetCounter("PatternCache/SizeBytes", false))
    {
        *counters->GetCounter("PatternCache/MaxSizeBytes", false) = MaxSizeBytes;
    }

    static std::shared_ptr<TPatternCacheEntry> CreateCacheEntry(bool useAlloc = true) {
        return std::make_shared<TPatternCacheEntry>(useAlloc);
    }

    std::shared_ptr<TPatternCacheEntry> Find(const TString& serialized) {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        if (auto it = Cache.Find(serialized); it != Cache.End()) {
            ++*Hits;
            return *it;
        } else {
            ++*Misses;
            return {};
        }
    }

    void RemoveOldest() {
        auto oldest = Cache.FindOldest();
        Y_VERIFY_DEBUG(oldest != Cache.End());
        CurrentSizeBytes -= oldest.Value()->SizeForCache;
        Cache.Erase(oldest);
    }

    void EmplacePattern(const TString& serialized, std::shared_ptr<TPatternCacheEntry> patternWithEnv) {
        Y_VERIFY_DEBUG(patternWithEnv && patternWithEnv->Pattern);
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        // normally remove only one old cache entry by iteration to prevent bursts
        if (CurrentSizeBytes > MaxSizeBytes) {
            RemoveOldest();
        }
        // to prevent huge memory overusage remove as much as needed
        while (CurrentSizeBytes > 2 * MaxSizeBytes) {
            RemoveOldest();
        }

        patternWithEnv->UpdateSizeForCache();
        CurrentSizeBytes += patternWithEnv->SizeForCache;

        Cache.Insert(serialized, std::move(patternWithEnv));

        *SizeItems = Cache.Size();
        *SizeBytes = CurrentSizeBytes;
    }

    void CleanCache() {
        *Hits = 0;
        *Misses = 0;
        *SizeItems = 0;
        *SizeBytes = 0;
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        CurrentSizeBytes = 0;
        Cache.Clear();
    }

    size_t GetSize() const {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        return Cache.Size();
    }

    size_t GetMaxSize() const {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        return MaxSizeBytes;
    }


    i64 GetCacheHits() const {
        return *Hits;
    }

    ~TComputationPatternLRUCache() {
        CleanCache();
        Mutex.lock();
    }
};

} // namespace NKikimr::NMiniKQL
