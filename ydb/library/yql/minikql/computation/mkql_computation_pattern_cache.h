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

    THashMap<TString, TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>>> Notify;
    TLRUCache<TString, std::shared_ptr<TPatternCacheEntry>> Cache;
    size_t CurrentSizeBytes = 0;
    const size_t MaxSizeBytes = 0;
public:
    NMonitoring::TDynamicCounters::TCounterPtr Hits;
    NMonitoring::TDynamicCounters::TCounterPtr Waits;
    NMonitoring::TDynamicCounters::TCounterPtr Misses;
    NMonitoring::TDynamicCounters::TCounterPtr NotSuitablePattern;
    NMonitoring::TDynamicCounters::TCounterPtr SizeItems;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBytes;
    NMonitoring::TDynamicCounters::TCounterPtr MaxSizeBytesCounter;

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

    TComputationPatternLRUCache(size_t sizeBytes, NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>())
        : Cache(10000)
        , MaxSizeBytes(sizeBytes)
        , Hits(counters->GetCounter("PatternCache/Hits", true))
        , Waits(counters->GetCounter("PatternCache/Waits", true))
        , Misses(counters->GetCounter("PatternCache/Misses", true))
        , NotSuitablePattern(counters->GetCounter("PatternCache/NotSuitablePattern", true))
        , SizeItems(counters->GetCounter("PatternCache/SizeItems", false))
        , SizeBytes(counters->GetCounter("PatternCache/SizeBytes", false))
        , MaxSizeBytesCounter(counters->GetCounter("PatternCache/MaxSizeBytes", false))
    {
        *MaxSizeBytesCounter = MaxSizeBytes;
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

    TTicket FindOrSubscribe(const TString& serialized) {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        if (auto it = Cache.Find(serialized); it != Cache.End()) {
            ++*Hits;
            return TTicket(serialized, false, NThreading::MakeFuture<std::shared_ptr<TPatternCacheEntry>>(*it), nullptr);
        }

        auto [notifyIt, isNew] = Notify.emplace(serialized, Nothing());
        if (isNew) {
            ++*Misses;
            return TTicket(serialized, true, {}, this);
        }

        ++*Waits;
        auto promise = NThreading::NewPromise<std::shared_ptr<TPatternCacheEntry>>();
        auto& subscribers = Notify[serialized];
        if (!subscribers) {
            subscribers.ConstructInPlace();
        }

        subscribers->push_back(promise);
        return TTicket(serialized, false, promise, nullptr);
    }

    void RemoveOldest() {
        auto oldest = Cache.FindOldest();
        Y_VERIFY_DEBUG(oldest != Cache.End());
        CurrentSizeBytes -= oldest.Value()->SizeForCache;
        Cache.Erase(oldest);
    }

    void NotifyMissing(const TString& serialized) {
        TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>> subscribers;
        {
            auto guard = std::scoped_lock<std::mutex>(Mutex);
            auto notifyIt = Notify.find(serialized);
            if (notifyIt != Notify.end()) {
                subscribers.swap(notifyIt->second);
                Notify.erase(notifyIt);
            }
       }

        if (subscribers) {
            for (auto& s : *subscribers) {
                s.SetValue(nullptr);
            }
        }
    }

    void EmplacePattern(const TString& serialized, std::shared_ptr<TPatternCacheEntry> patternWithEnv) {
        Y_VERIFY_DEBUG(patternWithEnv && patternWithEnv->Pattern);
        TMaybe<TVector<NThreading::TPromise<std::shared_ptr<TPatternCacheEntry>>>> subscribers;
        {
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
    
            Cache.Insert(serialized, patternWithEnv);
            auto notifyIt = Notify.find(serialized);
            if (notifyIt != Notify.end()) {
                subscribers.swap(notifyIt->second);
                Notify.erase(notifyIt);
            }
    
            *SizeItems = Cache.Size();
            *SizeBytes = CurrentSizeBytes;
        }

        if (subscribers) {
            for (auto& s : *subscribers) {
                s.SetValue(patternWithEnv);
            }
        }
    }

    void CleanCache() {
        *SizeItems = 0;
        *SizeBytes = 0;
        *MaxSizeBytesCounter = 0;
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

    void IncNotSuitablePattern() {
        ++*NotSuitablePattern;
    }

    ~TComputationPatternLRUCache() {
        CleanCache();
    }
};

} // namespace NKikimr::NMiniKQL
