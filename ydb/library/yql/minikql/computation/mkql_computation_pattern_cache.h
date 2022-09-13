#pragma once

#include "mkql_computation_node.h"

#include <ydb/library/yql/minikql/mkql_node.h>

#include <memory>

namespace NKikimr::NMiniKQL {

struct TPatternCacheEntry {
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    IComputationPattern::TPtr Pattern;

    TPatternCacheEntry()
        : Env(Alloc)
    {
        Alloc.Release();
    }

    ~TPatternCacheEntry() {
        Alloc.Acquire();
    }
};

class TComputationPatternLRUCache {
    mutable std::mutex Mutex;

    TLRUCache<TString, std::shared_ptr<TPatternCacheEntry>> Cache;
    std::atomic<size_t> Hits = 0;
    std::atomic<size_t> TotalKeysSize = 0;
    std::atomic<size_t> TotalValuesSize = 0;
public:
    TComputationPatternLRUCache(size_t size = 100)
        : Cache(size)
    {}

    static std::shared_ptr<TPatternCacheEntry> CreateCacheEntry() {
        return std::make_shared<TPatternCacheEntry>();
    }

    std::shared_ptr<TPatternCacheEntry> Find(const TString& serialized) {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        if (auto it = Cache.Find(serialized); it != Cache.End()) {
            ++Hits;
            return *it;
        }
        return {};
    }

    void EmplacePattern(const TString& serialized, std::shared_ptr<TPatternCacheEntry> patternWithEnv) {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        Y_VERIFY_DEBUG(patternWithEnv && patternWithEnv->Pattern);
        TotalKeysSize += serialized.Size();
        TotalValuesSize += patternWithEnv->Alloc.GetAllocated();

        if (Cache.TotalSize() == Cache.GetMaxSize()) {
            auto oldest = Cache.FindOldest();
            Y_VERIFY(oldest != Cache.End());
            TotalKeysSize -= oldest.Key().Size();
            TotalValuesSize -= oldest.Value()->Alloc.GetAllocated();
            Cache.Erase(oldest);
        }

        Cache.Insert(serialized, std::move(patternWithEnv));
    }

    void CleanCache() {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        Cache.Clear();
    }

    size_t GetSize() const {
        auto guard = std::scoped_lock<std::mutex>(Mutex);
        return Cache.TotalSize();
    }

    size_t GetCacheHits() const {
        return Hits.load();
    }

    ~TComputationPatternLRUCache() {
        Mutex.lock();
    }
};

} // namespace NKikimr::NMiniKQL
