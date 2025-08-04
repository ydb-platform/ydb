#pragma once
#include "counters.h"

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tx/general_cache/source/abstract.h>
#include <ydb/core/tx/general_cache/usage/abstract.h>
#include <ydb/core/tx/general_cache/usage/config.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/signals/object_counter.h>

#include <library/cpp/cache/cache.h>

namespace NKikimr::NGeneralCache::NPrivate {

template <class TPolicy>
class TRequest: public NColumnShard::TMonitoringObjectsCounter<TRequest<TPolicy>>, public TNonCopyable {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using EConsumer = typename TPolicy::EConsumer;
    using ICallback = NPublic::ICallback<TPolicy>;
    using TSourceId = typename TPolicy::TSourceId;
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(ui64, RequestId, Counter.Inc());
    YDB_READONLY(TMonotonic, Created, TMonotonic::Now());
    YDB_READONLY(TMonotonic, StartRequest, TMonotonic::Zero());
    THashSet<ui64> Cookies;
    THashMap<TSourceId, THashSet<TAddress>> Wait;
    THashMap<TAddress, TObject> Result;
    THashSet<TAddress> Removed;
    THashMap<TAddress, TString> Errors;

    TPositiveControlInteger WaitObjectsCount;

    std::shared_ptr<ICallback> Callback;
    const EConsumer Consumer;

    bool RemoveAddrOnFinished(const TAddress addr) {
        auto itSource = Wait.find(TPolicy::GetSourceId(addr));
        AFL_VERIFY(itSource != Wait.end());
        AFL_VERIFY(itSource->second.erase(addr));
        if (itSource->second.empty()) {
            Wait.erase(itSource);
        }
        if (Wait.empty()) {
            AFL_VERIFY(WaitObjectsCount.Val() == 0);
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
            return true;
        } else {
            return false;
        }
    }

public:
    bool TakeFromCache(const TManagerCounters& counters, TLRUCache<TAddress, TObject, TNoopDelete, typename TPolicy::TSizeCalcer>& cache) {
        std::vector<TAddress> toRemove;
        for (auto&& [sourceId, addresses] : Wait) {
            for (auto&& addr : addresses) {
                auto it = cache.Find(addr);
                if (it == cache.End()) {
                    counters.ObjectCacheMiss->Inc();
                } else {
                    counters.ObjectCacheHit->Inc();
                    AFL_VERIFY(Result.emplace(addr, it.Value()).second);
                    WaitObjectsCount.Dec();
                    toRemove.emplace_back(addr);
                }
            }
        }
        for (auto&& i : toRemove) {
            Y_UNUSED(RemoveAddrOnFinished(i));
        }
        return Wait.empty();
    }

    ui64 GetWaitObjectsCount() {
        return WaitObjectsCount.Val();
    }

    const THashMap<TSourceId, THashSet<TAddress>>& GetWaitBySource() const {
        return Wait;
    }

    const THashSet<TAddress>& GetWaitBySource(const TSourceId sourceId) const {
        auto it = Wait.find(sourceId);
        AFL_VERIFY(it != Wait.end());
        return it->second;
    }

    bool IsAborted() const {
        return Callback->IsAborted();
    }

    EConsumer GetConsumer() const {
        return Consumer;
    }

    [[nodiscard]] bool AddResult(const TAddress& addr, const TObject& obj) {
        AFL_VERIFY(Result.emplace(addr, obj).second);
        WaitObjectsCount.Dec();
        return RemoveAddrOnFinished(addr);
    }

    [[nodiscard]] bool AddRemoved(const TAddress& addr) {
        AFL_VERIFY(Removed.emplace(addr).second);
        WaitObjectsCount.Dec();
        return RemoveAddrOnFinished(addr);
    }

    [[nodiscard]] bool AddError(const TAddress& addr, const TString& errorMessage) {
        AFL_VERIFY(Errors.emplace(addr, errorMessage).second);
        WaitObjectsCount.Dec();
        return RemoveAddrOnFinished(addr);
    }

    TRequest(
        THashSet<TAddress>&& addresses, std::shared_ptr<ICallback>&& callback, const EConsumer consumer, const TMonotonic startRequestInstant)
        : StartRequest(startRequestInstant)
        , Callback(std::move(callback))
        , Consumer(consumer) {
        for (auto&& i : addresses) {
            Wait[TPolicy::GetSourceId(i)].emplace(i);
            WaitObjectsCount.Inc();
        }
    }
};

template <class TPolicy>
class TSourceInfo {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using TSourceId = typename TPolicy::TSourceId;
    using EConsumer = typename TPolicy::EConsumer;
    using TRequest = TRequest<TPolicy>;

    const TSourceId SourceId;
    static inline TAtomicCounter Counter = 0;
    const ui64 Cookie = Counter.Inc();
    THashMap<TAddress, std::vector<std::shared_ptr<TRequest>>> RequestedObjects;
    YDB_READONLY_DEF(std::deque<std::shared_ptr<TRequest>>, RequestsQueue);
    YDB_READONLY_DEF(THashSet<ui64>, RequestsInProgress);
    THashMap<ui64, std::vector<std::shared_ptr<TRequest>>> RequestsByCookies;
    const std::shared_ptr<TManagerCounters> Counters;
    const std::shared_ptr<NSource::IObjectsProcessor<TPolicy>> ObjectsProcessor;

public:
    ui64 GetCookie() const {
        return Cookie;
    }

    TSourceInfo(const std::shared_ptr<TManagerCounters>& counters, const TSourceId& sourceId,
        const std::shared_ptr<NSource::IObjectsProcessor<TPolicy>>& objectsProcessor)
        : SourceId(sourceId)
        , Counters(counters)
        , ObjectsProcessor(objectsProcessor) {
    }

    void EnqueueRequest(const std::shared_ptr<TRequest>& request) {
        Counters->GetQueueObjectsCount()->Add(request->GetWaitBySource(SourceId).size());
        RequestsQueue.emplace_back(request);
    }

    void AddObjects(THashMap<TAddress, TObject>&& add, const bool isAdditional, const TMonotonic now) {
        for (auto&& i : add) {
            auto it = RequestedObjects.find(i.first);
            if (it == RequestedObjects.end()) {
                continue;
            }
            for (auto&& r : it->second) {
                if (!r->AddResult(i.first, i.second)) {
                    continue;
                }
                RequestsInProgress.erase(r->GetRequestId());
                Counters->OnMissCacheRequestFinished(r->GetStartRequest(), r->GetCreated(), now);
                if (isAdditional) {
                    Counters->AdditionalObjectInfo->Inc();
                } else {
                    Counters->FetchedObject->Inc();
                }
            }
            RequestedObjects.erase(it);
            Counters->GetTotalInFlight()->Dec();
        }
    }

    void FailObjects(THashMap<TAddress, TString>&& failed, const TMonotonic now) {
        for (auto&& i : failed) {
            auto it = RequestedObjects.find(i.first);
            AFL_VERIFY(it != RequestedObjects.end());
            for (auto&& r : it->second) {
                Counters->FailedObject->Inc();
                if (r->AddError(i.first, i.second)) {
                    RequestsInProgress.erase(r->GetRequestId());
                    Counters->OnMissCacheRequestFinished(r->GetStartRequest(), r->GetCreated(), now);
                }
            }
            RequestedObjects.erase(it);
            Counters->GetTotalInFlight()->Dec();
        }
    }

    void RemoveObjects(THashSet<TAddress>&& remove, const bool isAdditional, const TMonotonic now) {
        for (auto&& i : remove) {
            auto it = RequestedObjects.find(i);
            if (it == RequestedObjects.end()) {
                continue;
            }
            for (auto&& r : it->second) {
                if (!r->AddRemoved(i)) {
                    continue;
                }
                RequestsInProgress.erase(r->GetRequestId());
                Counters->OnMissCacheRequestFinished(r->GetStartRequest(), r->GetCreated(), now);
                if (isAdditional) {
                    Counters->RemovedObjectInfo->Inc();
                } else {
                    Counters->NoExistsObject->Inc();
                }
            }
            RequestedObjects.erase(it);
            Counters->GetTotalInFlight()->Dec();
        }
    }

    void Abort() {
        const TMonotonic now = TMonotonic::Now();
        for (auto&& i : RequestsQueue) {
            auto addresses = i->GetWaitBySource(SourceId);
            Counters->GetQueueObjectsCount()->Sub(addresses.size());
            for (auto&& objAddr : addresses) {
                Y_UNUSED(i->AddError(objAddr, "source broken: " + ::ToString(SourceId)));
            }
        }
        RequestsQueue.clear();
        for (auto&& [objAddr, requests] : RequestedObjects) {
            for (auto&& r : requests) {
                Counters->FailedObject->Inc();
                if (r->AddError(objAddr, "source broken: " + ::ToString(SourceId))) {
                    RequestsInProgress.erase(r->GetRequestId());
                    Counters->OnMissCacheRequestFinished(r->GetStartRequest(), r->GetCreated(), now);
                }
            }
        }
        AFL_VERIFY(RequestsInProgress.empty());
        Counters->GetTotalInFlight()->Sub(RequestedObjects.size());
        RequestedObjects.clear();
    }

    ~TSourceInfo() {
        AFL_VERIFY(RequestedObjects.empty() || NActors::TActorSystem::IsStopped());
    }

    void DrainQueue() {
        THashMap<EConsumer, THashSet<TAddress>> requestedAddresses;
        THashSet<TAddress>* consumerAddresses = nullptr;
        std::optional<EConsumer> currentConsumer;
        while (RequestsQueue.size() && RequestedObjects.size() < Counters->GetConfig().GetDirectInflightSourceLimit() &&
               Counters->CheckTotalLimit()) {
            auto request = std::move(RequestsQueue.front());
            RequestsQueue.pop_front();
            auto& sourceWaitObjects = request->GetWaitBySource(SourceId);
            Counters->GetQueueObjectsCount()->Sub(sourceWaitObjects.size());
            if (request->IsAborted()) {
                Counters->AbortedRequests->Inc();
                continue;
            }
            AFL_VERIFY(RequestsInProgress.emplace(request->GetRequestId()).second);
            if (!currentConsumer || *currentConsumer != request->GetConsumer()) {
                consumerAddresses = &requestedAddresses[request->GetConsumer()];
            }
            Counters->DirectRequests->Inc();
            for (auto&& i : sourceWaitObjects) {
                auto it = RequestedObjects.find(i);
                if (it == RequestedObjects.end()) {
                    it = RequestedObjects.emplace(i, std::vector<std::shared_ptr<TRequest>>()).first;
                    Counters->GetTotalInFlight()->Inc();
                    AFL_VERIFY(consumerAddresses->emplace(i).second);
                    Counters->DirectObjects->Inc();
                }
                it->second.emplace_back(request);
            }
        }
        ObjectsProcessor->AskData(std::move(requestedAddresses), ObjectsProcessor, Cookie);
        Counters->ObjectsQueueSize->Set(Counters->GetQueueObjectsCount()->Val());
        Counters->ObjectsInFlight->Set(Counters->GetTotalInFlight()->Val());
    }
};

template <class TPolicy>
class TManager {
private:
    using TSourceId = typename TPolicy::TSourceId;
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using EConsumer = typename TPolicy::EConsumer;
    using TRequest = TRequest<TPolicy>;
    using TSourceInfo = TSourceInfo<TPolicy>;

    const TString CacheName = TPolicy::GetCacheName();
    const std::shared_ptr<TManagerCounters> Counters;
    std::shared_ptr<NSource::IObjectsProcessor<TPolicy>> ObjectsProcessor;
    TLRUCache<TAddress, TObject, TNoopDelete, typename TPolicy::TSizeCalcer> Cache;

    THashMap<TSourceId, TSourceInfo> SourcesInfo;

    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;

    void DrainQueue(const TSourceId sourceId) {
        MutableSourceInfo(sourceId).DrainQueue();
    }

    void DrainQueue() {
        for (auto&& i : SourcesInfo) {
            if (!Counters->CheckTotalLimit()) {
                return;
            }
            i.second.DrainQueue();
        }
    }

    TSourceInfo& MutableSourceInfo(const TSourceId sourceId) {
        auto it = SourcesInfo.find(sourceId);
        AFL_VERIFY(it != SourcesInfo.end());
        return it->second;
    }

    TSourceInfo* MutableSourceInfoOptional(const TSourceId sourceId) {
        auto it = SourcesInfo.find(sourceId);
        if (it != SourcesInfo.end()) {
            return &it->second;
        }
        return nullptr;
    }

    TSourceInfo& UpsertSourceInfo(const TSourceId sourceId) {
        auto it = SourcesInfo.find(sourceId);
        if (it == SourcesInfo.end()) {
            it = SourcesInfo.emplace(sourceId, TSourceInfo(Counters, sourceId, ObjectsProcessor)).first;
        }
        return it->second;
    }

    void AddObjectsToCache(const THashMap<TAddress, TObject>& add) {
        for (auto&& i : add) {
            Cache.Insert(i.first, i.second);
        }
        Counters->CacheSizeCount->Set(Cache.Size());

        const auto cacheTotalSize = Cache.TotalSize();
        Counters->CacheSizeBytes->Set(cacheTotalSize);
        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(cacheTotalSize);
        }
    }

    void RemoveObjectsFromCache(const THashSet<TAddress>& remove) {
        for (auto&& i : remove) {
            auto it = Cache.Find(i);
            if (it != Cache.End()) {
                Cache.Erase(it);
            }
        }
        Counters->CacheSizeCount->Set(Cache.Size());

        const auto cacheTotalSize = Cache.TotalSize();
        Counters->CacheSizeBytes->Set(cacheTotalSize);
        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(cacheTotalSize);
        }
    }

public:
    TManager(const NActors::TActorId& ownerActorId, const std::shared_ptr<TManagerCounters>& counters)
        : Counters(counters)
        , ObjectsProcessor(TPolicy::BuildObjectsProcessor(ownerActorId))
        , Cache(Counters->GetConfig().GetMemoryLimit()) {
        AFL_NOTICE(NKikimrServices::GENERAL_CACHE)("event", "general_cache_manager")("owner_actor_id", ownerActorId)(
            "config", Counters->GetConfig().DebugString());
        Counters->CacheSizeLimitBytes->Set(Cache.GetMaxSize());
        Counters->CacheConfigSizeLimitBytes->Set(Counters->GetConfig().GetMemoryLimit());
    }

    void CleanUseless(const ui32 countLimit) {
        for (ui32 idx = 0; idx < countLimit; ++idx) {
            auto it = Cache.FindOldest();
            if (it == Cache.End()) {
                break;
            }
            if (!SourcesInfo.contains(TPolicy::GetSourceId(it.Key()))) {
                Counters->UselessCleaningCount->Inc();
                Cache.Erase(it);
            } else {
                break;
            }
        }
    }

    TSourceId GetSourceByCookie(const ui64 cookie) const {
        for (auto&& i : SourcesInfo) {
            if (i.second.GetCookie() == cookie) {
                return i.first;
            }
        }
        AFL_VERIFY(false);
        return TSourceId();
    }

    void AbortSource(const TSourceId sourceId) {
        if (auto* sourceInfo = MutableSourceInfoOptional(sourceId)) {
            sourceInfo->Abort();
            SourcesInfo.erase(sourceId);
        }
    }

    void AddRequest(const std::shared_ptr<TRequest>& request) {
        AFL_DEBUG(NKikimrServices::GENERAL_CACHE)("event", "add_request");
        if (request->IsAborted()) {
            Counters->IncomingAbortedRequestsCount->Inc();
            return;
        } else {
            Counters->IncomingRequestsCount->Inc();
        }
        if (request->TakeFromCache(*Counters, Cache)) {
            Counters->OnHitCacheRequestFinished(request->GetStartRequest(), request->GetCreated(), TMonotonic::Now());
            Counters->RequestCacheHit->Inc();
            return;
        } else {
            Counters->RequestCacheMiss->Inc();
        }
        for (auto&& i : request->GetWaitBySource()) {
            auto& sourceInfo = UpsertSourceInfo(i.first);
            sourceInfo.EnqueueRequest(request);
            sourceInfo.DrainQueue();
        }
    }

    void OnAdditionalObjectsInfo(const TSourceId sourceId, THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& remove) {
        AFL_DEBUG(NKikimrServices::GENERAL_CACHE)("event", "objects_info");
        const TMonotonic now = TMonotonic::Now();
        const bool inFlightLimitBrokenBefore = !Counters->CheckTotalLimit();
        CleanUseless(add.size());
        AddObjectsToCache(add);
        RemoveObjectsFromCache(remove);

        auto& sourceInfo = UpsertSourceInfo(sourceId);
        sourceInfo.AddObjects(std::move(add), true, now);
        sourceInfo.RemoveObjects(std::move(remove), true, now);
        const bool inFlightLimitBrokenAfter = !Counters->CheckTotalLimit();
        if (inFlightLimitBrokenBefore && !inFlightLimitBrokenAfter) {
            DrainQueue();
        } else {
            DrainQueue(sourceId);
        }
    }

    void OnRequestResult(
        const TSourceId sourceId, THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& removed, THashMap<TAddress, TString>&& failed) {
        AFL_DEBUG(NKikimrServices::GENERAL_CACHE)("event", "on_result");
        const TMonotonic now = TMonotonic::Now();
        const bool inFlightLimitBrokenBefore = !Counters->CheckTotalLimit();
        CleanUseless(add.size());
        AddObjectsToCache(add);
        RemoveObjectsFromCache(removed);
        if (auto* sourceInfo = MutableSourceInfoOptional(sourceId)) {
            sourceInfo->AddObjects(std::move(add), false, now);
            sourceInfo->RemoveObjects(std::move(removed), false, now);
            sourceInfo->FailObjects(std::move(failed), now);
            const bool inFlightLimitBrokenAfter = !Counters->CheckTotalLimit();
            if (inFlightLimitBrokenBefore && !inFlightLimitBrokenAfter) {
                DrainQueue();
            } else {
                DrainQueue(sourceId);
            }
        } else {
            AFL_VERIFY(Counters->CheckTotalLimit() == !inFlightLimitBrokenBefore);
        }
    }

    void UpdateMaxCacheSize(const size_t maxCacheSize) {
        if (Cache.GetMaxSize() == maxCacheSize) {
            return;
        }
        return;
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "update_max_cache_size")("id", TPolicy::GetCacheName())("new_value", maxCacheSize)(
            "old_value", Cache.GetMaxSize());
        Cache.SetMaxSize(maxCacheSize);
        Counters->CacheSizeLimitBytes->Set(Cache.GetMaxSize());
    }

    void SetMemoryConsumer(TIntrusivePtr<NMemory::IMemoryConsumer> consumer) {
        MemoryConsumer = std::move(consumer);
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
