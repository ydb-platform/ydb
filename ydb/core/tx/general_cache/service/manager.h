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

#include <algorithm>
#include <map>

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
    const NActors::TActorId Owner;
    static inline TAtomicCounter Counter = 0;
    const ui64 Cookie = Counter.Inc();
    THashMap<TAddress, std::vector<std::shared_ptr<TRequest>>> RequestedObjects;
    std::map<ui32, std::deque<std::shared_ptr<TRequest>>> RequestsByPriority;
    YDB_READONLY_DEF(THashSet<ui64>, RequestsInProgress);
    THashMap<ui64, std::vector<std::shared_ptr<TRequest>>> RequestsByCookies;
    const std::shared_ptr<TManagerCounters> Counters;
    const std::shared_ptr<NSource::IObjectsProcessor<TPolicy>> ObjectsProcessor;

public:
    ui64 GetCookie() const {
        return Cookie;
    }

    const NActors::TActorId& GetOwner() const {
        return Owner;
    }

    TSourceInfo(const std::shared_ptr<TManagerCounters>& counters, const TSourceId& sourceId, const NActors::TActorId& owner,
        const std::shared_ptr<NSource::IObjectsProcessor<TPolicy>>& objectsProcessor)
        : SourceId(sourceId)
        , Owner(owner)
        , Counters(counters)
        , ObjectsProcessor(objectsProcessor) {
    }

    bool IsRequestAttached(const TAddress& address, const std::shared_ptr<TRequest>& request) const {
        const auto it = RequestedObjects.find(address);
        if (it == RequestedObjects.end()) {
            return false;
        }
        for (const auto& queued : it->second) {
            if (queued.get() == request.get()) {
                return true;
            }
        }
        return false;
    }

    void EnqueueRequest(const std::shared_ptr<TRequest>& request) {
        Counters->GetQueueObjectsCount()->Add(request->GetWaitBySource(SourceId).size());
        RequestsByPriority[GetCacheRequestPriority(request->GetConsumer())].emplace_back(request);
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
            if (it == RequestedObjects.end()) {
                continue;
            }
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
        const TString error = "source broken: " + ::ToString(SourceId);
        auto abortQueue = [&](std::deque<std::shared_ptr<TRequest>>& queue) {
            for (auto&& request : queue) {
                const auto addresses = request->GetWaitBySource(SourceId);
                ui64 queued = 0;
                bool finished = false;
                for (const auto& objAddr : addresses) {
                    if (IsRequestAttached(objAddr, request)) {
                        continue;
                    }
                    ++queued;
                    finished = request->AddError(objAddr, error) || finished;
                }
                Counters->GetQueueObjectsCount()->Sub(queued);
                if (finished && RequestsInProgress.erase(request->GetRequestId())) {
                    Counters->OnMissCacheRequestFinished(request->GetStartRequest(), request->GetCreated(), now);
                }
            }
            queue.clear();
        };
        for (auto& [priority, queue] : RequestsByPriority) {
            Y_UNUSED(priority);
            abortQueue(queue);
        }
        RequestsByPriority.clear();
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
        const ui64 sourceLimit = Counters->GetConfig().GetDirectInflightSourceLimit();
        const ui64 highPriorityInflightLimit = sourceLimit * 2;
        ui64 highPriorityAdded = 0;
        const TMonotonic now = TMonotonic::Now();

        for (auto it = RequestsByPriority.rbegin(); it != RequestsByPriority.rend(); ++it) {
            const bool highPriority = it->first >= GetCacheRequestPriority(EConsumer::SCAN);
            auto& queue = it->second;
            while (!queue.empty() && Counters->CheckTotalLimit()) {
                if (highPriority) {
                    if (highPriorityAdded >= sourceLimit || RequestedObjects.size() >= highPriorityInflightLimit) {
                        break;
                    }
                } else if (RequestedObjects.size() >= sourceLimit) {
                    break;
                }
                auto request = queue.front();
                queue.pop_front();
                const auto& sourceWaitObjects = request->GetWaitBySource(SourceId);
                if (request->IsAborted()) {
                    const bool started = RequestsInProgress.contains(request->GetRequestId());
                    const auto addresses = sourceWaitObjects;
                    ui64 queuedLeft = 0;
                    bool finished = false;
                    for (const auto& addr : addresses) {
                        if (IsRequestAttached(addr, request)) {
                            continue;
                        }
                        ++queuedLeft;
                        if (started) {
                            finished = request->AddError(addr, "request aborted") || finished;
                        }
                    }
                    Counters->GetQueueObjectsCount()->Sub(queuedLeft);
                    Counters->AbortedRequests->Inc();
                    if (finished) {
                        AFL_VERIFY(RequestsInProgress.erase(request->GetRequestId()));
                        Counters->OnMissCacheRequestFinished(request->GetStartRequest(), request->GetCreated(), now);
                    }
                    continue;
                }

                const bool firstDispatch = RequestsInProgress.emplace(request->GetRequestId()).second;
                if (firstDispatch) {
                    Counters->DirectRequests->Inc();
                }
                const ui64 budget = highPriority
                    ? std::min(sourceLimit - highPriorityAdded, highPriorityInflightLimit - RequestedObjects.size())
                    : (sourceLimit - RequestedObjects.size());
                auto& consumerAddresses = requestedAddresses[request->GetConsumer()];
                ui64 newAddresses = 0;
                ui64 leftQueue = 0;
                bool partial = false;
                for (const auto& addr : sourceWaitObjects) {
                    if (IsRequestAttached(addr, request)) {
                        continue;
                    }
                    auto itAddress = RequestedObjects.find(addr);
                    if (itAddress == RequestedObjects.end()) {
                        if (newAddresses >= budget) {
                            partial = true;
                            break;
                        }
                        itAddress = RequestedObjects.emplace(addr, std::vector<std::shared_ptr<TRequest>>()).first;
                        Counters->GetTotalInFlight()->Inc();
                        AFL_VERIFY(consumerAddresses.emplace(addr).second);
                        Counters->DirectObjects->Inc();
                        ++newAddresses;
                    }
                    itAddress->second.emplace_back(request);
                    ++leftQueue;
                }
                Counters->GetQueueObjectsCount()->Sub(leftQueue);
                if (highPriority) {
                    highPriorityAdded += newAddresses;
                }
                if (partial) {
                    queue.emplace_front(std::move(request));
                    break;
                }
            }
        }
        for (auto it = RequestsByPriority.begin(); it != RequestsByPriority.end();) {
            if (it->second.empty()) {
                it = RequestsByPriority.erase(it);
            } else {
                ++it;
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
    const bool UseCacheSizeFromConfig;
    static constexpr ui64 DEFAULT_CACHE_SIZE = (ui64)1 << 30;

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

    TSourceInfo& UpsertSourceInfo(const TSourceId sourceId, const NActors::TActorId& owner) {
        auto it = SourcesInfo.find(sourceId);
        if (it == SourcesInfo.end()) {
            it = SourcesInfo.emplace(sourceId, TSourceInfo(Counters, sourceId, owner, ObjectsProcessor)).first;
        } else {
            AFL_VERIFY(it->second.GetOwner() == owner);
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
        , Cache(Counters->GetConfig().GetMemoryLimit().value_or(DEFAULT_CACHE_SIZE))
        , UseCacheSizeFromConfig(Counters->GetConfig().GetMemoryLimit().has_value()) {
        YDB_LOG_NOTICE_COMP(NKikimrServices::GENERAL_CACHE, "",
            {"event", "general_cache_manager"},
            {"ownerActorId", ownerActorId},
            {"config", Counters->GetConfig().DebugString()});
        Counters->CacheSizeLimitBytes->Set(Cache.GetMaxSize());
        Counters->CacheConfigSizeLimitBytes->Set(Counters->GetConfig().GetMemoryLimit().value_or(DEFAULT_CACHE_SIZE));
    }

    void CleanUseless(const ui32 countLimit) {
        if constexpr (!TPolicy::EvictCacheWhenSourceGone) {
            return;
        }
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

    void AbortSource(const TSourceId sourceId, const NActors::TActorId& owner) {
        if (auto* sourceInfo = MutableSourceInfoOptional(sourceId)) {
            if (sourceInfo->GetOwner() != owner) {
                return;
            }
        }
        AbortSource(sourceId);
    }

    void AddRequest(const std::shared_ptr<TRequest>& request) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::GENERAL_CACHE, "",
            {"event", "add_request"});
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
            AFL_VERIFY(!i.second.empty());
            const auto owner = TPolicy::GetRequestOwner(*i.second.begin());
            for (const auto& address : i.second) {
                AFL_VERIFY(TPolicy::GetRequestOwner(address) == owner);
            }
            if (auto* existing = MutableSourceInfoOptional(i.first)) {
                if (existing->GetOwner() != owner) {
                    AbortSource(i.first);
                }
            }
            auto& sourceInfo = UpsertSourceInfo(i.first, owner);
            sourceInfo.EnqueueRequest(request);
            sourceInfo.DrainQueue();
        }
    }

    void OnAdditionalObjectsInfo(const TSourceId sourceId, THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& remove) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::GENERAL_CACHE, "",
            {"event", "objects_info"});
        const TMonotonic now = TMonotonic::Now();
        const bool inFlightLimitBrokenBefore = !Counters->CheckTotalLimit();
        bool hasOwner = false;
        NActors::TActorId owner;
        auto rememberOwner = [&](const TAddress& address) {
            const auto addressOwner = TPolicy::GetRequestOwner(address);
            if (!hasOwner) {
                owner = addressOwner;
                hasOwner = true;
            } else {
                AFL_VERIFY(owner == addressOwner);
            }
        };
        for (const auto& item : add) {
            rememberOwner(item.first);
        }
        for (const auto& address : remove) {
            rememberOwner(address);
        }

        auto* sourceInfo = MutableSourceInfoOptional(sourceId);
        if (sourceInfo && hasOwner && sourceInfo->GetOwner() != owner) {
            AddObjectsToCache(add);
            return;
        }
        CleanUseless(add.size());
        AddObjectsToCache(add);
        RemoveObjectsFromCache(remove);
        if (!sourceInfo) {
            return;
        }
        sourceInfo->AddObjects(std::move(add), true, now);
        sourceInfo->RemoveObjects(std::move(remove), true, now);
        const bool inFlightLimitBrokenAfter = !Counters->CheckTotalLimit();
        if (inFlightLimitBrokenBefore && !inFlightLimitBrokenAfter) {
            DrainQueue();
        } else {
            DrainQueue(sourceId);
        }
    }

    void OnRequestResult(const TSourceId sourceId, THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& removed,
        THashMap<TAddress, TString>&& failed, const ui64 sourceCookie) {
        YDB_LOG_DEBUG_COMP(NKikimrServices::GENERAL_CACHE, "",
            {"event", "on_result"});
        const TMonotonic now = TMonotonic::Now();
        const bool inFlightLimitBrokenBefore = !Counters->CheckTotalLimit();
        if (auto* sourceInfo = MutableSourceInfoOptional(sourceId)) {
            if (sourceInfo->GetCookie() != sourceCookie) {
                AddObjectsToCache(add);
                return;
            }
        }
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
        if (UseCacheSizeFromConfig) {
            return;
        }

        if (Cache.GetMaxSize() == maxCacheSize) {
            return;
        }

        YDB_LOG_NOTICE_COMP(NKikimrServices::TX_COLUMNSHARD, "",
            {"event", "update_max_cache_size"},
            {"id", TPolicy::GetCacheName()},
            {"newValue", maxCacheSize},
            {"oldValue", Cache.GetMaxSize()});
        Cache.SetMaxSize(maxCacheSize);
        Counters->CacheSizeLimitBytes->Set(Cache.GetMaxSize());
    }

    void SetMemoryConsumer(TIntrusivePtr<NMemory::IMemoryConsumer> consumer) {
        MemoryConsumer = std::move(consumer);
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
