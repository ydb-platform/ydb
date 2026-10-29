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
#include <optional>
#include <utility>
#include <vector>

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
                } else if (!TPolicy::IsCacheEntryActual(addr, it.Value())) {
                    cache.Erase(it);
                    counters.ObjectCacheMiss->Inc();
                    counters.CacheSizeCount->Set(cache.Size());
                    counters.CacheSizeBytes->Set(cache.TotalSize());
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
    struct TWeightedQueue {
        ui32 Weight = 0;
        ui64 Inflight = 0;
        std::deque<std::shared_ptr<TRequest>> Requests;
    };

    THashMap<TAddress, std::vector<std::shared_ptr<TRequest>>> RequestedObjects;
    std::map<ui32, std::map<ui64, TWeightedQueue>> WeightedQueues;
    std::map<ui32, std::deque<std::shared_ptr<TRequest>>> UnweightedQueues;
    THashMap<TAddress, std::pair<ui32, ui64>> InflightOwner;
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

    NPublic::TCacheRequestClass ClassOf(const EConsumer consumer) const {
        return GetCacheRequestClass(consumer, Counters->GetConfig());
    }

    void EnqueueRequest(const std::shared_ptr<TRequest>& request) {
        Counters->GetQueueObjectsCount()->Add(request->GetWaitBySource(SourceId).size());
        const auto requestClass = ClassOf(request->GetConsumer());
        if (requestClass.Weight == 0) {
            UnweightedQueues[requestClass.Priority].emplace_back(request);
            return;
        }
        auto& queue = WeightedQueues[requestClass.Priority][requestClass.QueueId];
        queue.Weight = requestClass.Weight;
        queue.Requests.emplace_back(request);
    }

    void ReleaseInflight(const TAddress& address) {
        const auto owner = InflightOwner.find(address);
        if (owner == InflightOwner.end()) {
            return;
        }
        const auto [priority, queueId] = owner->second;
        InflightOwner.erase(owner);
        const auto band = WeightedQueues.find(priority);
        if (band == WeightedQueues.end()) {
            return;
        }
        const auto queue = band->second.find(queueId);
        if (queue == band->second.end()) {
            return;
        }
        if (queue->second.Inflight > 0) {
            --queue->second.Inflight;
        }
        if (queue->second.Requests.empty() && queue->second.Inflight == 0) {
            band->second.erase(queue);
        }
        if (band->second.empty()) {
            WeightedQueues.erase(band);
        }
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
            ReleaseInflight(i.first);
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
            ReleaseInflight(i.first);
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
            ReleaseInflight(i);
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
        for (auto& [priority, queues] : WeightedQueues) {
            Y_UNUSED(priority);
            for (auto& [queueId, queue] : queues) {
                Y_UNUSED(queueId);
                abortQueue(queue.Requests);
            }
        }
        WeightedQueues.clear();
        for (auto& [priority, queue] : UnweightedQueues) {
            Y_UNUSED(priority);
            abortQueue(queue);
        }
        UnweightedQueues.clear();
        InflightOwner.clear();
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

    std::optional<ui32> HighestWaitingPriority() const {
        std::optional<ui32> priority;
        for (const auto& [band, queues] : WeightedQueues) {
            for (const auto& [queueId, queue] : queues) {
                Y_UNUSED(queueId);
                if (!queue.Requests.empty()) {
                    priority = std::max(priority.value_or(band), band);
                }
            }
        }
        for (const auto& [band, queue] : UnweightedQueues) {
            if (!queue.empty()) {
                priority = std::max(priority.value_or(band), band);
            }
        }
        return priority;
    }

    std::optional<std::pair<ui64, ui64>> PickWeighted(const ui32 priority, const ui64 sourceLimit) const {
        const auto band = WeightedQueues.find(priority);
        if (band == WeightedQueues.end()) {
            return std::nullopt;
        }
        struct TCandidate {
            ui64 QueueId = 0;
            ui32 Weight = 0;
            ui64 Inflight = 0;
            ui64 Remainder = 0;
            ui64 Target = 0;
        };
        std::vector<TCandidate> candidates;
        ui64 sumWeight = 0;
        for (const auto& [queueId, queue] : band->second) {
            if (queue.Requests.empty() || queue.Weight == 0) {
                continue;
            }
            sumWeight += queue.Weight;
            candidates.push_back(TCandidate{ queueId, queue.Weight, queue.Inflight, 0, 0 });
        }
        if (candidates.empty() || sumWeight == 0 || sourceLimit == 0) {
            return std::nullopt;
        }
        ui64 assigned = 0;
        for (auto& candidate : candidates) {
            const ui64 product = sourceLimit * static_cast<ui64>(candidate.Weight);
            candidate.Target = product / sumWeight;
            candidate.Remainder = product % sumWeight;
            assigned += candidate.Target;
        }
        std::vector<size_t> order(candidates.size());
        for (size_t i = 0; i < order.size(); ++i) {
            order[i] = i;
        }
        std::sort(order.begin(), order.end(), [&](const size_t left, const size_t right) {
            const auto& a = candidates[left];
            const auto& b = candidates[right];
            if (a.Remainder != b.Remainder) {
                return a.Remainder > b.Remainder;
            }
            if (a.Weight != b.Weight) {
                return a.Weight > b.Weight;
            }
            return a.QueueId < b.QueueId;
        });
        for (size_t extra = 0; extra < sourceLimit - assigned && extra < order.size(); ++extra) {
            ++candidates[order[extra]].Target;
        }

        const TCandidate* chosen = nullptr;
        for (const auto& candidate : candidates) {
            if (candidate.Inflight >= candidate.Target) {
                continue;
            }
            if (!chosen || candidate.Target - candidate.Inflight > chosen->Target - chosen->Inflight ||
                (candidate.Target - candidate.Inflight == chosen->Target - chosen->Inflight && candidate.Weight > chosen->Weight) ||
                (candidate.Target - candidate.Inflight == chosen->Target - chosen->Inflight && candidate.Weight == chosen->Weight &&
                    candidate.QueueId < chosen->QueueId)) {
                chosen = &candidate;
            }
        }
        if (!chosen) {
            return std::nullopt;
        }
        return std::make_pair(chosen->QueueId, chosen->Target - chosen->Inflight);
    }

    struct TDispatchResult {
        ui64 NewAddresses = 0;
        bool PutBack = false;
    };

    TDispatchResult DispatchFront(std::deque<std::shared_ptr<TRequest>>& queue, const ui64 newAddressLimit,
        const std::optional<std::pair<ui32, ui64>>& weightedSlot, THashMap<EConsumer, THashSet<TAddress>>& requestedAddresses,
        const TMonotonic now) {
        if (queue.empty() || newAddressLimit == 0) {
            return {};
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
            return {};
        }

        const bool firstDispatch = RequestsInProgress.emplace(request->GetRequestId()).second;
        if (firstDispatch) {
            Counters->DirectRequests->Inc();
        }
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
                if (newAddresses >= newAddressLimit) {
                    partial = true;
                    break;
                }
                itAddress = RequestedObjects.emplace(addr, std::vector<std::shared_ptr<TRequest>>()).first;
                Counters->GetTotalInFlight()->Inc();
                AFL_VERIFY(consumerAddresses.emplace(addr).second);
                Counters->DirectObjects->Inc();
                ++newAddresses;
                if (weightedSlot) {
                    InflightOwner.emplace(addr, *weightedSlot);
                    WeightedQueues[weightedSlot->first][weightedSlot->second].Inflight++;
                }
            }
            itAddress->second.emplace_back(request);
            ++leftQueue;
        }
        Counters->GetQueueObjectsCount()->Sub(leftQueue);
        if (partial) {
            queue.emplace_front(std::move(request));
        }
        return TDispatchResult{ newAddresses, partial };
    }

    void DrainQueue() {
        THashMap<EConsumer, THashSet<TAddress>> requestedAddresses;
        const ui64 sourceLimit = Counters->GetConfig().GetDirectInflightSourceLimit();
        const TMonotonic now = TMonotonic::Now();

        while (RequestedObjects.size() < sourceLimit && Counters->CheckTotalLimit()) {
            const auto priority = HighestWaitingPriority();
            if (!priority) {
                break;
            }
            bool weightedWaiting = false;
            if (const auto band = WeightedQueues.find(*priority); band != WeightedQueues.end()) {
                for (const auto& [queueId, queue] : band->second) {
                    Y_UNUSED(queueId);
                    if (!queue.Requests.empty()) {
                        weightedWaiting = true;
                        break;
                    }
                }
            }
            if (const auto chosen = PickWeighted(*priority, sourceLimit)) {
                auto& queue = WeightedQueues[*priority][chosen->first];
                const ui64 budget = std::min(chosen->second, sourceLimit - RequestedObjects.size());
                const auto dispatched =
                    DispatchFront(queue.Requests, budget, std::make_pair(*priority, chosen->first), requestedAddresses, now);
                if (dispatched.PutBack && dispatched.NewAddresses == 0) {
                    break;
                }
                continue;
            }
            if (weightedWaiting) {
                break;
            }
            auto unweighted = UnweightedQueues.find(*priority);
            if (unweighted == UnweightedQueues.end() || unweighted->second.empty()) {
                break;
            }
            const auto dispatched = DispatchFront(unweighted->second, sourceLimit - RequestedObjects.size(), std::nullopt, requestedAddresses, now);
            if (dispatched.PutBack) {
                break;
            }
        }
        for (auto band = WeightedQueues.begin(); band != WeightedQueues.end();) {
            for (auto queue = band->second.begin(); queue != band->second.end();) {
                if (queue->second.Requests.empty() && queue->second.Inflight == 0) {
                    queue = band->second.erase(queue);
                } else {
                    ++queue;
                }
            }
            if (band->second.empty()) {
                band = WeightedQueues.erase(band);
            } else {
                ++band;
            }
        }
        for (auto it = UnweightedQueues.begin(); it != UnweightedQueues.end();) {
            if (it->second.empty()) {
                it = UnweightedQueues.erase(it);
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
        if (auto* existing = MutableSourceInfoOptional(sourceId)) {
            if (existing->GetOwner() != owner) {
                AbortSource(sourceId);
            }
        }
        auto it = SourcesInfo.find(sourceId);
        if (it == SourcesInfo.end()) {
            it = SourcesInfo.emplace(sourceId, TSourceInfo(Counters, sourceId, owner, ObjectsProcessor)).first;
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
            RemoveObjectsFromCache(remove);
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
        auto* sourceInfo = MutableSourceInfoOptional(sourceId);
        if (!sourceInfo || sourceInfo->GetCookie() != sourceCookie) {
            return;
        }
        const TMonotonic now = TMonotonic::Now();
        const bool inFlightLimitBrokenBefore = !Counters->CheckTotalLimit();
        CleanUseless(add.size());
        AddObjectsToCache(add);
        RemoveObjectsFromCache(removed);
        sourceInfo->AddObjects(std::move(add), false, now);
        sourceInfo->RemoveObjects(std::move(removed), false, now);
        sourceInfo->FailObjects(std::move(failed), now);
        const bool inFlightLimitBrokenAfter = !Counters->CheckTotalLimit();
        if (inFlightLimitBrokenBefore && !inFlightLimitBrokenAfter) {
            DrainQueue();
        } else {
            DrainQueue(sourceId);
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
