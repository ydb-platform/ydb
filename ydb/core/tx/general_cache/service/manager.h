#pragma once
#include "counters.h"

#include <ydb/core/tx/general_cache/source/abstract.h>
#include <ydb/core/tx/general_cache/usage/abstract.h>
#include <ydb/core/tx/general_cache/usage/config.h>

#include <ydb/library/accessor/positive_integer.h>
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
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(ui64, RequestId, Counter.Inc());
    YDB_READONLY(TMonotonic, Created, TMonotonic::Now());
    YDB_READONLY_DEF(THashSet<TAddress>, Wait);
    THashMap<TAddress, TObject> Result;
    THashSet<TAddress> Removed;
    THashMap<TAddress, TString> Errors;

    std::shared_ptr<ICallback> Callback;
    const EConsumer Consumer;

public:
    EConsumer GetConsumer() const {
        return Consumer;
    }

    [[nodiscard]] bool AddResult(const TAddress& addr, const TObject& obj) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Result.emplace(addr, obj).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
        }
        return Wait.empty();
    }

    [[nodiscard]] bool AddRemoved(const TAddress& addr) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Removed.emplace(addr).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
        }
        return Wait.empty();
    }

    [[nodiscard]] bool AddError(const TAddress& addr, const TString& errorMessage) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Errors.emplace(addr, errorMessage).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
        }
        return Wait.empty();
    }

    TRequest(THashSet<TAddress>&& addresses, std::shared_ptr<ICallback>&& callback, const EConsumer consumer)
        : Wait(addresses)
        , Callback(callback)
        , Consumer(consumer) {
    }
};

template <class TPolicy>
class TManager {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using EConsumer = typename TPolicy::EConsumer;
    using TRequest = TRequest<TPolicy>;

    const NPublic::TConfig Config;
    const TString CacheName = TPolicy::GetCacheName();
    const std::shared_ptr<TManagerCounters> Counters;
    std::shared_ptr<NSource::IObjectsProcessor<TPolicy>> ObjectsProcessor;
    TLRUCache<TAddress, TObject, TNoopDelete, typename TPolicy::TSizeCalcer> Cache;
    THashMap<TAddress, std::vector<std::shared_ptr<TRequest>>> RequestedObjects;
    THashSet<ui64> RequestsInProgress;
    std::deque<std::shared_ptr<TRequest>> RequestsQueue;
    TPositiveControlInteger QueueObjectsCount;

    void DrainQueue() {
        THashMap<EConsumer, THashSet<TAddress>> requestedAddresses;
        while (RequestsQueue.size() && RequestedObjects.size() < Config.GetDirectInflightLimit()) {
            auto request = std::move(RequestsQueue.front());
            RequestsQueue.pop_front();
            QueueObjectsCount.Sub(request->GetWait().size());
            AFL_VERIFY(RequestsInProgress.emplace(request->GetRequestId()).second);
            auto& addresses = requestedAddresses[request->GetConsumer()];
            for (auto&& i : request->GetWait()) {
                auto it = RequestedObjects.find(i);
                if (it == RequestedObjects.end()) {
                    it = RequestedObjects.emplace(i, std::vector<std::shared_ptr<TRequest>>()).first;
                    AFL_VERIFY(addresses.emplace(i).second);
                }
                it->second.emplace_back(request);
            }
        }
        ObjectsProcessor->AskData(std::move(requestedAddresses), ObjectsProcessor);
        Counters->DirectRequests->Inc();
        Counters->RequestsQueueSize->Set(RequestsQueue.size());
        Counters->ObjectsQueueSize->Set(QueueObjectsCount.Val());
        Counters->ObjectsInFlight->Set(RequestedObjects.size());
        Counters->RequestsInFlight->Set(RequestsInProgress.size());
        Counters->CacheSizeCount->Set(Cache.Size());
        Counters->CacheSizeBytes->Set(Cache.TotalSize());
    }

    void AddObjects(THashMap<TAddress, TObject>&& add, const bool isAdditional, const TMonotonic now) {
        for (auto&& i : add) {
            auto it = RequestedObjects.find(i.first);
            if (it != RequestedObjects.end()) {
                Cache.Insert(i.first, i.second);
                for (auto&& r : it->second) {
                    if (r->AddResult(i.first, i.second)) {
                        RequestsInProgress.erase(r->GetRequestId());
                        Counters->OnRequestFinished(now - r->GetCreated());
                        if (isAdditional) {
                            Counters->AdditionalObjectInfo->Inc();
                        } else {
                            Counters->FetchedObject->Inc();
                        }
                    }

                }
                RequestedObjects.erase(it);
            }
        }
    }

    void RemoveObjects(THashSet<TAddress>&& remove, const bool isAdditional, const TMonotonic now) {
        for (auto&& i : remove) {
            auto it = RequestedObjects.find(i);
            AFL_VERIFY(it != RequestedObjects.end());
            for (auto&& r : it->second) {
                if (r->AddRemoved(i)) {
                    RequestsInProgress.erase(r->GetRequestId());
                    Counters->OnRequestFinished(now - r->GetCreated());
                    if (isAdditional) {
                        Counters->RemovedObjectInfo->Inc();
                    } else {
                        Counters->NoExistsObject->Inc();
                    }
                }
            }
            RequestedObjects.erase(it);
        }
    }

public:
    TManager(const NPublic::TConfig& config, const NActors::TActorId& ownerActorId,
        const std::shared_ptr<TManagerCounters>& counters)
        : Config(config)
        , Counters(counters)
        , ObjectsProcessor(TPolicy::BuildObjectsProcessor(ownerActorId))
        , Cache(Config.GetMemoryLimit()) {
        AFL_NOTICE(NKikimrServices::GENERAL_CACHE)("event", "general_cache_manager")("owner_actor_id", ownerActorId)("config", config.DebugString());
    }

    void AddRequest(const std::shared_ptr<TRequest>& request) {
        AFL_DEBUG(NKikimrServices::GENERAL_CACHE)("event", "add_request");
        std::vector<TAddress> addressesToAsk;
        THashMap<TAddress, TObject> objectsResult;
        Counters->IncomingRequestsCount->Inc();
        for (auto&& i : request->GetWait()) {
            auto it = Cache.Find(i);
            if (it == Cache.End()) {
                Counters->ObjectCacheMiss->Inc();
                addressesToAsk.emplace_back(i);
            } else {
                Counters->ObjectCacheHit->Inc();
                AFL_VERIFY(objectsResult.emplace(i, it.Value()).second);
            }
        }
        for (auto&& i : objectsResult) {
            Y_UNUSED(request->AddResult(i.first, std::move(i.second)));
        }
        if (request->GetWait().empty()) {
            Counters->RequestCacheHit->Inc();
            return;
        } else {
            Counters->RequestCacheMiss->Inc();
        }
        QueueObjectsCount.Add(request->GetWait().size());
        RequestsQueue.emplace_back(request);
        DrainQueue();
    }

    void OnAdditionalObjectsInfo(THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& remove) {
        AFL_DEBUG(NKikimrServices::GENERAL_CACHE)("event", "objects_info");
        const TMonotonic now = TMonotonic::Now();
        AddObjects(std::move(add), true, now);
        RemoveObjects(std::move(remove), true, now);
        DrainQueue();
    }

    void OnRequestResult(THashMap<TAddress, TObject>&& objects, THashSet<TAddress>&& removed, THashMap<TAddress, TString>&& failed) {
        AFL_DEBUG(NKikimrServices::GENERAL_CACHE)("event", "on_result");
        const TMonotonic now = TMonotonic::Now();
        AddObjects(std::move(objects), false, now);
        RemoveObjects(std::move(removed), false, now);

        for (auto&& i : failed) {
            auto it = RequestedObjects.find(i.first);
            AFL_VERIFY(it != RequestedObjects.end());
            for (auto&& r : it->second) {
                if (r->AddError(i.first, i.second)) {
                    RequestsInProgress.erase(r->GetRequestId());
                    Counters->OnRequestFinished(now - r->GetCreated());
                    Counters->FailedObject->Inc();
                }
            }
            RequestedObjects.erase(it);
        }
        DrainQueue();
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
