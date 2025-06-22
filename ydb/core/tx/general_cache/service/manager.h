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
class TRequest: public NColumnShard::TMonitoringObjectsCounter<TRequest<TPolicy>> {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using EConsumer = typename TPolicy::EConsumer;
    using ICallback = NPublic::ICallback<TPolicy>;
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

    void AddResult(const TAddress& addr, const TObject& obj) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Result.emplace(addr, obj).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
        }
    }

    void AddRemoved(const TAddress& addr) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Removed.emplace(addr).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
        }
    }

    void AddError(const TAddress& addr, const TString& errorMessage) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Errors.emplace(addr, errorMessage).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed), std::move(Errors));
        }
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
    const TCounters& Counters;
    std::shared_ptr<NSource::IObjectsProcessor<TPolicy>> ObjectsProcessor;
    TLRUCache<TAddress, TObject, TNoopDelete, typename TPolicy::TSizeCalcer> Cache;
    THashMap<TAddress, std::vector<std::shared_ptr<TRequest>>> Requests;
    std::deque<std::shared_ptr<TRequest>> RequestsQueue;
    TPositiveControlInteger RequestedObjectsInFlight;

    void DrainQueue() {
        THashMap<EConsumer, THashSet<TAddress>> requestedAddresses;
        while (RequestsQueue.size() && RequestedObjectsInFlight.Val() < Config.GetDirectInflightLimit()) {
            auto request = std::move(RequestsQueue.front());
            RequestsQueue.pop_front();
            RequestedObjectsInFlight.Add(request->GetWait().size());
            auto& addresses = requestedAddresses[request->GetConsumer()];
            for (auto&& i : request->GetWait()) {
                auto it = Requests.find(i);
                if (it == Requests.end()) {
                    it = Requests.emplace(i, std::vector<std::shared_ptr<TRequest>>()).first;
                    AFL_VERIFY(addresses.emplace(i).second);
                }
                it->second.emplace_back(request);
            }
        }
        ObjectsProcessor->AskData(std::move(requestedAddresses), ObjectsProcessor);
    }

public:
    TManager(const NPublic::TConfig& config, const NActors::TActorId& ownerActorId,
        const TCounters& counters)
        : Config(config)
        , Counters(counters)
        , ObjectsProcessor(TPolicy::BuildObjectsProcessor(ownerActorId))
        , Cache(Config.GetMemoryLimit()) {
        AFL_WARN(NKikimrServices::GENERAL_CACHE)("event", "general_cache_manager")("owner_actor_id", ownerActorId)("config", config.DebugString());
    }

    void AddRequest(const std::shared_ptr<TRequest>& request) {
        AFL_WARN(NKikimrServices::GENERAL_CACHE)("event", "add_request");
        std::vector<TAddress> addressesToAsk;
        THashMap<TAddress, TObject> objectsResult;
        for (auto&& i : request->GetWait()) {
            auto it = Cache.Find(i);
            if (it == Cache.End()) {
                addressesToAsk.emplace_back(i);
            } else {
                AFL_VERIFY(objectsResult.emplace(i, it.Value()).second);
            }
        }
        for (auto&& i : objectsResult) {
            request->AddResult(i.first, std::move(i.second));
        }
        if (request->GetWait().empty()) {
            return;
        }
        RequestsQueue.emplace_back(request);
        DrainQueue();
    }

    void OnAdditionalObjectsInfo(THashMap<TAddress, TObject>&& objects) {
        AFL_WARN(NKikimrServices::GENERAL_CACHE)("event", "objects_info");
        for (auto&& i : objects) {
            auto it = Requests.find(i.first);
            if (it != Requests.end()) {
                Cache.Insert(i.first, i.second);
                for (auto&& r : it->second) {
                    r->AddResult(i.first, i.second);
                }
                Requests.erase(it);
            }
        }
    }

    void OnRequestResult(THashMap<TAddress, TObject>&& objects, THashSet<TAddress>&& removed, THashMap<TAddress, TString>&& failed) {
        AFL_WARN(NKikimrServices::GENERAL_CACHE)("event", "on_result");
        RequestedObjectsInFlight.Sub(objects.size() + removed.size() + failed.size());
        for (auto&& i : objects) {
            auto it = Requests.find(i.first);
            AFL_VERIFY(it != Requests.end());
            Cache.Insert(i.first, i.second);
            for (auto&& r : it->second) {
                r->AddResult(i.first, i.second);
            }
            Requests.erase(it);
        }
        for (auto&& i : removed) {
            auto it = Requests.find(i);
            AFL_VERIFY(it != Requests.end());
            for (auto&& r : it->second) {
                r->AddRemoved(i);
            }
            Requests.erase(it);
        }
        for (auto&& i : failed) {
            auto it = Requests.find(i.first);
            AFL_VERIFY(it != Requests.end());
            for (auto&& r : it->second) {
                r->AddError(i.first, i.second);
            }
            Requests.erase(it);
        }
        DrainQueue();
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
