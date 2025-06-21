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
    void AddResult(const TAddress& addr, const TObject& obj) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Result.emplace(addr, obj).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed));
        }
    }

    void AddRemoved(const TAddress& addr) {
        AFL_VERIFY(Wait.erase(addr));
        AFL_VERIFY(Removed.emplace(addr).second);
        if (Wait.empty()) {
            Callback->OnResultReady(std::move(Result), std::move(Removed));
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
    using TRequest = TRequest<TPolicy>;

    const NPublic::TConfig Config;
    const TString CacheName = TPolicy::GetCacheName();
    TCounters Counters;
    std::shared_ptr<NSource::IObjectsProcessor<TAddress>> ObjectsProcessor;
    TLRUCache<TAddress, TObject, TNoopDelete, typename TPolicy::TSizeCalcer> Cache;
    THashMap<TAddress, std::vector<std::shared_ptr<TRequest>>> Requests;
    std::deque<std::shared_ptr<TRequest>> RequestsQueue;
    TPositiveControlInteger RequestedObjectsInFlight;

    void DrainQueue() {
        THashSet<TAddress> requestedAddresses;
        while (RequestsQueue.size() && RequestedObjectsInFlight.Val() < Config.GetDirectInflightLimit()) {
            auto request = std::move(RequestsQueue.front());
            RequestsQueue.pop_front();
            RequestedObjectsInFlight.Add(request->GetWait().size());
            for (auto&& i : request->GetWait()) {
                auto it = Requests.find(i);
                if (it == Requests.end()) {
                    it = Requests.emplace(i).first;
                    AFL_VERIFY(requestedAddresses.emplace(i).second);
                }
                it->second.emplace_back(request);
            }
        }
        ObjectsProcessor->AskData(requestedAddresses, ObjectsProcessor);
    }

public:
    TManager(const NPublic::TConfig& config, const NActors::TActorId& ownerActorId,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& baseCounters)
        : Config(config)
        , Counters(CacheName, baseCounters)
        , ObjectsProcessor(TPolicy::BuildObjectsProcessor(ownerActorId))
        , Cache(Config.GetMemoryLimit()) {
    }

    void AddRequest(const std::shared_ptr<TRequest>& request) {
        std::vector<TAddress> addressesToAsk;
        for (auto&& i : request->GetWait()) {
            auto it = Cache.Find(i);
            if (it == Cache.End()) {
                addressesToAsk.emplace_back(i);
            } else {
                request->AddResult(it.Value());
            }
        }
        if (request->IsFinished()) {
            return;
        }
        RequestsQueue.emplace_back(request);
        DrainQueue();
    }

    void ModifyObjects(THashMap<TAddress, TObject>&& objects, THashSet<TAddress>&& removed, THashMap<TAddress, TString>&& failed) {
        RequestedObjectsInFlight.Sub(objects.size() + removed.size());
        for (auto&& i : objects) {
            auto it = Requests.find(i.first);
            AFL_VERIFY(it != Requests.end());
            Cache.Insert(i.first, i.second);
            for (auto&& r : it->second) {
                r->AddResult(i.second, i.second);
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
