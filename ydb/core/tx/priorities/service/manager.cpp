#include "manager.h"

#include <ydb/core/tx/priorities/usage/abstract.h>
#include <ydb/core/tx/priorities/usage/events.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPrioritiesQueue {

void TManager::AllocateNext() {
    while (WaitingQueue.size() && UsedCount + WaitingQueue.begin()->second.GetSize() <= Config.GetLimit()) {
        auto& waitRequest = WaitingQueue.begin()->second;
        auto it = Clients.find(waitRequest.GetClientId());
        AFL_VERIFY(it != Clients.end());
        UsedCount += waitRequest.GetSize();
        it->second.MutableCount() += waitRequest.GetSize();
        it->second.SetLastPriority(std::nullopt);
        waitRequest.GetRequest()->OnAllocated(std::make_shared<TAllocationGuard>(ServiceActorId, waitRequest.GetClientId(), waitRequest.GetSize()));
        WaitingQueue.erase(WaitingQueue.begin());
    }
    Counters->QueueSize->Set(WaitingQueue.size());
    Counters->UsedCount->Set(UsedCount);
}

void TManager::RemoveFromQueue(const TClientStatus& client) {
    if (!client.GetLastPriority()) {
        return;
    }
    AFL_VERIFY(WaitingQueue.erase(*client.GetLastPriority()));
    Counters->QueueSize->Set(WaitingQueue.size());
}

void TManager::Free(const ui64 clientId, const ui32 count) {
    auto it = Clients.find(clientId);
    if (it == Clients.end()) {
        Counters->FreeNoClient->Inc();
        return;
    }
    Counters->Free->Inc();
    AFL_VERIFY(it->second.GetCount() <= UsedCount);
    AFL_VERIFY(count <= it->second.GetCount());
    it->second.MutableCount() -= count;
    UsedCount -= count;
    AllocateNext();
}

void TManager::Ask(const ui64 clientId, const ui32 count, const std::shared_ptr<IRequest>& request, const ui64 extPriority) {
    AFL_VERIFY(request);
    Counters->Ask->Inc();
    auto it = Clients.find(clientId);
    AFL_VERIFY(it != Clients.end());
    RemoveFromQueue(it->second);
    AFL_VERIFY(count <= Config.GetLimit())("requested", count)("limit", Config.GetLimit());
    TPriority priority(extPriority);
    it->second.SetLastPriority(priority);
    AFL_VERIFY(WaitingQueue.emplace(priority, TAskRequest(clientId, request, count)).second);
    AllocateNext();
}

void TManager::RegisterClient(const ui64 clientId) {
    Counters->Register->Inc();
    AFL_VERIFY(Clients.emplace(clientId, TClientStatus(clientId)).second);
    Counters->Clients->Set(Clients.size());
}

void TManager::UnregisterClient(const ui64 clientId) {
    Counters->Unregister->Inc();
    auto it = Clients.find(clientId);
    AFL_VERIFY(it != Clients.end());
    AFL_VERIFY(it->second.GetCount() <= UsedCount);
    UsedCount -= it->second.GetCount();
    RemoveFromQueue(it->second);
    Clients.erase(it);
    AllocateNext();
    Counters->Clients->Set(Clients.size());
}

TManager::TManager(const std::shared_ptr<TCounters>& counters, const TConfig& config, const NActors::TActorId& serviceActorId)
    : Counters(counters)
    , Config(config)
    , ServiceActorId(serviceActorId) {
    AFL_VERIFY(Counters);
}

}   // namespace NKikimr::NPrioritiesQueue
