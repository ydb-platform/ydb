#include "worker_lag_expiry_queue.h"

#include <util/system/yassert.h>

#include <iterator>

namespace NKikimr::NReplication::NController {

void TWorkerLagExpiryQueue::SetDeadline(const TWorkerId& id, TDeadline deadline) {
    Y_DEBUG_ABORT_UNLESS(Expirations.empty() || deadline >= Expirations.back().first);
    const auto it = WorkerEntries.find(id);
    if (it != WorkerEntries.end()) {
        it->second->first = deadline;
        Expirations.splice(Expirations.end(), Expirations, it->second);
    } else {
        Expirations.emplace_back(deadline, id);
        WorkerEntries.emplace(id, std::prev(Expirations.end()));
    }
}

void TWorkerLagExpiryQueue::Remove(const TWorkerId& id) {
    const auto it = WorkerEntries.find(id);
    if (it == WorkerEntries.end()) {
        return;
    }

    Expirations.erase(it->second);
    WorkerEntries.erase(it);
}

std::optional<TWorkerLagExpiryQueue::TDeadline> TWorkerLagExpiryQueue::GetNextDeadline() const {
    if (Expirations.empty()) {
        return std::nullopt;
    }

    return Expirations.front().first;
}

std::optional<TWorkerId> TWorkerLagExpiryQueue::PopExpired(TDeadline now) {
    if (Expirations.empty() || Expirations.front().first > now) {
        return std::nullopt;
    }

    const auto id = Expirations.front().second;
    WorkerEntries.erase(id);
    Expirations.pop_front();
    return id;
}

size_t TWorkerLagExpiryQueue::GetSize() const {
    return WorkerEntries.size();
}

void TWorkerLagExpiryQueue::Clear() {
    WorkerEntries.clear();
    Expirations.clear();
}

}
