#pragma once

#include <ydb/core/tx/general_cache/usage/config.h>

#include <util/generic/hash.h>

#include <deque>
#include <map>
#include <optional>
#include <vector>

namespace NKikimr::NGeneralCache::NPrivate {

struct TWeightedQueueState {
    ui64 QueueId = 0;
    ui32 Weight = 0;
    ui64 Inflight = 0;
    ui64 LastServed = 0;
};

struct TWeightedPick {
    ui64 QueueId = 0;
    ui64 Budget = 0;
};

// The queue with the lowest Inflight / Weight goes first, a tie goes to the queue served longest ago.
// It may send up to its share of the source limit, at least one address.
std::optional<TWeightedPick> PickWeightedQueue(const std::vector<TWeightedQueueState>& waiting, const ui64 sourceLimit);

template <class TAddress, class TRequestPtr>
class TWeightedQueues {
private:
    struct TQueue {
        ui32 Weight = 0;
        ui64 Inflight = 0;
        ui64 LastServed = 0;
        std::deque<TRequestPtr> Requests;
    };

    std::map<ui64, TQueue> Queues;
    THashMap<TAddress, ui64> InflightOwners;
    ui64 ServeClock = 0;

public:
    void Enqueue(const NPublic::TCacheRequestClass& requestClass, const TRequestPtr& request) {
        auto [it, inserted] = Queues.try_emplace(requestClass.QueueId);
        if (inserted) {
            it->second.Weight = requestClass.Weight;
        }
        it->second.Requests.emplace_back(request);
    }

    bool HasWaiting() const {
        for (const auto& [queueId, queue] : Queues) {
            Y_UNUSED(queueId);
            if (!queue.Requests.empty()) {
                return true;
            }
        }
        return false;
    }

    std::optional<TWeightedPick> Pick(const ui64 sourceLimit) {
        std::vector<TWeightedQueueState> waiting;
        for (const auto& [queueId, queue] : Queues) {
            if (!queue.Requests.empty()) {
                waiting.push_back(TWeightedQueueState{ queueId, queue.Weight, queue.Inflight, queue.LastServed });
            }
        }
        const auto pick = PickWeightedQueue(waiting, sourceLimit);
        if (pick) {
            Queues[pick->QueueId].LastServed = ++ServeClock;
        }
        return pick;
    }

    std::deque<TRequestPtr>& MutableRequests(const ui64 queueId) {
        return Queues[queueId].Requests;
    }

    void OnSent(const ui64 queueId, const TAddress& address) {
        InflightOwners.emplace(address, queueId);
        ++Queues[queueId].Inflight;
    }

    void OnFinished(const TAddress& address) {
        const auto owner = InflightOwners.find(address);
        if (owner == InflightOwners.end()) {
            return;
        }
        const auto queue = Queues.find(owner->second);
        InflightOwners.erase(owner);
        if (queue == Queues.end()) {
            return;
        }
        if (queue->second.Inflight) {
            --queue->second.Inflight;
        }
        if (queue->second.Requests.empty() && !queue->second.Inflight) {
            Queues.erase(queue);
        }
    }

    void DropIdle() {
        for (auto it = Queues.begin(); it != Queues.end();) {
            if (it->second.Requests.empty() && !it->second.Inflight) {
                it = Queues.erase(it);
            } else {
                ++it;
            }
        }
    }

    template <class TFunc>
    void ForEachRequestQueue(TFunc&& func) {
        for (auto& [queueId, queue] : Queues) {
            Y_UNUSED(queueId);
            func(queue.Requests);
        }
    }

    void Clear() {
        Queues.clear();
        InflightOwners.clear();
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
