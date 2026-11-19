#pragma once

#include <ydb/core/tx/general_cache/usage/config.h>

#include <util/generic/hash.h>

#include <deque>
#include <map>
#include <optional>
#include <vector>

namespace NKikimr::NGeneralCache::NPrivate {

class TRequestQueuesBase {
protected:
    struct TQueueState {
        ui64 QueueId = 0;
        ui32 Weight = 0;
        ui64 Inflight = 0;
        ui64 LastServed = 0;
    };

    struct TPick {
        ui64 QueueId = 0;
        ui64 Budget = 0;
    };

    // The queue with the lowest Inflight / Weight goes first, a tie goes to the queue served longest ago.
    // It may send up to its share of the source limit, at least one address.
    static std::optional<TPick> Pick(const std::vector<TQueueState>& waiting, const ui64 sourceLimit);
};

template <class TAddress, class TRequestPtr>
class TRequestQueues: private TRequestQueuesBase {
private:
    struct TQueue {
        ui32 Weight = 0;
        ui64 Inflight = 0;
        ui64 LastServed = 0;
        std::deque<TRequestPtr> Requests;
    };

    std::map<ui64, TQueue> Queues;
    std::deque<TRequestPtr> UnweightedQueue;
    THashMap<TAddress, ui64> InflightOwners;
    ui64 ServeClock = 0;

    bool HasWeightedWaiting() const {
        for (const auto& [queueId, queue] : Queues) {
            Y_UNUSED(queueId);
            if (!queue.Requests.empty()) {
                return true;
            }
        }
        return false;
    }

    std::optional<TPick> PickWeighted(const ui64 sourceLimit) const {
        std::vector<TQueueState> waiting;
        for (const auto& [queueId, queue] : Queues) {
            if (!queue.Requests.empty()) {
                waiting.push_back(TQueueState{ queueId, queue.Weight, queue.Inflight, queue.LastServed });
            }
        }
        return Pick(waiting, sourceLimit);
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

public:
    void Enqueue(const NPublic::TCacheRequestClass& requestClass, const TRequestPtr& request) {
        if (!requestClass.Weight) {
            UnweightedQueue.emplace_back(request);
            return;
        }
        auto [it, inserted] = Queues.try_emplace(requestClass.QueueId);
        if (inserted) {
            it->second.Weight = requestClass.Weight;
        }
        it->second.Requests.emplace_back(request);
    }

    // freeSlots() returns how many more addresses the source may send now.
    // dispatch(requests, budget, weightedQueueId) sends up to budget new addresses of the front request
    // and returns the number of new addresses and whether the request was put back.
    template <class TFreeSlots, class TDispatch>
    void Drain(const ui64 sourceLimit, const bool chunked, TFreeSlots&& freeSlots, TDispatch&& dispatch) {
        while (const ui64 free = freeSlots()) {
            if (const auto chosen = PickWeighted(sourceLimit)) {
                auto& queue = Queues[chosen->QueueId];
                const auto dispatched = dispatch(queue.Requests, std::min(chosen->Budget, free), std::optional<ui64>(chosen->QueueId));
                if (dispatched.NewAddresses) {
                    queue.LastServed = ++ServeClock;
                }
                continue;
            }
            // Weight 0 sends nothing while a weighted consumer has queued requests.
            if (HasWeightedWaiting() || UnweightedQueue.empty()) {
                break;
            }
            // Without weights in the config a request is sent whole and the limit is checked between requests only.
            const auto dispatched = dispatch(UnweightedQueue, chunked ? free : Max<ui64>(), std::optional<ui64>());
            if (dispatched.PutBack) {
                break;
            }
        }
        DropIdle();
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

    template <class TFunc>
    void Abort(TFunc&& abortRequests) {
        for (auto& [queueId, queue] : Queues) {
            Y_UNUSED(queueId);
            abortRequests(queue.Requests);
        }
        abortRequests(UnweightedQueue);
        Queues.clear();
        UnweightedQueue.clear();
        InflightOwners.clear();
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
