#include "request_queues.h"

namespace NKikimr::NGeneralCache::NPrivate {

std::optional<TRequestQueuesBase::TPick> TRequestQueuesBase::Pick(const std::vector<TQueueState>& waiting, const ui64 sourceLimit) {
    if (waiting.empty() || !sourceLimit) {
        return std::nullopt;
    }
    const auto goesBefore = [](const TQueueState& a, const TQueueState& b) {
        const ui64 left = a.Inflight * b.Weight;
        const ui64 right = b.Inflight * a.Weight;
        if (left != right) {
            return left < right;
        }
        if (a.LastServed != b.LastServed) {
            return a.LastServed < b.LastServed;
        }
        if (a.Weight != b.Weight) {
            return a.Weight > b.Weight;
        }
        return a.QueueId < b.QueueId;
    };
    ui64 sumWeight = 0;
    const TQueueState* best = nullptr;
    for (const auto& queue : waiting) {
        if (!queue.Weight) {
            continue;
        }
        sumWeight += queue.Weight;
        if (!best || goesBefore(queue, *best)) {
            best = &queue;
        }
    }
    if (!best) {
        return std::nullopt;
    }
    const ui64 share = sourceLimit * best->Weight / sumWeight;
    return TPick{ best->QueueId, share > best->Inflight ? share - best->Inflight : 1 };
}

}   // namespace NKikimr::NGeneralCache::NPrivate
