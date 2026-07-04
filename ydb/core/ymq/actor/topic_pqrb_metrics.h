#pragma once

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSQS {

inline bool HasTopicSqsActionMetrics(const NKikimrPQ::TEvTopicSqsActionMetrics& metrics) {
    return metrics.GetSendMessageCount()
        || metrics.GetBytesWritten()
        || metrics.GetDeduplicationCount()
        || metrics.GetDeleteMessageCount()
        || metrics.GetReceiveMessageCount()
        || metrics.GetReceiveMessageBytesRead()
        || metrics.GetReceiveMessageEmptyCount();
}

template<typename TActor>
void SendTopicSqsActionMetricsToPqrb(
    [[maybe_unused]] TActor& actor,
    ui64 balancerTabletId,
    const NKikimrPQ::TEvTopicSqsActionMetrics& metrics
) {
    if (balancerTabletId == 0 || !HasTopicSqsActionMetrics(metrics)) {
        return;
    }

    auto ev = MakeHolder<TEvPQ::TEvTopicSqsActionMetrics>();
    ev->Record = metrics;
    NActors::TActivationContext::Send(
        MakePipePerNodeCacheID(false),
        std::unique_ptr<IEventBase>(new TEvPipeCache::TEvForward(ev.Release(), balancerTabletId, true, balancerTabletId))
    );
}

} // namespace NKikimr::NSQS
