#pragma once

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/events/topic_sqs_action_metrics.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NSQS {

inline bool HasTopicSqsActionMetrics(const NKikimrPQ::TEvTopicSqsActionMetrics& metrics) {
    return NPQ::HasTopicSqsActionMetrics(metrics);
}

inline void SendTopicSqsActionMetricsToPqrb(
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

template<typename TActor>
void SendTopicSqsActionMetricsToPqrb(
    [[maybe_unused]] TActor& actor,
    ui64 balancerTabletId,
    const NKikimrPQ::TEvTopicSqsActionMetrics& metrics
) {
    Y_UNUSED(actor);
    SendTopicSqsActionMetricsToPqrb(balancerTabletId, metrics);
}

NActors::IActor* CreateTopicPqrbMetricsSender(
    TString databasePath,
    TString topicPath,
    NKikimrPQ::TEvTopicSqsActionMetrics metrics
);

void SendTopicPqrbMetrics(
    ui64 balancerTabletId,
    const TString& databasePath,
    const TString& topicPath,
    NKikimrPQ::TEvTopicSqsActionMetrics metrics
);

} // namespace NKikimr::NSQS
