#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ {

namespace NQuoterEvents {

struct TEvQuotaUpdated : public TEventLocal<TEvQuotaUpdated, TEvPQ::EvQuotaUpdated> {
    TEvQuotaUpdated(TVector<std::pair<TString, ui64>> updatedConsumerQuotas, ui64 updatedTotalPartitionReadQuota)
        : UpdatedConsumerQuotas(std::move(updatedConsumerQuotas)),
        UpdatedTotalPartitionReadQuota(updatedTotalPartitionReadQuota)
    {}

    TVector<std::pair<TString, ui64>> UpdatedConsumerQuotas;
    ui64 UpdatedTotalPartitionReadQuota;
};

struct TEvAccountQuotaCountersUpdated : public TEventLocal<TEvAccountQuotaCountersUpdated, TEvPQ::EvAccountQuotaCountersUpdated> {
    TEvAccountQuotaCountersUpdated(TAutoPtr<TTabletCountersBase> counters)
        : AccountQuotaCounters(std::move(counters))
    {}

    TAutoPtr<TTabletCountersBase> AccountQuotaCounters;
};

struct TEvQuotaCountersUpdated : public TEventLocal<TEvQuotaCountersUpdated, TEvPQ::EvQuotaCountersUpdated> {
    TEvQuotaCountersUpdated() = default;

    static TEvQuotaCountersUpdated* ReadCounters(ui32 avgInflightLimitThrottledMicroseconds) {
        auto* result = new TEvQuotaCountersUpdated{};
        result->AvgInflightLimitThrottledMicroseconds = avgInflightLimitThrottledMicroseconds;
        result->ForWriteQuota = false;
        return result;
    }

    static TEvQuotaCountersUpdated* WriteCounters(ui64 totalPartitionWriteSpeed) {
        auto result = new TEvQuotaCountersUpdated{};
        result->TotalPartitionWriteSpeed = totalPartitionWriteSpeed;
        result->ForWriteQuota = true;
        return result;
    }

    bool ForWriteQuota;
    ui64 TotalPartitionWriteSpeed;
    ui32 AvgInflightLimitThrottledMicroseconds;

};

} // namespace NQuoterEvents

NActors::IActor* CreateReadQuoter(
    const NKikimrPQ::TPQConfig& pqConfig,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionId& partition,
    TActorId tabletActor,
    const TActorId& parent,
    ui64 tabletId,
    const std::shared_ptr<TTabletCountersBase>& counters
);

NActors::IActor* CreateWriteQuoter(
    const NKikimrPQ::TPQConfig& pqConfig,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionId& partition,
    TActorId tabletActor,
    ui64 tabletId,
    const std::shared_ptr<TTabletCountersBase>& counters
);

} // namespace NKikimr::NPQ
