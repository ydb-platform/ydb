#pragma once

#include "quota_tracker.h"
#include "account_read_quoter.h"
#include "user_info.h"
#include "microseconds_sliding_window.h"

#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/persqueue/events/internal.h>

#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr {
namespace NPQ {

namespace NReadQuoterEvents {

struct TEvQuotaUpdated : public TEventLocal<TEvQuotaUpdated, TEvPQ::EvQuotaUpdated> {
    TEvQuotaUpdated(TVector<std::pair<TString, ui64>> updatedConsumerQuotas, ui64 updatedTotalPartitionQuota)
        : UpdatedConsumerQuotas(std::move(updatedConsumerQuotas)),
        UpdatedTotalPartitionQuota(updatedTotalPartitionQuota)
    {}

    TVector<std::pair<TString, ui64>> UpdatedConsumerQuotas;
    ui64 UpdatedTotalPartitionQuota;
};

struct TEvAccountQuotaCountersUpdated : public TEventLocal<TEvAccountQuotaCountersUpdated, TEvPQ::EvAccountQuotaCountersUpdated> {
    TEvAccountQuotaCountersUpdated(TAutoPtr<TTabletCountersBase> counters)
        : AccountQuotaCounters(std::move(counters))
    {}

    TAutoPtr<TTabletCountersBase> AccountQuotaCounters;
};

struct TEvQuotaCountersUpdated : public TEventLocal<TEvQuotaCountersUpdated, TEvPQ::EvQuotaCountersUpdated> {
    TEvQuotaCountersUpdated(ui32 avgInflightLimitThrottledMicroseconds)
        : AvgInflightLimitThrottledMicroseconds(avgInflightLimitThrottledMicroseconds)
    {}

    ui32 AvgInflightLimitThrottledMicroseconds;
};

}// NReadQuoterEvents

struct TAccountReadQuoterHolder {
    TAccountReadQuoterHolder(const TActorId& actor, const TTabletCountersBase& baseline)
        : Actor(actor)
    {
        Baseline.Populate(baseline);
    }

    TActorId Actor;
    TTabletCountersBase Baseline;
};

class TConsumerReadQuota {
    public:
        TConsumerReadQuota(THolder<TAccountReadQuoterHolder> accountQuotaTracker, ui64 readQuotaBurst, ui64 readQuotaSpeed):
            PartitionPerConsumerQuotaTracker(readQuotaBurst, readQuotaSpeed, TAppData::TimeProvider->Now()),
            AccountQuotaTracker(std::move(accountQuotaTracker))
        { }
    public:
        TQuotaTracker PartitionPerConsumerQuotaTracker;
        THolder<TAccountReadQuoterHolder> AccountQuotaTracker;
        std::deque<TEvPQ::TEvRead::TPtr> ReadRequests;
};

class TReadQuoter : public TActorBootstrapped<TReadQuoter> {

const TDuration WAKE_UP_TIMEOUT = TDuration::Seconds(1);
const ui64 DEFAULT_READ_SPEED_AND_BURST = 1'000'000'000;

public:
    TReadQuoter(
        const TActorContext& ctx,
        TActorId partitionActor,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        ui32 partition,
        TActorId tabletActor,
        ui64 tabletId,
        const TTabletCountersBase& counters
    ):
    TabletActor(tabletActor),
    PartitionActor(partitionActor),
    PQTabletConfig(config),
    PartitionTotalQuotaTracker(CreatePartitionTotalQuotaTracker(ctx)),
    TopicConverter(topicConverter),
    Partition(partition),
    TabletId(tabletId),
    RequestsInflight(0),
    InflightLimitSlidingWindow(1000, TDuration::Minutes(1))
    {
        Counters.Populate(counters);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvRequestQuota, HandleQuotaRequest);
            HFunc(TEvents::TEvWakeup, HandleWakeUp);
            HFunc(NAccountReadQuoterEvents::TEvResponse, HandleAccountQuotaApproved);
            HFunc(TEvPQ::TEvConsumed, HandleConsumed);
            HFunc(TEvPQ::TEvChangePartitionConfig, HandleConfigUpdate);
            HFunc(NAccountReadQuoterEvents::TEvCounters, HandleUpdateAccountQuotaCounters);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(TEvPQ::TEvConsumerRemoved, HandleConsumerRemoved);
        default:
            break;
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_READ_QUOTER;
    }

    void Bootstrap(const TActorContext &ctx);

    void HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev,const TActorContext& ctx);
    void HandleAccountQuotaApproved(NAccountReadQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleWakeUp(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev, const TActorContext& ctx);
    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx);
    void HandleUpdateAccountQuotaCounters(NAccountReadQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx);
    void HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const TActorContext& ctx);

private:
    TConsumerReadQuota* GetOrCreateConsumerQuota(const TString& consumerStr, const TActorContext& ctx);
    THolder<TAccountReadQuoterHolder> CreateAccountQuotaTracker(const TString& user) const;
    TQuotaTracker CreatePartitionTotalQuotaTracker(const TActorContext& ctx) const;
    void StartQuoting(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx);
    void CheckConsumerPerPartitionQuota(TEvPQ::TEvRead::TPtr, const TActorContext& ctx);
    void CheckTotalPartitionQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx);
    void ApproveQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx);
    void ScheduleWakeUp(const TActorContext& ctx);
    void UpdateConsumersWithCustomQuota(const TActorContext &ctx);
    void UpdateQuota(const TActorContext &ctx);
    void ProcessInflightQueue(const TActorContext& ctx);
    void ProcessPerConsumerQuotaQueue(const TActorContext& ctx);
    void ProcessPartititonTotalQuotaQueue(const TActorContext& ctx);
    void UpdateCounters(const TActorContext& ctx);
    TConsumerReadQuota* GetConsumerQuotaIfExists(const TString& consumerStr);
    ui64 GetConsumerReadSpeed(const TActorContext& ctx) const;
    ui64 GetConsumerReadBurst(const TActorContext& ctx) const;
    ui64 GetTotalPartitionReadSpeed(const TActorContext& ctx) const;
    ui64 GetTotalPartitionReadBurst(const TActorContext& ctx) const;

private:
    TActorId TabletActor;
    TActorId PartitionActor;
    THashMap<TString, TConsumerReadQuota> ConsumerQuotas;
    THashMap<ui64, TInstant> QuotaRequestedTimes;
    std::deque<TEvPQ::TEvRead::TPtr> WaitingTotalPartitionQuotaReadRequests;
    NKikimrPQ::TPQTabletConfig PQTabletConfig;
    TQuotaTracker PartitionTotalQuotaTracker;
    NPersQueue::TTopicConverterPtr TopicConverter;
    const ui32 Partition;
    ui64 TabletId;
    TTabletCountersBase Counters;
    ui32 RequestsInflight;
    std::deque<TEvPQ::TEvRead::TPtr> WaitingInflightReadRequests;
    TMicrosecondsSlidingWindow InflightLimitSlidingWindow;
    TInstant InflightIsFullStartTime;
};




}// NPQ
}// NKikimr
