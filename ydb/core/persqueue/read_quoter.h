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

}// NReadQuoterEvents

struct TRequestContext {
    THolder<TEvPQ::TEvRequestQuota> Request;
    TDuration AccountQuotaWaitTime;
    TInstant PartitionQuotaWaitStart;
    TDuration TotalQuotaWaitTime;
    TActorId PartitionActor;
    
    TRequestContext() = default;
    TRequestContext(THolder<TEvPQ::TEvRequestQuota>&& request, const TActorId& partitionActor)
        : Request(std::move(request))
        , PartitionActor(partitionActor)
    {}
    TRequestContext(THolder<TEvPQ::TEvRequestQuota>&& request, const TActorId& partitionActor, const TDuration& accountWaitTime, TInstant now)
        : Request(std::move(request))
        , AccountQuotaWaitTime(accountWaitTime)
        , PartitionQuotaWaitStart(std::move(now))
        , PartitionActor(partitionActor)
    {}
};

struct TAccountQuoterHolder {
    TAccountQuoterHolder(const TActorId& actor, const TTabletCountersBase& baseline)
        : Actor(actor)
    {
        Baseline.Populate(baseline);
    }

    TActorId Actor;
    TTabletCountersBase Baseline;
};

class TConsumerReadQuota {
    public:
        TConsumerReadQuota(THolder<TAccountQuoterHolder> accountQuotaTracker, ui64 readQuotaBurst, ui64 readQuotaSpeed):
            PartitionPerConsumerQuotaTracker(readQuotaBurst, readQuotaSpeed, TAppData::TimeProvider->Now()),
            AccountQuotaTracker(std::move(accountQuotaTracker))
        { }
    public:
        TQuotaTracker PartitionPerConsumerQuotaTracker;
        THolder<TAccountQuoterHolder> AccountQuotaTracker;
        std::deque<TRequestContext> ReadRequests;
};

class TPartitionQuoterBase : public TActorBootstrapped<TPartitionQuoterBase> {

const TDuration WAKE_UP_TIMEOUT = TDuration::Seconds(1);

public:
    TPartitionQuoterBase(
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        const TPartitionId& partition,
        TActorId tabletActor,
        const TActorId& parent,
        TMaybe<TQuotaTracker>&& partitionTotalQuotaTracker,
        ui64 tabletId,
        const TTabletCountersBase& counters,
        ui64 maxRequestsInflight
    )
        : InflightLimitSlidingWindow(1000, TDuration::Minutes(1))
        , RequestsInflight(0)
        , PQTabletConfig(config)
        , PartitionTotalQuotaTracker(std::move(partitionTotalQuotaTracker))
        , TopicConverter(topicConverter)
        , TabletActor(tabletActor)
        , Parent(parent)
        , Partition(partition)
        , TabletId(tabletId)
        , MaxInflightRequests(maxRequestsInflight)
    {
        Counters.Populate(counters);
    }

public:
    
    void Bootstrap(const TActorContext &ctx);

    void HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev,const TActorContext& ctx);
    void HandleAccountQuotaApproved(NAccountQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleWakeUp(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev, const TActorContext& ctx);
    void HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx);
    virtual void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) = 0;
    virtual void HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx) = 0;

protected:
    virtual void HandleQuotaRequestImpl(TRequestContext& context) = 0;
    virtual void HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) = 0;

    virtual THolder<TAccountQuoterHolder>& GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) = 0;
    virtual void OnAccountQuotaApproved(TRequestContext&& context) = 0;
    virtual IEventBase* MakeQuotaApprovedEvent(TRequestContext& context) = 0;
    virtual void HandleWakeUpImpl() = 0;

    virtual void ProcessEventImpl(TAutoPtr<IEventHandle>& ev) = 0;
    virtual void UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) = 0;
    virtual void UpdateCounters(const TActorContext& ctx) = 0;
    virtual ui64 GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const = 0;
    virtual ui64 GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const = 0;
    virtual THolder<TAccountQuoterHolder> CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const = 0;
    
protected:
    void CheckTotalPartitionQuota(TRequestContext&& context);
    void ApproveQuota(TRequestContext& context);
    TQuotaTracker CreatePartitionTotalQuotaTracker(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const;

    inline const TActorId& GetParent() const {return Parent;}
    inline const TActorId& GetTabletActor() const {return TabletActor;}
    inline ui64 GetTabletId() const {return TabletId;}
    inline const TPartitionId& GetPartition() const {return Partition;}

private:
    void StartQuoting(TRequestContext&& context);
    void UpdateQuota();
    void ProcessInflightQueue();
    void ProcessPartitionTotalQuotaQueue();
    
    void ScheduleWakeUp(const TActorContext& ctx);
    
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvRequestQuota, HandleQuotaRequest);
            HFunc(TEvents::TEvWakeup, HandleWakeUp);
            HFunc(NAccountQuoterEvents::TEvResponse, HandleAccountQuotaApproved);
            HFunc(TEvPQ::TEvConsumed, HandleConsumed);
            HFunc(TEvPQ::TEvChangePartitionConfig, HandleConfigUpdate);
            HFunc(NAccountQuoterEvents::TEvCounters, HandleUpdateAccountQuotaCounters);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        default:
            ProcessEventImpl(ev);
            break;
        };
    }

protected:
    std::deque<TRequestContext> WaitingInflightRequests;
    TMicrosecondsSlidingWindow InflightLimitSlidingWindow;
    TInstant InflightIsFullStartTime;
    ui32 RequestsInflight;
    THashMap<ui64, TInstant> QuotaRequestedTimes;
    NKikimrPQ::TPQTabletConfig PQTabletConfig;
    TMaybe<TQuotaTracker> PartitionTotalQuotaTracker;
    NPersQueue::TTopicConverterPtr TopicConverter;
    TTabletCountersBase Counters;

private:
    TActorId TabletActor;
    TActorId Parent;
    std::deque<TRequestContext> WaitingTotalPartitionQuotaRequests;
    THashMap<ui64, TRequestContext> PendingAccountQuotaRequests;
    const TPartitionId Partition;
    ui64 TabletId;
    ui64 MaxInflightRequests;

    
};


class TReadQuoter : public TPartitionQuoterBase {

const static ui64 DEFAULT_READ_SPEED_AND_BURST = 1'000'000'000;

public:
    TReadQuoter(
        const TActorContext& ctx,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        const TPartitionId& partition,
        TActorId tabletActor,
        const TActorId& parent,
        ui64 tabletId,
        const TTabletCountersBase& counters
    )
        : TPartitionQuoterBase(
                topicConverter, config, partition, tabletActor, parent,
                CreatePartitionTotalQuotaTracker(config, ctx),
                tabletId, counters, AppData(ctx)->PQConfig.GetMaxInflightReadRequestsPerPartition()
        )
    {
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_READ_QUOTER;
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) override;
    void HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx) override;
    void HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const TActorContext& ctx);
    
    void UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) override;
    IEventBase* MakeQuotaApprovedEvent(TRequestContext& context) override;

protected: 
    void HandleQuotaRequestImpl(TRequestContext& context) override;
    void HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) override;
    THolder<TAccountQuoterHolder>& GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) override;
    void OnAccountQuotaApproved(TRequestContext&& request) override;
    ui64 GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const override;
    ui64 GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const override;
    void UpdateCounters(const TActorContext& ctx) override;
    void HandleWakeUpImpl() override;
    THolder<TAccountQuoterHolder> CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const override;

    STFUNC(ProcessEventImpl) override
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvConsumerRemoved, HandleConsumerRemoved);
        default:
            break;
        };
    }
private:
    TConsumerReadQuota* GetOrCreateConsumerQuota(const TString& consumerStr, const TActorContext& ctx);
    void CheckConsumerPerPartitionQuota(TRequestContext&& context);
    void ApproveQuota(TAutoPtr<TEvPQ::TEvRead>&& ev, const TActorContext& ctx);
    void ProcessPerConsumerQuotaQueue(const TActorContext& ctx);
    TConsumerReadQuota* GetConsumerQuotaIfExists(const TString& consumerStr);
    ui64 GetConsumerReadSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const;
    ui64 GetConsumerReadBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const;

private:
    THashMap<TString, TConsumerReadQuota> ConsumerQuotas;
};


class TWriteQuoter : public TPartitionQuoterBase {
public:
    TWriteQuoter(
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        const TPartitionId& partition,
        TActorId tabletActor,
        const TActorId& parent,
        ui64 tabletId,
        bool isLocalDc,
        const TTabletCountersBase& counters,
        const TActorContext& ctx
    );

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_WRITE_QUOTER;
    }

    void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) override;
    void HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx) override;
    
    void UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) override;
    IEventBase* MakeQuotaApprovedEvent(TRequestContext& context) override;

protected:
    void HandleQuotaRequestImpl(TRequestContext& context) override;
    void HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) override;
    THolder<TAccountQuoterHolder>& GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) override;
    void OnAccountQuotaApproved(TRequestContext&& request) override;
    ui64 GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const override;
    ui64 GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const override;
    void UpdateCounters(const TActorContext& ctx) override;
    void HandleWakeUpImpl() override;
    THolder<TAccountQuoterHolder> CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const override;

    STFUNC(ProcessEventImpl) override
    {
        Y_UNUSED(ev);
    }

private:
    bool IsLocalDC;
    bool QuotingEnabled;
    bool AccountQuotingEnabled; 
    THolder<TAccountQuoterHolder> AccountQuotaTracker;
};


}// NPQ
}// NKikimr
