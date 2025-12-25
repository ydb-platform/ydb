#pragma once

#include "quota.h"
#include "quota_tracker.h"
#include "account_read_quoter.h"
#include <ydb/core/persqueue/common/microseconds_sliding_window.h>

#include <ydb/core/quoter/public/quoter.h>

#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr {
namespace NPQ {

struct TRequestContext {
    THolder<TEvPQ::TEvRequestQuota> Request;
    TDuration AccountQuotaWaitTime;
    TInstant PartitionQuotaWaitStart;
    TDuration TotalQuotaWaitTime;
    TActorId PartitionActor;

    TRequestContext();
    TRequestContext(THolder<TEvPQ::TEvRequestQuota>&& request, const TActorId& partitionActor);
    TRequestContext(THolder<TEvPQ::TEvRequestQuota>&& request, const TActorId& partitionActor, const TDuration& accountWaitTime, TInstant now);
};

struct TAccountQuoterHolder {
    TAccountQuoterHolder(const TActorId& actor, const TTabletCountersBase& baseline);

    TActorId Actor;
    TTabletCountersBase Baseline;
};

class TConsumerReadQuota {
public:
    TConsumerReadQuota(THolder<TAccountQuoterHolder> accountQuotaTracker, ui64 readQuotaBurst, ui64 readQuotaSpeed);

public:
    TQuotaTracker PartitionPerConsumerQuotaTracker;
    THolder<TAccountQuoterHolder> AccountQuotaTracker;
    std::deque<TRequestContext> ReadRequests;
};


class TPartitionQuoterBase : public TBaseTabletActor<TPartitionQuoterBase>
                           , private TConstantLogPrefix {

const TDuration WAKE_UP_TIMEOUT = TDuration::Seconds(1);

public:
    TPartitionQuoterBase(
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const NKikimrPQ::TPQTabletConfig& config,
        const TPartitionId& partition,
        TActorId tabletActor,
        bool totalPartitionQuotaEnabled,
        ui64 tabletId,
        const std::shared_ptr<TTabletCountersBase>& counters,
        ui64 maxRequestsInflight
    );

public:

    virtual void Bootstrap(const TActorContext &ctx);

    void HandleQuotaRequestOnInit(TEvPQ::TEvRequestQuota::TPtr& ev,const TActorContext& ctx);
    void HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev,const TActorContext& ctx);
    void HandleAccountQuotaApproved(NAccountQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleWakeUp(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev, const TActorContext& ctx);
    void HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx);
    virtual void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) = 0;
    virtual void HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx) = 0;
    void HandleAcquireExclusiveLock(TEvPQ::TEvAcquireExclusiveLock::TPtr& ev, const TActorContext& ctx);
    void HandleReleaseExclusiveLock(TEvPQ::TEvReleaseExclusiveLock::TPtr& ev, const TActorContext& ctx);

protected:
    virtual void HandleQuotaRequestImpl(TRequestContext& context) = 0;
    virtual void HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) = 0;

    virtual TAccountQuoterHolder* GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) = 0;
    virtual void OnAccountQuotaApproved(TRequestContext&& context) = 0;
    virtual IEventBase* MakeQuotaApprovedEvent(TRequestContext& context) = 0;
    virtual void HandleWakeUpImpl() = 0;

    virtual void ProcessEventImpl(TAutoPtr<IEventHandle>& ev) = 0;
    virtual void UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) = 0;
    virtual void UpdateCounters(const TActorContext& ctx) = 0;
    virtual ui64 GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const = 0;
    virtual ui64 GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const = 0;
    virtual TString Description() const = 0;

    virtual bool CanExaust(TInstant now);

protected:
    void CheckTotalPartitionQuota(TRequestContext&& context);
    void ApproveQuota(TRequestContext& context);
    TQuotaTracker CreatePartitionTotalQuotaTracker(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const;

    inline const TActorId& GetTabletActor() const {return TabletActorId;}
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
            HFunc(TEvPQ::TEvConsumed, HandleConsumed);
            HFunc(TEvents::TEvWakeup, HandleWakeUp);
            HFunc(NAccountQuoterEvents::TEvResponse, HandleAccountQuotaApproved);
            HFunc(TEvPQ::TEvChangePartitionConfig, HandleConfigUpdate);
            HFunc(NAccountQuoterEvents::TEvCounters, HandleUpdateAccountQuotaCounters);
            HFunc(TEvPQ::TEvAcquireExclusiveLock, HandleAcquireExclusiveLock);
            HFunc(TEvPQ::TEvReleaseExclusiveLock, HandleReleaseExclusiveLock);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        default:
            ProcessEventImpl(ev);
            break;
        };
    }

    void ReplyExclusiveLockAcquired(const TActorId& receiver);

protected:
    const TPartitionId Partition;
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
    enum class EExclusiveLockState {
        EReleased,
        EAcquiring,
        EAcquired,
    };

    std::deque<TRequestContext> WaitingTotalPartitionQuotaRequests;
    THashMap<ui64, TRequestContext> PendingAccountQuotaRequests;
    ui64 MaxInflightRequests;
    bool TotalPartitionQuotaEnabled;
    TVector<TEvPQ::TEvRequestQuota::TPtr> PendingQuotaRequests;
    EExclusiveLockState ExclusiveLockState = EExclusiveLockState::EReleased;
    TActorId ExclusiveLockRequester;
};

}// NPQ
}// NKikimr
