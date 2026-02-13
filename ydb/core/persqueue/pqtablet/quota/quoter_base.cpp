#include "quoter_base.h"

#include <ydb/core/persqueue/public/constants.h>


namespace NKikimr::NPQ {

TRequestContext::TRequestContext() = default;

TRequestContext::TRequestContext(THolder<TEvPQ::TEvRequestQuota>&& request, const TActorId& partitionActor)
    : Request(std::move(request))
    , PartitionActor(partitionActor)
{
}

TRequestContext::TRequestContext(THolder<TEvPQ::TEvRequestQuota>&& request, const TActorId& partitionActor, const TDuration& accountWaitTime, TInstant now)
    : Request(std::move(request))
    , AccountQuotaWaitTime(accountWaitTime)
    , PartitionQuotaWaitStart(std::move(now))
    , PartitionActor(partitionActor)
{
}


TAccountQuoterHolder::TAccountQuoterHolder(const TActorId& actor, const TTabletCountersBase& baseline)
    : Actor(actor)
{
    Baseline.Populate(baseline);
}


TConsumerReadQuota::TConsumerReadQuota(THolder<TAccountQuoterHolder> accountQuotaTracker, ui64 readQuotaBurst, ui64 readQuotaSpeed)
    : PartitionPerConsumerQuotaTracker(readQuotaBurst, readQuotaSpeed, TAppData::TimeProvider->Now())
    , AccountQuotaTracker(std::move(accountQuotaTracker))
{
}


TPartitionQuoterBase::TPartitionQuoterBase(
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionId& partition,
    TActorId tabletActor,
    bool totalPartitionQuotaEnabled,
    ui64 tabletId,
    const std::shared_ptr<TTabletCountersBase>& counters,
    ui64 maxRequestsInflight
)
    : TBaseTabletActor(tabletId, tabletActor, NKikimrServices::PERSQUEUE)
    , Partition(partition)
    , InflightLimitSlidingWindow(1000, TDuration::Minutes(1))
    , RequestsInflight(0)
    , PQTabletConfig(config)
    , TopicConverter(topicConverter)
    , MaxInflightRequests(maxRequestsInflight)
    , TotalPartitionQuotaEnabled(totalPartitionQuotaEnabled)
{
    Counters.Populate(*counters);
}


void TPartitionQuoterBase::Bootstrap(const TActorContext &ctx) {
    if (TotalPartitionQuotaEnabled)
        PartitionTotalQuotaTracker = CreatePartitionTotalQuotaTracker(PQTabletConfig, ctx);
    Become(&TThis::StateWork);
    ScheduleWakeUp(ctx);
}

void TPartitionQuoterBase::HandleQuotaRequestOnInit(TEvPQ::TEvRequestQuota::TPtr& ev, const TActorContext&) {
    PendingQuotaRequests.emplace_back(ev);
}

void TPartitionQuoterBase::HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev, const TActorContext& ctx) {
    QuotaRequestedTimes.emplace(ev->Get()->Cookie, ctx.Now());
    TRequestContext context{ev->Release(), ev->Sender};
    HandleQuotaRequestImpl(context);
    if ((ExclusiveLockState != EExclusiveLockState::EReleased) || (RequestsInflight >= MaxInflightRequests) || !WaitingInflightRequests.empty()) {
        if (WaitingInflightRequests.empty())
            InflightIsFullStartTime = ctx.Now();
        WaitingInflightRequests.push_back(std::move(context));
    } else {
        StartQuoting(std::move(context));
    }
}

void TPartitionQuoterBase::StartQuoting(TRequestContext&& context) {
    RequestsInflight++;
    auto& request = context.Request;
    auto* accountQuotaTracker = GetAccountQuotaTracker(request);
    if (accountQuotaTracker) {
        Send(accountQuotaTracker->Actor, new NAccountQuoterEvents::TEvRequest(request->Cookie, request->Request.Release()));
        PendingAccountQuotaRequests[request->Cookie] = std::move(context);
    } else {
        context.PartitionQuotaWaitStart = ActorContext().Now();
        OnAccountQuotaApproved(std::move(context));
    }
}

void TPartitionQuoterBase::CheckTotalPartitionQuota(TRequestContext&& context) {
    if (!PartitionTotalQuotaTracker) {
        return ApproveQuota(context);
    }
    if (!CanExaust(ActorContext().Now()) || !WaitingTotalPartitionQuotaRequests.empty()) {
        WaitingTotalPartitionQuotaRequests.push_back(std::move(context));
        return;
    }
    ApproveQuota(context);
}

void TPartitionQuoterBase::HandleAccountQuotaApproved(NAccountQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    auto pendingIter = PendingAccountQuotaRequests.find(ev->Get()->Request->Cookie);
    AFL_ENSURE(!pendingIter.IsEnd());

    TRequestContext context{std::move(pendingIter->second.Request), pendingIter->second.PartitionActor, ev->Get()->WaitTime, ctx.Now()};
    context.Request->Request = std::move(ev->Get()->Request->Request);
    OnAccountQuotaApproved(std::move(context));
    PendingAccountQuotaRequests.erase(pendingIter);
}

void TPartitionQuoterBase::ApproveQuota(TRequestContext& context) {
    auto waitTimeIter = QuotaRequestedTimes.find(context.Request->Cookie);
    if (waitTimeIter != QuotaRequestedTimes.end()) {
        auto waitTime = ActorContext().Now() - waitTimeIter->second;
        QuotaRequestedTimes.erase(waitTimeIter);
        context.TotalQuotaWaitTime = waitTime;
    }
    Send(context.PartitionActor, MakeQuotaApprovedEvent(context));
}

bool TPartitionQuoterBase::CanExaust(TInstant now) {
    return PartitionTotalQuotaTracker->CanExaust(now);
}

void TPartitionQuoterBase::ProcessPartitionTotalQuotaQueue() {
    if (!PartitionTotalQuotaTracker)
        return;
    while (!WaitingTotalPartitionQuotaRequests.empty() && CanExaust(ActorContext().Now())) {
        auto& request = WaitingTotalPartitionQuotaRequests.front();
        ApproveQuota(request);
        WaitingTotalPartitionQuotaRequests.pop_front();
    }
}

void TPartitionQuoterBase::HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev, const TActorContext& ctx) {
    if (PartitionTotalQuotaTracker)
        PartitionTotalQuotaTracker->Exaust(ev->Get()->ConsumedBytes, ctx.Now());
    HandleConsumedImpl(ev);

    if (ev->Get()->IsOverhead)
        return;
    if (RequestsInflight > 0) {
        RequestsInflight--;
        ProcessInflightQueue();
    } else {
        LOG_E("Attempt to make the inflight counter below zero. Topic " << TopicConverter->GetClientsideName() <<
              " partition " << Partition <<
              " readCookie " << ev->Get()->RequestCookie);
    }

    if (!RequestsInflight && (ExclusiveLockState == EExclusiveLockState::EAcquiring)) {
        ExclusiveLockState = EExclusiveLockState::EAcquired;
        ReplyExclusiveLockAcquired(ExclusiveLockRequester);
    }
}

void TPartitionQuoterBase::ProcessInflightQueue() {
   if (ExclusiveLockState != EExclusiveLockState::EReleased)
       return;
    auto now = ActorContext().Now();
    while (!WaitingInflightRequests.empty() && RequestsInflight < MaxInflightRequests) {
        StartQuoting(std::move(WaitingInflightRequests.front()));
        WaitingInflightRequests.pop_front();
        if (WaitingInflightRequests.empty()) {
            InflightLimitSlidingWindow.Update((now - InflightIsFullStartTime).MicroSeconds(), now);
            UpdateCounters(ActorContext());
        }
    }
}

void TPartitionQuoterBase::HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    PQTabletConfig = ev->Get()->Config;
    TopicConverter = ev->Get()->TopicConverter;
    bool totalQuotaUpdated = false;
    if (PartitionTotalQuotaTracker.Defined()) {
        totalQuotaUpdated = PartitionTotalQuotaTracker->UpdateConfigIfChanged(
                GetTotalPartitionSpeedBurst(PQTabletConfig, ctx), GetTotalPartitionSpeed(PQTabletConfig, ctx)
        );
    }
    UpdateQuotaConfigImpl(totalQuotaUpdated, ctx);
}


TQuotaTracker TPartitionQuoterBase::CreatePartitionTotalQuotaTracker(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    return {GetTotalPartitionSpeedBurst(pqTabletConfig, ctx), GetTotalPartitionSpeed(pqTabletConfig, ctx), ctx.Now()};
}

void TPartitionQuoterBase::HandleWakeUp(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    HandleWakeUpImpl();
    ProcessPartitionTotalQuotaQueue();
    UpdateCounters(ctx);
    ScheduleWakeUp(ctx);
}

void TPartitionQuoterBase::ScheduleWakeUp(const TActorContext& ctx) {
    ctx.Schedule(WAKE_UP_TIMEOUT, new TEvents::TEvWakeup());
}

void TPartitionQuoterBase::HandleAcquireExclusiveLock(TEvPQ::TEvAcquireExclusiveLock::TPtr& ev, const TActorContext& ctx) {
    AFL_ENSURE(ExclusiveLockState != EExclusiveLockState::EAcquired);
    switch (ExclusiveLockState) {
    case EExclusiveLockState::EReleased:
        ExclusiveLockState = EExclusiveLockState::EAcquiring;
        ExclusiveLockRequester = ev->Sender;

        [[fallthrough]];

    case EExclusiveLockState::EAcquiring:
        if (RequestsInflight) {
            return;
        }

        ExclusiveLockState = EExclusiveLockState::EAcquired;

        [[fallthrough]];

    case EExclusiveLockState::EAcquired:
        ReplyExclusiveLockAcquired(ev->Sender);
        return;
    }

    Y_UNUSED(ctx);
}

void TPartitionQuoterBase::HandleReleaseExclusiveLock(TEvPQ::TEvReleaseExclusiveLock::TPtr& ev, const TActorContext& ctx) {
    ExclusiveLockState = EExclusiveLockState::EReleased;

    ProcessInflightQueue();

    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TPartitionQuoterBase::ReplyExclusiveLockAcquired(const TActorId& receiver) {
    Send(receiver, new TEvPQ::TEvExclusiveLockAcquired());
}

} // namespace NKikimr::NPQ
