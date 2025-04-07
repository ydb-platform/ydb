#include "read_quoter.h"
#include "account_read_quoter.h"


namespace NKikimr {
namespace NPQ {

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
    if (RequestsInflight >= MaxInflightRequests || !WaitingInflightRequests.empty()) {
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
    if (!PartitionTotalQuotaTracker)
        return ApproveQuota(context);
    if (!PartitionTotalQuotaTracker->CanExaust(ActorContext().Now()) || !WaitingTotalPartitionQuotaRequests.empty()) {
        WaitingTotalPartitionQuotaRequests.push_back(std::move(context));
        return;
    }
    ApproveQuota(context);
}

void TPartitionQuoterBase::HandleAccountQuotaApproved(NAccountQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    auto pendingIter = PendingAccountQuotaRequests.find(ev->Get()->Request->Cookie);
    Y_ABORT_UNLESS(!pendingIter.IsEnd());

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

void TPartitionQuoterBase::ProcessPartitionTotalQuotaQueue() {
    if (!PartitionTotalQuotaTracker)
        return;
    while (!WaitingTotalPartitionQuotaRequests.empty() && PartitionTotalQuotaTracker->CanExaust(ActorContext().Now())) {
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
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                        "Attempt to make the inflight counter below zero. Topic " << TopicConverter->GetClientsideName() <<
                        " partition " << Partition <<
                        " readCookie " << ev->Get()->RequestCookie);
    }
}

void TPartitionQuoterBase::ProcessInflightQueue() {
    auto now = ActorContext().Now();
    while (!WaitingInflightRequests.empty() && RequestsInflight < MaxInflightRequests) {
        StartQuoting(std::move(WaitingInflightRequests.front()));
        WaitingInflightRequests.pop_front();
        if (WaitingInflightRequests.size() == 0) {
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

void TReadQuoter::HandleQuotaRequestImpl(TRequestContext& context) {
    auto* readRequest = context.Request->Request->CastAsLocal<TEvPQ::TEvRead>();
    GetOrCreateConsumerQuota(readRequest->ClientId, ActorContext());
}

void TReadQuoter::OnAccountQuotaApproved(TRequestContext&& context) {
    CheckConsumerPerPartitionQuota(std::move(context));
}

TAccountQuoterHolder* TReadQuoter::GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) {
    if (!TopicConverter)
        return nullptr;
    auto clientId = request->Request->CastAsLocal<TEvPQ::TEvRead>()->ClientId;
    return GetOrCreateConsumerQuota(clientId, ActorContext())->AccountQuotaTracker.Get();
}

IEventBase* TReadQuoter::MakeQuotaApprovedEvent(TRequestContext& context) {
    return new TEvPQ::TEvApproveReadQuota(IEventHandle::Downcast<TEvPQ::TEvRead>(std::move(context.Request->Request)), context.TotalQuotaWaitTime);
};

void TReadQuoter::CheckConsumerPerPartitionQuota(TRequestContext&& context) {
    Y_ABORT_UNLESS(context.Request->Request);
    auto consumerQuota = GetOrCreateConsumerQuota(
            context.Request->Request->CastAsLocal<TEvPQ::TEvRead>()->ClientId,
            ActorContext()
    );
    if (!consumerQuota->PartitionPerConsumerQuotaTracker.CanExaust(ActorContext().Now()) || !consumerQuota->ReadRequests.empty()) {
        consumerQuota->ReadRequests.push_back(std::move(context));
        return;
    }
    CheckTotalPartitionQuota(std::move(context));
}

void TReadQuoter::HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) {
    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->Consumer);
    if (consumerQuota) {
        if (consumerQuota->AccountQuotaTracker) {
            Send(
                consumerQuota->AccountQuotaTracker->Actor,
                new NAccountQuoterEvents::TEvConsumed(ev->Get()->ConsumedBytes, ev->Get()->RequestCookie)
            );
        }
        consumerQuota->PartitionPerConsumerQuotaTracker.Exaust(ev->Get()->ConsumedBytes, ActorContext().Now());
    }
}

void TReadQuoter::HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext&) {
    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->Subject);
    if (!consumerQuota)
        return;
    if (consumerQuota->AccountQuotaTracker) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(consumerQuota->AccountQuotaTracker->Baseline);
        ev->Get()->Counters.RememberCurrentStateAsBaseline(consumerQuota->AccountQuotaTracker->Baseline);
        Send(Parent, new NReadQuoterEvents::TEvAccountQuotaCountersUpdated(diff));
    }
}

void TReadQuoter::HandleWakeUpImpl() {
    ProcessPerConsumerQuotaQueue(ActorContext());
}

void TReadQuoter::ProcessPerConsumerQuotaQueue(const TActorContext& ctx) {
    for (auto& [consumerStr, consumer] : ConsumerQuotas) {
        while (!consumer.ReadRequests.empty() && consumer.PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now())) {
            CheckTotalPartitionQuota(std::move(consumer.ReadRequests.front()));
            consumer.ReadRequests.pop_front();
        }
    }
}

void TReadQuoter::HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const TActorContext&) {
    auto it = ConsumerQuotas.find(ev->Get()->Consumer);
    if (it != ConsumerQuotas.end()) {
        if (it->second.AccountQuotaTracker) {
            Send(it->second.AccountQuotaTracker->Actor, new TEvents::TEvPoisonPill());
        }
        ConsumerQuotas.erase(it);
    }
}

void TReadQuoter::UpdateCounters(const TActorContext& ctx) {
    auto now = ctx.Now();
    if (!WaitingInflightRequests.empty()) {
        InflightLimitSlidingWindow.Update((now - InflightIsFullStartTime).MicroSeconds(), now);
        InflightIsFullStartTime = now;
    } else {
        InflightLimitSlidingWindow.Update(now);
    }
    Send(Parent, NReadQuoterEvents::TEvQuotaCountersUpdated::ReadCounters(InflightLimitSlidingWindow.GetValue() / 60));
}

void TReadQuoter::HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    for (auto& consumerQuota : ConsumerQuotas) {
        if (consumerQuota.second.AccountQuotaTracker) {
            Send(consumerQuota.second.AccountQuotaTracker->Actor, new TEvents::TEvPoisonPill());
        }
    }
    ConsumerQuotas.clear();
    Die(ctx);
}

void TReadQuoter::UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) {
    TVector<std::pair<TString, ui64>> updatedQuotas;
    for (auto& [consumerStr, consumerQuota] : ConsumerQuotas) {
        if (consumerQuota.PartitionPerConsumerQuotaTracker.UpdateConfigIfChanged(
                GetConsumerReadBurst(PQTabletConfig, ctx), GetConsumerReadSpeed(PQTabletConfig, ctx)
        )) {
            updatedQuotas.push_back({consumerStr, consumerQuota.PartitionPerConsumerQuotaTracker.GetTotalSpeed()});
        }
    }

    ui64 totalSpeed = 0;
    if (PartitionTotalQuotaTracker.Defined()) {
        totalSpeed = PartitionTotalQuotaTracker->GetTotalSpeed();
    }
    if (updatedQuotas.size() || totalQuotaUpdated) {
        Send(Parent, new NReadQuoterEvents::TEvQuotaUpdated(updatedQuotas, totalSpeed));
    }
}

ui64 TReadQuoter::GetConsumerReadSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    return AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() ?
        pqTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetConsumerReadBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    return AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() ?
        pqTabletConfig.GetPartitionConfig().GetBurstSize() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadSpeed(pqTabletConfig, ctx) * consumersPerPartition;
}

ui64 TReadQuoter::GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadBurst(pqTabletConfig, ctx) * consumersPerPartition;
}

THolder<TAccountQuoterHolder> TReadQuoter::CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const {
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    TActorId actorId;
    Y_ENSURE(TopicConverter);
    if (GetTabletActor() && quotingConfig.GetEnableQuoting()) {
        if(quotingConfig.GetEnableReadQuoting()) {
            actorId = TActivationContext::Register(
                new TAccountReadQuoter(
                    GetTabletActor(),
                    ctx.SelfID,
                    GetTabletId(),
                    TopicConverter,
                    GetPartition(),
                    user,
                    Counters
                ),
                Parent
            );
        }
    }
    if (actorId) {
        return MakeHolder<TAccountQuoterHolder>(actorId, Counters);
    } else {
        return nullptr;
    }
}

TConsumerReadQuota* TReadQuoter::GetOrCreateConsumerQuota(const TString& consumerStr, const TActorContext& ctx) {
    Y_ABORT_UNLESS(!consumerStr.empty());
    auto it = ConsumerQuotas.find(consumerStr);
    if (it == ConsumerQuotas.end()) {
        TConsumerReadQuota consumer(
                CreateAccountQuotaTracker(consumerStr, ctx),
                GetConsumerReadBurst(PQTabletConfig, ctx),
                GetConsumerReadSpeed(PQTabletConfig, ctx)
        );
        Send(Parent, new NReadQuoterEvents::TEvQuotaUpdated(
                {{consumerStr, consumer.PartitionPerConsumerQuotaTracker.GetTotalSpeed()}},
                GetTotalPartitionSpeed(PQTabletConfig, ctx)
        ));

        auto result = ConsumerQuotas.emplace(consumerStr, std::move(consumer));
        return &result.first->second;
    }
    return &it->second;
}

TConsumerReadQuota* TReadQuoter::GetConsumerQuotaIfExists(const TString& consumerStr) {
    auto it = ConsumerQuotas.find(consumerStr);
    return it != ConsumerQuotas.end() ? &it->second : nullptr;
}

}// NPQ
}// NKikimr
