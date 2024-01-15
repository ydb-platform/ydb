#include "read_quoter.h"
#include "account_read_quoter.h"


namespace NKikimr {
namespace NPQ {

void TPartitionQuoterBase::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);
    ScheduleWakeUp(ctx);
}

void TPartitionQuoterBase::HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev, const TActorContext& ctx) {
    QuotaRequestedTimes.emplace(ev->Get()->Cookie, ctx.Now());
    TRequestContext context{ev, TDuration::Zero()};
    HandleQuotaRequestImpl(context);
    if (RequestsInflight >= MaxInflightRequests || !WaitingInflightRequests.empty()) {
        if (WaitingInflightRequests.empty())
            InflightIsFullStartTime = ctx.Now();
        WaitingInflightRequests.push_back(std::move(context));
    } else {
        StartQuoting(context);
    }
}

void TPartitionQuoterBase::StartQuoting(TRequestContext& context) {
    RequestsInflight++;
    auto& ev = context.Request;
    auto& accountQuotaTracker = GetAccountQuotaTracker(ev);
    if (accountQuotaTracker) {
        Send(accountQuotaTracker->Actor, new NAccountQuoterEvents::TEvRequest(ev->Get()->Cookie, std::move(ev->Get()->Request)));
        PendingAccountQuotaRequests.insert(std::make_pair(ev->Get()->Cookie, context));
    } else {
        OnAccountQuotaApproved(context);
    }
}
void TPartitionQuoterBase::CheckTotalPartitionQuota(TRequestContext& context) {
    if (!PartitionTotalQuotaTracker)
        return ApproveQuota(context);
    if (!PartitionTotalQuotaTracker->CanExaust(ActorContext().Now()) || !WaitingTotalPartitionQuotaRequests.empty()) {
        WaitingTotalPartitionQuotaRequests.push_back(context);
        return;
    }
    ApproveQuota(context);
}

void TPartitionQuoterBase::HandleAccountQuotaApproved(NAccountQuoterEvents::TEvResponse::TPtr& ev, const TActorContext&) {
    auto pendingIter = PendingAccountQuotaRequests.find(ev->Get()->Request->Get()->Cookie);
    Y_ABORT_UNLESS(!pendingIter.IsEnd());
    pendingIter->second.Request->Get()->Request = std::move(ev->Get()->Request->Get()->Request);
    TRequestContext context{pendingIter->second.Request, ev->Get()->WaitTime};
    PendingAccountQuotaRequests.erase(pendingIter);
    OnAccountQuotaApproved(context);
}

void TPartitionQuoterBase::ApproveQuota(TRequestContext& context) {
    auto waitTime = TDuration::Zero();
    auto waitTimeIter = QuotaRequestedTimes.find(context.Request->Get()->Cookie);
    if (waitTimeIter != QuotaRequestedTimes.end()) {
        waitTime = ActorContext().Now() - waitTimeIter->second;
        QuotaRequestedTimes.erase(waitTimeIter);
    }
    Send(PartitionActor, MakeQuotaApprovedEvent(context));
}

void TPartitionQuoterBase::ProcessPartititonTotalQuotaQueue() {
    Y_ABORT_UNLESS(PartitionTotalQuotaTracker.Defined());
    while (!WaitingTotalPartitionQuotaRequests.empty() && PartitionTotalQuotaTracker->CanExaust(ActorContext().Now())) {
        auto request = WaitingTotalPartitionQuotaRequests.front();
        WaitingTotalPartitionQuotaRequests.pop_front();
        ApproveQuota(request);
    }
}

void TPartitionQuoterBase::HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev, const TActorContext& ctx) {
    if (PartitionTotalQuotaTracker.Defined())
        PartitionTotalQuotaTracker->Exaust(ev->Get()->ReadBytes, ctx.Now());
    HandleConsumedImpl(ev);

    if (RequestsInflight > 0) {
        RequestsInflight--;
        ProcessInflightQueue();
    } else {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                        "Attempt to make the inflight counter below zero. Topic " << TopicConverter->GetClientsideName() <<
                        " partition " << Partition <<
                        " readCookie " << ev->Get()->ReadRequestCookie);
    }
}

void TPartitionQuoterBase::ProcessInflightQueue() {
    auto now = ActorContext().Now();
    while (!WaitingInflightRequests.empty() && RequestsInflight < MaxInflightRequests) {
        auto request = std::move(WaitingInflightRequests.front());
        WaitingInflightRequests.pop_front();
        StartQuoting(request);
        if (WaitingInflightRequests.size() == 0) {
            InflightLimitSlidingWindow.Update((now - InflightIsFullStartTime).MicroSeconds(), now);
            UpdateCounters(ActorContext());
        }
    }
}

void TPartitionQuoterBase::HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    PQTabletConfig = ev->Get()->Config;
    bool totalQuotaUpdated = false;
    if (PartitionTotalQuotaTracker.Defined()) {
        totalQuotaUpdated = PartitionTotalQuotaTracker->UpdateConfigIfChanged(
                GetTotalPartitionSpeedBurst(PQTabletConfig, ctx), GetTotalPartitionSpeed(PQTabletConfig, ctx)
        );
    }
    UpdateQuotaConfigImpl(totalQuotaUpdated);
}


TQuotaTracker TPartitionQuoterBase::CreatePartitionTotalQuotaTracker(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    return {GetTotalPartitionSpeedBurst(pqTabletConfig, ctx), GetTotalPartitionSpeed(pqTabletConfig, ctx), ctx.Now()};
}

//ToDo: (?) Remove from a basic class maybe?
THolder<TAccountQuoterHolder> TPartitionQuoterBase::CreateAccountQuotaTracker(const TString& user) const {
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    if (TabletActor && quotingConfig.GetEnableQuoting() && quotingConfig.GetEnableReadQuoting()) {
        TActorId actorId;
        if (!user.empty()) {
            actorId = TActivationContext::Register(
                new TAccountReadQuoter(
                    TabletActor,
                    SelfId(),
                    TabletId,
                    TopicConverter,
                    Partition,
                    user,
                    Counters
                ),
                PartitionActor
            );
        } else {
            actorId = TActivationContext::Register(
                new TAccountWriteQuoter(
                    TabletActor,
                    SelfId(),
                    TabletId,
                    TopicConverter,
                    Partition,
                    Counters,
                    ActorContext()
                ),
                PartitionActor
            );
        }
        return MakeHolder<TAccountQuoterHolder>(actorId, Counters);
    }
    return nullptr;
}

void TPartitionQuoterBase::HandleWakeUp(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    HandleWakeUpImpl();
    ProcessPartititonTotalQuotaQueue();
    UpdateCounters(ctx);
    ScheduleWakeUp(ctx);
}

void TPartitionQuoterBase::ScheduleWakeUp(const TActorContext& ctx) {
    ctx.Schedule(WAKE_UP_TIMEOUT, new TEvents::TEvWakeup());
}

void TReadQuoter::HandleQuotaRequestImpl(TRequestContext& context) {
    auto* readRequest = context.Request->Get()->Request->CastAsLocal<TEvPQ::TEvRead>();
    GetOrCreateConsumerQuota(readRequest->ClientId, ActorContext());
}

void TReadQuoter::OnAccountQuotaApproved(TRequestContext& context) {
    CheckConsumerPerPartitionQuota(context);
}

THolder<TAccountQuoterHolder>& TReadQuoter::GetAccountQuotaTracker(TEvPQ::TEvRequestQuota::TPtr& ev) {
    auto clientId = ev->Get()->Request->CastAsLocal<TEvPQ::TEvRead>()->ClientId;
    return GetOrCreateConsumerQuota(clientId, ActorContext())->AccountQuotaTracker;
}

IEventBase* TReadQuoter::MakeQuotaApprovedEvent(TRequestContext& context) {
    return new TEvPQ::TEvApproveReadQuota(IEventHandle::Downcast<TEvPQ::TEvRead>(std::move(context.Request->Get()->Request)), context.WaitTime);
};

void TReadQuoter::CheckConsumerPerPartitionQuota(TRequestContext& context) {
    auto consumerQuota = GetOrCreateConsumerQuota(
            context.Request->Get()->Request->CastAsLocal<TEvPQ::TEvRead>()->ClientId,
            ActorContext()
    );
    if (!consumerQuota->PartitionPerConsumerQuotaTracker.CanExaust(ActorContext().Now()) || !consumerQuota->ReadRequests.empty()) {
        consumerQuota->ReadRequests.push_back(context);
        return;
    }
    CheckTotalPartitionQuota(context);
}

void TReadQuoter::HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) {
    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->Consumer);
    if (consumerQuota) {
        if (consumerQuota->AccountQuotaTracker) {
            Send(
                consumerQuota->AccountQuotaTracker->Actor,
                new NAccountQuoterEvents::TEvConsumed(ev->Get()->ReadBytes, ev->Get()->ReadRequestCookie)
            );
        }
        consumerQuota->PartitionPerConsumerQuotaTracker.Exaust(ev->Get()->ReadBytes, ActorContext().Now());
    }
}

void TReadQuoter::HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr& ev, const TActorContext&) {
    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->Subject);
    if (!consumerQuota)
        return;
    if (consumerQuota->AccountQuotaTracker) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(consumerQuota->AccountQuotaTracker->Baseline);
        ev->Get()->Counters.RememberCurrentStateAsBaseline(consumerQuota->AccountQuotaTracker->Baseline);
        Send(PartitionActor, new NReadQuoterEvents::TEvAccountQuotaCountersUpdated(diff));
    }
}

void TReadQuoter::HandleWakeUpImpl() {
    ProcessPerConsumerQuotaQueue(ActorContext());
}

void TReadQuoter::ProcessPerConsumerQuotaQueue(const TActorContext& ctx) {
    for (auto& [consumerStr, consumer] : ConsumerQuotas) {
        while (!consumer.ReadRequests.empty() && consumer.PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now())) {
            auto requestContext = std::move(consumer.ReadRequests.front());
            consumer.ReadRequests.pop_front();
            CheckTotalPartitionQuota(requestContext);
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
    Send(PartitionActor, new NReadQuoterEvents::TEvQuotaCountersUpdated(InflightLimitSlidingWindow.GetValue() / 60));
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

void TReadQuoter::UpdateQuotaConfigImpl(bool totalQuotaUpdated) {
    TVector<std::pair<TString, ui64>> updatedQuotas;
    for (auto& [consumerStr, consumerQuota] : ConsumerQuotas) {
        if (consumerQuota.PartitionPerConsumerQuotaTracker.UpdateConfigIfChanged(
                GetConsumerReadBurst(PQTabletConfig, ActorContext()), GetConsumerReadSpeed(PQTabletConfig, ActorContext())
        )) {
            updatedQuotas.push_back({consumerStr, consumerQuota.PartitionPerConsumerQuotaTracker.GetTotalSpeed()});
        }
    }

    ui64 totalSpeed = 0;
    if (PartitionTotalQuotaTracker.Defined()) {
        totalSpeed = PartitionTotalQuotaTracker->GetTotalSpeed();
    }
    if (updatedQuotas.size() || totalQuotaUpdated) {
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaUpdated(updatedQuotas, totalSpeed));
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

TConsumerReadQuota* TReadQuoter::GetOrCreateConsumerQuota(const TString& consumerStr, const TActorContext& ctx) {
    Y_ABORT_UNLESS(!consumerStr.empty());
    auto it = ConsumerQuotas.find(consumerStr);
    if (it == ConsumerQuotas.end()) {
        TConsumerReadQuota consumer(
                CreateAccountQuotaTracker(consumerStr),
                GetConsumerReadBurst(PQTabletConfig, ctx),
                GetConsumerReadSpeed(PQTabletConfig, ctx)
        );
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaUpdated(
                {{consumerStr, consumer.PartitionPerConsumerQuotaTracker.GetTotalSpeed()}},
                GetTotalPartitionSpeed(PQTabletConfig, ctx)
        ));

        auto result = ConsumerQuotas.emplace(consumerStr, std::move(consumer));
        Y_ABORT_UNLESS(result.second);
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
