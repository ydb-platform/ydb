#include "read_quoter.h"
#include "account_read_quoter.h"


namespace NKikimr {
namespace NPQ {

void TReadQuoter::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);
    ScheduleWakeUp(ctx);

    //UpdateConsumersWithCustomQuota(ctx); // depricated. Delete this after 01.10.2023
}

void TReadQuoter::HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev, const TActorContext& ctx) {
    auto readRequest = ev->Get()->ReadRequest;
    GetOrCreateConsumerQuota(readRequest->Get()->ClientId, ctx);
    QuotaRequestedTimes.emplace(readRequest->Cookie, ctx.Now());
    if (RequestsInflight >= AppData(ctx)->PQConfig.GetMaxInflightReadRequestsPerPartition() || !WaitingInflightReadRequests.empty()) {
        if (WaitingInflightReadRequests.empty())
            InflightIsFullStartTime = ctx.Now();
        WaitingInflightReadRequests.push_back(readRequest);
    } else {
        StartQuoting(readRequest, ctx);
    }
}

void TReadQuoter::StartQuoting(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx) {
    RequestsInflight++;
    TConsumerReadQuota* consumerQuota = GetOrCreateConsumerQuota(ev->Get()->ClientId, ctx);
    if (consumerQuota->AccountQuotaTracker) {
        Send(consumerQuota->AccountQuotaTracker->Actor, new NAccountReadQuoterEvents::TEvRequest(ev.Release()));
    } else {
        CheckConsumerPerPartitionQuota(ev, ctx);
    }
}

void TReadQuoter::HandleAccountQuotaApproved(NAccountReadQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    CheckConsumerPerPartitionQuota(ev->Get()->ReadRequest, ctx);
}

void TReadQuoter::CheckConsumerPerPartitionQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx) {
    auto consumerQuota = GetOrCreateConsumerQuota(ev->Get()->ClientId, ctx);
    if (!consumerQuota->PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now()) || !consumerQuota->ReadRequests.empty()) {
        consumerQuota->ReadRequests.push_back(ev);
        return;
    }
    CheckTotalPartitionQuota(ev, ctx);
}

void TReadQuoter::CheckTotalPartitionQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx) {
    if (!PartitionTotalQuotaTracker.CanExaust(ctx.Now()) || !WaitingTotalPartitionQuotaReadRequests.empty()) {
        WaitingTotalPartitionQuotaReadRequests.push_back(ev);
        return;
    }
    ApproveQuota(ev, ctx);
}

void TReadQuoter::ApproveQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx) {
    auto waitTime = TDuration::Zero();
    auto waitTimeIter = QuotaRequestedTimes.find(ev->Get()->Cookie);
    if (waitTimeIter != QuotaRequestedTimes.end()) {
        waitTime = ctx.Now() - waitTimeIter->second;
        QuotaRequestedTimes.erase(waitTimeIter);
    }
    Send(PartitionActor, new TEvPQ::TEvApproveQuota(ev, waitTime));
}

void TReadQuoter::HandleWakeUp(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    ProcessPerConsumerQuotaQueue(ctx);
    ProcessPartititonTotalQuotaQueue(ctx);
    UpdateCounters(ctx);
    ScheduleWakeUp(ctx);
}

void TReadQuoter::ProcessInflightQueue(const TActorContext& ctx) {
    while (!WaitingInflightReadRequests.empty() && RequestsInflight < AppData(ctx)->PQConfig.GetMaxInflightReadRequestsPerPartition()) {
        auto readEvent(std::move(WaitingInflightReadRequests.front()));
        WaitingInflightReadRequests.pop_front();
        StartQuoting(readEvent, ctx);
        if (WaitingInflightReadRequests.size() == 0) {
            InflightLimitSlidingWindow.Update((ctx.Now() - InflightIsFullStartTime).MicroSeconds(), ctx.Now());
            UpdateCounters(ctx);
        }
    }
}

void TReadQuoter::ProcessPerConsumerQuotaQueue(const TActorContext& ctx) {
    for (auto& [consumerStr, consumer] : ConsumerQuotas) {
        while (!consumer.ReadRequests.empty() && consumer.PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now())) {
            auto readEvent(std::move(consumer.ReadRequests.front()));
            consumer.ReadRequests.pop_front();
            CheckTotalPartitionQuota(readEvent, ctx);
        }
    }
}

void TReadQuoter::ProcessPartititonTotalQuotaQueue(const TActorContext& ctx) {
    while (!WaitingTotalPartitionQuotaReadRequests.empty() && PartitionTotalQuotaTracker.CanExaust(ctx.Now())) {
        auto readEvent(std::move(WaitingTotalPartitionQuotaReadRequests.front()));
        WaitingTotalPartitionQuotaReadRequests.pop_front();
        ApproveQuota(readEvent, ctx);
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

void TReadQuoter::HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev, const TActorContext& ctx) {
    PartitionTotalQuotaTracker.Exaust(ev->Get()->ReadBytes, ctx.Now());
    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->Consumer);
    if (consumerQuota) {
        if (consumerQuota->AccountQuotaTracker) {
            Send(
                consumerQuota->AccountQuotaTracker->Actor,
                new NAccountReadQuoterEvents::TEvConsumed(ev->Get()->ReadBytes, ev->Get()->ReadRequestCookie)
            );
        }
        consumerQuota->PartitionPerConsumerQuotaTracker.Exaust(ev->Get()->ReadBytes, ctx.Now());
    }
    if (RequestsInflight > 0) {
        RequestsInflight--;
        ProcessInflightQueue(ctx);
    } else {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                        "Attempt to make the inflight counter below zero. Topic " << TopicConverter->GetClientsideName() <<
                        " partition " << Partition <<
                        " readCookie " << ev->Get()->ReadRequestCookie);
    }
}

void TReadQuoter::HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    PQTabletConfig = ev->Get()->Config;
    UpdateQuota(ctx);
    //UpdateConsumersWithCustomQuota(ctx); // depricated. Delete this after 01.10.2023
}

void TReadQuoter::HandleUpdateAccountQuotaCounters(NAccountReadQuoterEvents::TEvCounters::TPtr& ev, const TActorContext&) {
    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->User);
    if (!consumerQuota)
        return;
    if (consumerQuota->AccountQuotaTracker) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(consumerQuota->AccountQuotaTracker->Baseline);
        ev->Get()->Counters.RememberCurrentStateAsBaseline(consumerQuota->AccountQuotaTracker->Baseline);
        Send(PartitionActor, new NReadQuoterEvents::TEvAccountQuotaCountersUpdated(diff));
    }
}

void TReadQuoter::UpdateCounters(const TActorContext& ctx) {
    if (!WaitingInflightReadRequests.empty()) {
        InflightLimitSlidingWindow.Update((ctx.Now() - InflightIsFullStartTime).MicroSeconds(), ctx.Now());
    } else {
        InflightLimitSlidingWindow.Update(ctx.Now());
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

void TReadQuoter::UpdateQuota(const TActorContext &ctx) {
    TVector<std::pair<TString, ui64>> updatedQuotas;
    for (auto& [consumerStr, consumerQuota] : ConsumerQuotas) {
        if (consumerQuota.PartitionPerConsumerQuotaTracker.UpdateConfigIfChanged(
        GetConsumerReadBurst(ctx),
        GetConsumerReadSpeed(ctx))) {
             updatedQuotas.push_back({consumerStr, consumerQuota.PartitionPerConsumerQuotaTracker.GetTotalSpeed()});
        }
    }
    auto totalQuotaUpdated = PartitionTotalQuotaTracker.UpdateConfigIfChanged(GetTotalPartitionReadBurst(ctx), GetTotalPartitionReadSpeed(ctx));
    if (updatedQuotas.size() || totalQuotaUpdated) {
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaUpdated(updatedQuotas, PartitionTotalQuotaTracker.GetTotalSpeed()));
    }
}

void TReadQuoter::UpdateConsumersWithCustomQuota(const TActorContext &ctx) {
    TVector<std::pair<TString, ui64>> updatedQuotas;
    for (const auto& readQuota : PQTabletConfig.GetPartitionConfig().GetReadQuota()) {
        auto consumerQuota = GetOrCreateConsumerQuota(readQuota.GetClientId(), ctx);
        if (consumerQuota->PartitionPerConsumerQuotaTracker.UpdateConfigIfChanged(readQuota.GetBurstSize(), readQuota.GetSpeedInBytesPerSecond())) {
            updatedQuotas.push_back({readQuota.GetClientId(), consumerQuota->PartitionPerConsumerQuotaTracker.GetTotalSpeed()});
        }
    }
    if (updatedQuotas.size()) {
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaUpdated(updatedQuotas, PartitionTotalQuotaTracker.GetTotalSpeed()));
    }
}

ui64 TReadQuoter::GetConsumerReadSpeed(const TActorContext& ctx) const {
    return AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() ?
        PQTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetConsumerReadBurst(const TActorContext& ctx) const {
    return AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() ?
        PQTabletConfig.GetPartitionConfig().GetBurstSize() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetTotalPartitionReadSpeed(const TActorContext& ctx) const {
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadSpeed(ctx) * consumersPerPartition;
}

ui64 TReadQuoter::GetTotalPartitionReadBurst(const TActorContext& ctx) const {
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadBurst(ctx) * consumersPerPartition;
}

TConsumerReadQuota* TReadQuoter::GetOrCreateConsumerQuota(const TString& consumerStr, const TActorContext& ctx) {
    Y_VERIFY(!consumerStr.empty());
    auto it = ConsumerQuotas.find(consumerStr);
    if (it == ConsumerQuotas.end()) {
        TConsumerReadQuota consumer(CreateAccountQuotaTracker(consumerStr), GetConsumerReadBurst(ctx), GetConsumerReadSpeed(ctx));
        auto result = ConsumerQuotas.emplace(consumerStr, std::move(consumer));
        Y_VERIFY(result.second);
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaUpdated({{consumerStr, consumer.PartitionPerConsumerQuotaTracker.GetTotalSpeed()}}, PartitionTotalQuotaTracker.GetTotalSpeed()));
        return &result.first->second;
    }
    return &it->second;
}

TConsumerReadQuota* TReadQuoter::GetConsumerQuotaIfExists(const TString& consumerStr) {
    auto it = ConsumerQuotas.find(consumerStr);
    return it != ConsumerQuotas.end() ? &it->second : nullptr;
}

void TReadQuoter::ScheduleWakeUp(const TActorContext& ctx) {
    ctx.Schedule(WAKE_UP_TIMEOUT, new TEvents::TEvWakeup());
}

THolder<TAccountReadQuoterHolder> TReadQuoter::CreateAccountQuotaTracker(const TString& user) const {
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    if (TabletActor && quotingConfig.GetEnableQuoting() && quotingConfig.GetEnableReadQuoting()) {
        TActorId actorId = TActivationContext::Register(
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
        return MakeHolder<TAccountReadQuoterHolder>(actorId, Counters);
    }
    return nullptr;
}

TQuotaTracker TReadQuoter::CreatePartitionTotalQuotaTracker(const TActorContext& ctx) const {
    return {GetTotalPartitionReadBurst(ctx), GetTotalPartitionReadSpeed(ctx), ctx.Now()};
}

}// NPQ
}// NKikimr
