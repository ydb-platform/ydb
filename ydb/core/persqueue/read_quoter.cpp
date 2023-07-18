#include "read_quoter.h"
#include "account_read_quoter.h"


namespace NKikimr {
namespace NPQ {

const TDuration WAKE_UP_TIMEOUT = TDuration::Seconds(5);
const ui64 DEFAULT_READ_SPEED_AND_BURST = 1'000'000'000;

void TReadQuoter::Bootstrap(const TActorContext &ctx) {
    Become(&TThis::StateWork);
    ScheduleWakeUp(ctx);

    //UpdateConsumersWithCustomQuota(ctx); // depricated. Delete this after 01.10.2023
}

void TReadQuoter::HandleQuotaRequest(TEvPQ::TEvRequestQuota::TPtr& ev, const TActorContext& ctx) {
    TConsumerReadQuota* consumer = GetOrCreateConsumerQuota(ev->Get()->ReadRequest->Get()->ClientId, ctx);
    QuotaRequestedTimes.emplace(ev->Get()->ReadRequest->Cookie, ctx.Now());

    if (consumer->ReadSpeedLimiter) {
        Send(consumer->ReadSpeedLimiter->Actor, new NAccountReadQuoterEvents::TEvRequest(ev->Get()->ReadRequest.Release()));
    } else {
        CheckConsumerPerPartitionQuota(ev->Get()->ReadRequest, ctx);
    }
}

void TReadQuoter::HandleAccountQuotaApproved(NAccountReadQuoterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    CheckConsumerPerPartitionQuota(ev->Get()->ReadRequest, ctx);
}

void TReadQuoter::CheckConsumerPerPartitionQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx) {
    TConsumerReadQuota* consumerQuota = GetOrCreateConsumerQuota(ev->Get()->ClientId, ctx);
    if (!consumerQuota->PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now())) {
        consumerQuota->ReadRequests.push_back(ev);
        return;
    }
    CheckTotalPartitionQuota(ev, ctx);
}

void TReadQuoter::CheckTotalPartitionQuota(TEvPQ::TEvRead::TPtr ev, const TActorContext& ctx) {
    if (!PartitionTotalQuotaTracker.CanExaust(ctx.Now())) {
        ReadRequests.push_back(ev);
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
    for (auto& [consumerStr, consumer] : ConsumerQuotas) {
        while (!consumer.ReadRequests.empty() && consumer.PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now())) {
            auto readEvent(std::move(consumer.ReadRequests.front()));
            consumer.ReadRequests.pop_front();
            CheckTotalPartitionQuota(readEvent, ctx);
        }
    }
    while (!ReadRequests.empty() && PartitionTotalQuotaTracker.CanExaust(ctx.Now())) {
        auto readEvent(std::move(ReadRequests.front()));
        ReadRequests.pop_front();
        ApproveQuota(readEvent, ctx);
    }
    ScheduleWakeUp(ctx);
}

void TReadQuoter::HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const TActorContext&) {
    auto it = ConsumerQuotas.find(ev->Get()->Consumer);
    if(it != ConsumerQuotas.end()) {
        if(it->second.ReadSpeedLimiter) {
            Send(it->second.ReadSpeedLimiter->Actor, new TEvents::TEvPoisonPill());
        }
        ConsumerQuotas.erase(it);
    }   
}

void TReadQuoter::HandleConsumed(TEvPQ::TEvConsumed::TPtr& ev,const TActorContext& ctx) {
    PartitionTotalQuotaTracker.Exaust(ev->Get()->ReadBytes, ctx.Now());
    auto consumerQuota = GetIfExists(ev->Get()->Consumer);
    if(!consumerQuota)
        return;
    if (consumerQuota->ReadSpeedLimiter) {
        Send(
            consumerQuota->ReadSpeedLimiter->Actor,
            new NAccountReadQuoterEvents::TEvConsumed(ev->Get()->ReadBytes, ev->Get()->ReadRequestCookie)
        );
    }
    consumerQuota->PartitionPerConsumerQuotaTracker.Exaust(ev->Get()->ReadBytes, ctx.Now());
}

void TReadQuoter::HandleConfigUpdate(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    PQTabletConfig = ev->Get()->Config;
    UpdateQuota(ctx);
    //UpdateConsumersWithCustomQuota(ctx); // depricated. Delete this after 01.10.2023
}

void TReadQuoter::HandleUpdateAccountQuotaCounters(NAccountReadQuoterEvents::TEvCounters::TPtr& ev, const TActorContext&) {
    auto consumerQuota = GetIfExists(ev->Get()->User);
    if(!consumerQuota)
        return;
    if (consumerQuota->ReadSpeedLimiter) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(consumerQuota->ReadSpeedLimiter->Baseline);
        ev->Get()->Counters.RememberCurrentStateAsBaseline(consumerQuota->ReadSpeedLimiter->Baseline);
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaCountersUpdated(diff));
    }
}

void TReadQuoter::HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    for (auto& consumerQuota : ConsumerQuotas) {
        if(consumerQuota.second.ReadSpeedLimiter) {
            Send(consumerQuota.second.ReadSpeedLimiter->Actor, new TEvents::TEvPoisonPill());
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
        TConsumerReadQuota consumer(CreateReadSpeedLimiter(consumerStr), GetConsumerReadBurst(ctx), GetConsumerReadSpeed(ctx));
        auto result = ConsumerQuotas.emplace(consumerStr, std::move(consumer));
        Y_VERIFY(result.second);
        Send(PartitionActor, new NReadQuoterEvents::TEvQuotaUpdated({{consumerStr, consumer.PartitionPerConsumerQuotaTracker.GetTotalSpeed()}}, PartitionTotalQuotaTracker.GetTotalSpeed()));
        return &result.first->second;
    }
    return &it->second;
}

TConsumerReadQuota* TReadQuoter::GetIfExists(const TString& consumerStr) {
    auto it = ConsumerQuotas.find(consumerStr);
    return it != ConsumerQuotas.end() ? &it->second : nullptr;
}

void TReadQuoter::ScheduleWakeUp(const TActorContext& ctx) {
    ctx.Schedule(WAKE_UP_TIMEOUT, new TEvents::TEvWakeup());
}

THolder<TAccountReadQuoterHolder> TReadQuoter::CreateReadSpeedLimiter(const TString& user) const {
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
