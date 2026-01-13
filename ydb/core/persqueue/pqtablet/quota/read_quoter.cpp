#include "read_quoter.h"

#include <ydb/core/persqueue/public/constants.h>

namespace NKikimr::NPQ {

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

TString TReadQuoter::BuildLogPrefix() const {
    return TStringBuilder() << "[ReadQuoter][" << Partition << "] ";
}

void TReadQuoter::CheckConsumerPerPartitionQuota(TRequestContext&& context) {
    AFL_ENSURE(context.Request->Request);
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
        Send(Parent, new NQuoterEvents::TEvAccountQuotaCountersUpdated(diff));
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
    Send(Parent, NQuoterEvents::TEvQuotaCountersUpdated::ReadCounters(InflightLimitSlidingWindow.GetValue() / 60));
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
                GetConsumerReadBurst(PQTabletConfig, consumerStr, ctx), GetConsumerReadSpeed(PQTabletConfig, consumerStr, ctx)
        )) {
            updatedQuotas.push_back({consumerStr, consumerQuota.PartitionPerConsumerQuotaTracker.GetTotalSpeed()});
        }
    }

    ui64 totalSpeed = 0;
    if (PartitionTotalQuotaTracker.Defined()) {
        totalSpeed = PartitionTotalQuotaTracker->GetTotalSpeed();
    }
    if (updatedQuotas.size() || totalQuotaUpdated) {
        Send(Parent, new NQuoterEvents::TEvQuotaUpdated(updatedQuotas, totalSpeed));
    }
}

ui64 TReadQuoter::GetConsumerReadSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerName, const TActorContext& ctx) const {
    return (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() || consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER)
        ? pqTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetConsumerReadBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerName, const TActorContext& ctx) const {
    bool doLimitInternalConsumer = AppData(ctx)->PQConfig.GetQuotingConfig().GetEnableQuoting() && consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER;
    return (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()  || doLimitInternalConsumer)
        ? pqTabletConfig.GetPartitionConfig().GetBurstSize() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadSpeed(pqTabletConfig, {}, ctx) * consumersPerPartition;
}

ui64 TReadQuoter::GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadBurst(pqTabletConfig, {}, ctx) * consumersPerPartition;
}

THolder<TAccountQuoterHolder> TReadQuoter::CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const {
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    TActorId actorId;
    if (GetTabletActor() && quotingConfig.GetEnableQuoting()) {
        Y_ENSURE(TopicConverter);
        if (quotingConfig.GetEnableReadQuoting()) {
            actorId = TActivationContext::RegisterWithSameMailbox(
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
    AFL_ENSURE(!consumerStr.empty());
    auto it = ConsumerQuotas.find(consumerStr);
    if (it == ConsumerQuotas.end()) {
        TConsumerReadQuota consumer(
                CreateAccountQuotaTracker(consumerStr, ctx),
                GetConsumerReadBurst(PQTabletConfig, consumerStr, ctx),
                GetConsumerReadSpeed(PQTabletConfig, consumerStr, ctx)
        );
        Send(Parent, new NQuoterEvents::TEvQuotaUpdated(
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

NActors::IActor* CreateReadQuoter(
    const NKikimrPQ::TPQConfig& pqConfig,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionId& partition,
    TActorId tabletActor,
    const TActorId& parent,
    ui64 tabletId,
    const std::shared_ptr<TTabletCountersBase>& counters
) {
    return new TReadQuoter(pqConfig, topicConverter, config, partition, tabletActor, parent, tabletId, counters);
}


} // namespace NKikimr::NPQ
