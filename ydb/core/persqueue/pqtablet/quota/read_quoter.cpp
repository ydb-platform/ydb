#include "read_quoter.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {

void TReadQuoter::Bootstrap(const TActorContext& ctx) {
    TPartitionQuoterBase::Bootstrap(ctx);
    PartitionTotalMessageQuotaTracker = TQuotaTracker(
            GetTotalPartitionMessageSpeedBurst(PQTabletConfig, ctx),
            GetTotalPartitionMessageSpeed(PQTabletConfig, ctx),
            ctx.Now());
    UpdateQuotaConfigImpl(true, ctx);
}

void TReadQuoter::HandleQuotaRequestImpl(TRequestContext& context) {
    if (!context.Request || !context.Request->Request) {
        return;
    }
    auto* readRequest = context.Request->Request->CastAsLocal<TEvPQ::TEvRead>();
    if (!readRequest) {
        return;
    }
    context.Consumer = readRequest->ClientId;
    if (!context.Consumer.empty()) {
        GetOrCreateConsumerQuota(context.Consumer, ActorContext());
    }
}

void TReadQuoter::OnAccountQuotaApproved(TRequestContext&& context) {
    CheckConsumerPerPartitionQuota(std::move(context));
}

TAccountQuoterHolder* TReadQuoter::GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>& request) {
    if (!TopicConverter || !request || !request->Request) {
        return nullptr;
    }
    auto* readRequest = request->Request->CastAsLocal<TEvPQ::TEvRead>();
    if (!readRequest || readRequest->ClientId.empty()) {
        return nullptr;
    }
    auto* consumerQuota = GetOrCreateConsumerQuota(readRequest->ClientId, ActorContext());
    return consumerQuota ? consumerQuota->AccountQuotaTracker.Get() : nullptr;
}

IEventBase* TReadQuoter::MakeQuotaApprovedEvent(TRequestContext& context) {
    return new TEvPQ::TEvApproveReadQuota(IEventHandle::Downcast<TEvPQ::TEvRead>(std::move(context.Request->Request)), context.TotalQuotaWaitTime);
};

TLogPrefix TReadQuoter::BuildLogPrefix() const {
    return YDB_LOG_CREATE_MESSAGE(
        {"actorClassName", "ReadQuoter"},
        {"partition", Partition.ToString()});
}

bool TReadQuoter::CanExaust(TInstant now) {
    return TPartitionQuoterBase::CanExaust(now) && (!PartitionTotalMessageQuotaTracker.Defined() || PartitionTotalMessageQuotaTracker->CanExaust(now));
}

void TReadQuoter::CheckConsumerPerPartitionQuota(TRequestContext&& context) {
    if (!context.Request || !context.Request->Request) {
        return;
    }
    TString consumerId = context.Consumer;
    if (consumerId.empty()) {
        if (auto* readRequest = context.Request->Request->CastAsLocal<TEvPQ::TEvRead>()) {
            consumerId = readRequest->ClientId;
        }
    }
    auto consumerQuota = GetConsumerQuotaIfExists(consumerId);
    if (!consumerQuota) {
        ApproveQuota(context);
        return;
    }
    auto now = ActorContext().Now();
    if (!consumerQuota->PartitionPerConsumerQuotaTracker.CanExaust(now)
            || !consumerQuota->PartitionPerConsumerMessageQuotaTracker.CanExaust(now)
            || !consumerQuota->ReadRequests.empty()) {
        consumerQuota->ReadRequests.push_back(std::move(context));
        return;
    }
    CheckTotalPartitionQuota(std::move(context));
}

void TReadQuoter::HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) {
    if (PartitionTotalMessageQuotaTracker.Defined()) {
        PartitionTotalMessageQuotaTracker->Exaust(ev->Get()->ConsumedMessages, ActorContext().Now());
    }

    auto consumerQuota = GetConsumerQuotaIfExists(ev->Get()->Consumer);
    if (consumerQuota) {
        if (consumerQuota->AccountQuotaTracker) {
            Send(
                consumerQuota->AccountQuotaTracker->Actor,
                new NAccountQuoterEvents::TEvConsumed(ev->Get()->ConsumedBytes, ev->Get()->RequestCookie)
            );
        }
        consumerQuota->PartitionPerConsumerQuotaTracker.Exaust(ev->Get()->ConsumedBytes, ActorContext().Now());
        consumerQuota->PartitionPerConsumerMessageQuotaTracker.Exaust(ev->Get()->ConsumedMessages, ActorContext().Now());
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
    if (PartitionTotalMessageQuotaTracker.Defined()) {
        PartitionTotalMessageQuotaTracker->Update(ActorContext().Now());
    }
    ProcessPerConsumerQuotaQueue(ActorContext());
}

void TReadQuoter::ProcessPerConsumerQuotaQueue(const TActorContext& ctx) {
    for (auto& [consumerStr, consumer] : ConsumerQuotas) {
        while (consumer.PartitionPerConsumerQuotaTracker.CanExaust(ctx.Now())
                && consumer.PartitionPerConsumerMessageQuotaTracker.CanExaust(ctx.Now())
                && !consumer.ReadRequests.empty()) {
            CheckTotalPartitionQuota(std::move(consumer.ReadRequests.front()));
            consumer.ReadRequests.pop_front();
        }
    }
}

void TReadQuoter::HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const TActorContext&) {
    const TString& consumer = ev->Get()->Consumer;
    auto it = ConsumerQuotas.find(consumer);
    if (it != ConsumerQuotas.end()) {
        for (auto& context : it->second.ReadRequests) {
            ApproveQuota(context);
        }
        it->second.ReadRequests.clear();
    }

    ApproveQueuedRequestsForConsumer(consumer);

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
    PoisonChildren();
    ConsumerQuotas.clear();
    Die(ctx);
}

void TReadQuoter::PoisonChildren() {
    for (auto& consumerQuota : ConsumerQuotas) {
        if (consumerQuota.second.AccountQuotaTracker) {
            Send(consumerQuota.second.AccountQuotaTracker->Actor, new TEvents::TEvPoisonPill());
        }
    }
}

void TReadQuoter::UpdateQuotaConfigImpl(bool totalQuotaUpdated, const TActorContext& ctx) {
    TVector<std::pair<TString, ui64>> updatedQuotas;
    TVector<std::pair<TString, ui64>> updatedMessagesQuotas;
    for (auto& [consumerStr, consumerQuota] : ConsumerQuotas) {
        if (consumerQuota.PartitionPerConsumerQuotaTracker.UpdateConfigIfChanged(
            GetConsumerReadBurst(PQTabletConfig, consumerStr, ctx), GetConsumerReadSpeed(PQTabletConfig, consumerStr, ctx), ctx.Now())) {
            updatedQuotas.push_back({consumerStr, consumerQuota.PartitionPerConsumerQuotaTracker.GetTotalSpeed()});
        }

        if (consumerQuota.PartitionPerConsumerMessageQuotaTracker.UpdateConfigIfChanged(
            GetConsumerReadMessageBurst(PQTabletConfig, consumerStr, ctx), GetConsumerReadMessageSpeed(PQTabletConfig, consumerStr, ctx), ctx.Now())) {
            updatedMessagesQuotas.push_back({consumerStr, consumerQuota.PartitionPerConsumerMessageQuotaTracker.GetTotalSpeed()});
        }
    }

    totalQuotaUpdated |= PartitionTotalMessageQuotaTracker.Defined() && PartitionTotalMessageQuotaTracker->UpdateConfigIfChanged(
        GetTotalPartitionMessageSpeedBurst(PQTabletConfig, ctx), GetTotalPartitionMessageSpeed(PQTabletConfig, ctx), ctx.Now()
    );

    ui64 totalSpeed = 0;
    if (PartitionTotalQuotaTracker.Defined()) {
        totalSpeed = PartitionTotalQuotaTracker->GetTotalSpeed();
    }
    ui64 totalMessagesSpeed = 0;
    if (PartitionTotalMessageQuotaTracker.Defined()) {
        totalMessagesSpeed = PartitionTotalMessageQuotaTracker->GetTotalSpeed();
    }
    if (updatedQuotas.size() || updatedMessagesQuotas.size() || totalQuotaUpdated) {
        Send(Parent, new NQuoterEvents::TEvQuotaUpdated(updatedQuotas, totalSpeed, updatedMessagesQuotas, totalMessagesSpeed));
    }
}

ui64 TReadQuoter::GetConsumerReadSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerName, const TActorContext& ctx) const {
    const auto* readQuota = GetReadQuota(pqTabletConfig, consumerName);
    if (readQuota && readQuota->GetSpeedInBytesPerSecond() > 0) {
        return readQuota->GetSpeedInBytesPerSecond();
    }
    return (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() || consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER)
        ? pqTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetConsumerReadBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerName, const TActorContext& ctx) const {
    const auto* readQuota = GetReadQuota(pqTabletConfig, consumerName);
    if (readQuota && readQuota->GetBurstSize() > 0) {
        return readQuota->GetBurstSize();
    }
    bool doLimitInternalConsumer = AppData(ctx)->PQConfig.GetQuotingConfig().GetEnableQuoting() && consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER;
    return (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()  || doLimitInternalConsumer)
        ? pqTabletConfig.GetPartitionConfig().GetBurstSize() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetConsumerReadMessageSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerName, const TActorContext& ctx) const {
    const auto* readQuota = GetReadQuota(pqTabletConfig, consumerName);
    if (readQuota && readQuota->GetSpeedInMessagesPerSecond() > 0) {
        return readQuota->GetSpeedInMessagesPerSecond();
    }
    return (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() || consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER)
        ? pqTabletConfig.GetPartitionConfig().GetWriteSpeedInMessagesPerSecond() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetConsumerReadMessageBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& consumerName, const TActorContext& ctx) const {
    const auto* readQuota = GetReadQuota(pqTabletConfig, consumerName);
    if (readQuota && readQuota->GetBurstSizeInMessages() > 0) {
        return readQuota->GetBurstSizeInMessages();
    }
    return (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota() || consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER)
        ? pqTabletConfig.GetPartitionConfig().GetBurstSizeInMessages() * 2
        : DEFAULT_READ_SPEED_AND_BURST;
}

ui64 TReadQuoter::GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    if (pqTabletConfig.GetPartitionConfig().GetReadSpeedInBytesPerSecond() > 0) {
        return pqTabletConfig.GetPartitionConfig().GetReadSpeedInBytesPerSecond();
    }
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadSpeed(pqTabletConfig, {}, ctx) * consumersPerPartition;
}

ui64 TReadQuoter::GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    if (pqTabletConfig.GetPartitionConfig().GetReadBurstBytes() > 0) {
        return pqTabletConfig.GetPartitionConfig().GetReadBurstBytes();
    }
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadBurst(pqTabletConfig, {}, ctx) * consumersPerPartition;
}

ui64 TReadQuoter::GetTotalPartitionMessageSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    if (pqTabletConfig.GetPartitionConfig().GetReadSpeedInMessagesPerSecond() > 0) {
        return pqTabletConfig.GetPartitionConfig().GetReadSpeedInMessagesPerSecond();
    }
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadMessageSpeed(pqTabletConfig, {}, ctx) * consumersPerPartition;
}

ui64 TReadQuoter::GetTotalPartitionMessageSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext& ctx) const {
    if (pqTabletConfig.GetPartitionConfig().GetReadBurstMessages() > 0) {
        return pqTabletConfig.GetPartitionConfig().GetReadBurstMessages();
    }
    auto consumersPerPartition = AppData(ctx)->PQConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition();
    return GetConsumerReadMessageBurst(pqTabletConfig, {}, ctx) * consumersPerPartition;
}

THolder<TAccountQuoterHolder> TReadQuoter::CreateAccountQuotaTracker(const TString& user, const TActorContext& ctx) const {
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    TActorId actorId;
    if (GetTabletActor() && quotingConfig.GetEnableQuoting()) {
        AFL_ENSURE(TopicConverter)("tablet_id", TabletId)("partition_id", Partition);
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
    if (consumerStr.empty()) {
        YDB_LOG_ERROR("Refuse to create consumer quota with empty name",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"tablet_id", TabletId},
            {"partition", Partition});
        return nullptr;
    }
    auto it = ConsumerQuotas.find(consumerStr);
    if (it == ConsumerQuotas.end()) {
        TConsumerReadQuota consumer(
                CreateAccountQuotaTracker(consumerStr, ctx),
                GetConsumerReadBurst(PQTabletConfig, consumerStr, ctx),
                GetConsumerReadSpeed(PQTabletConfig, consumerStr, ctx),
                GetConsumerReadMessageBurst(PQTabletConfig, consumerStr, ctx),
                GetConsumerReadMessageSpeed(PQTabletConfig, consumerStr, ctx)
        );
        Send(Parent, new NQuoterEvents::TEvQuotaUpdated(
                {{consumerStr, consumer.PartitionPerConsumerQuotaTracker.GetTotalSpeed()}},
                GetTotalPartitionSpeed(PQTabletConfig, ctx),
                {{consumerStr, consumer.PartitionPerConsumerMessageQuotaTracker.GetTotalSpeed()}},
                GetTotalPartitionMessageSpeed(PQTabletConfig, ctx)
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
