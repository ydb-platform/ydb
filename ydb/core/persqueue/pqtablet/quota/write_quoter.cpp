#include "write_quoter.h"

#include <ydb/core/persqueue/public/config.h>

namespace NKikimr::NPQ  {

TWriteQuoter::TWriteQuoter(
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const NKikimrPQ::TPQConfig& pqConfig,
    const TPartitionId& partition,
    TActorId tabletActor,
    ui64 tabletId,
    const std::shared_ptr<TTabletCountersBase>& counters
)
    : TPartitionQuoterBase(
            topicConverter, config, partition, tabletActor, pqConfig.GetQuotingConfig().GetEnableQuoting(),
            tabletId, counters, 1
    )
    , QuotingEnabled(pqConfig.GetQuotingConfig().GetEnableQuoting())
    , PartitionDeduplicationIdQuotaTracker(config.GetPartitionConfig().GetWriteMessageDeduplicationIdPerSecond(),
        config.GetPartitionConfig().GetWriteMessageDeduplicationIdPerSecond(), TAppData::TimeProvider->Now()) // TODO MLP config
{
}

TString TWriteQuoter::BuildLogPrefix() const {
    return TStringBuilder() << "[WriteQuoter][" << Partition << "] ";
}

void TWriteQuoter::Bootstrap(const TActorContext& ctx) {
    UpdateQuotaConfigImpl(true, ctx);
    TBase::Bootstrap(ctx);
}

void TWriteQuoter::OnAccountQuotaApproved(TRequestContext&& context) {
    CheckTotalPartitionQuota(std::move(context));
}

bool TWriteQuoter::CanExaust(TInstant now) {
    return TPartitionQuoterBase::CanExaust(now) && PartitionDeduplicationIdQuotaTracker.CanExaust(now);
}

void TWriteQuoter::HandleQuotaRequestImpl(TRequestContext& context) {
    Y_UNUSED(context);
}

void TWriteQuoter::HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) {
    if (AccountQuotaTracker) {
        Send(
            AccountQuotaTracker->Actor,
            new NAccountQuoterEvents::TEvConsumed(ev->Get()->ConsumedBytes, ev->Get()->RequestCookie)
        );
    }

    PartitionDeduplicationIdQuotaTracker.Exaust(ev->Get()->ConsumedDeduplicationIds, ActorContext().Now());
}

bool TWriteQuoter::GetAccountQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig) const {
    return IsQuotingEnabled(pqConfig, PQTabletConfig.GetLocalDC());
}

void TWriteQuoter::HandleWakeUpImpl() {
}

void TWriteQuoter::UpdateQuotaConfigImpl(bool, const TActorContext&) {

}

THolder<TAccountQuoterHolder> TWriteQuoter::CreateAccountQuotaTracker() const {
    TActorId actorId;
    if (GetTabletActor() && GetAccountQuotingEnabled(AppData()->PQConfig)) {
        actorId = TActivationContext::RegisterWithSameMailbox(
            new TAccountWriteQuoter(
                GetTabletActor(),
                SelfId(),
                GetTabletId(),
                TopicConverter,
                GetPartition(),
                Counters,
                ActorContext()
            ),
            TabletActorId
        );
    }
    if (actorId) {
        return MakeHolder<TAccountQuoterHolder>(actorId, Counters);
    } else {
        return nullptr;
    }
}

void TWriteQuoter::UpdateCounters(const TActorContext&) {
}

void TWriteQuoter::HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TWriteQuoter:: HandleUpdateAccountQuotaCounters(NAccountQuoterEvents::TEvCounters::TPtr&, const TActorContext&) {

}

ui64 TWriteQuoter::GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext&) const {
    return pqTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
}

ui64 TWriteQuoter::GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext&) const {
        return pqTabletConfig.GetPartitionConfig().GetBurstSize();
}

IEventBase* TWriteQuoter::MakeQuotaApprovedEvent(TRequestContext& context) {
    return new TEvPQ::TEvApproveWriteQuota(context.Request->Cookie, context.AccountQuotaWaitTime, ActorContext().Now() - context.PartitionQuotaWaitStart);
};

TAccountQuoterHolder* TWriteQuoter::GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>&) {
    if (!GetAccountQuotingEnabled(AppData()->PQConfig) && !QuotingEnabled) {
        return nullptr;
    }
    if (!TopicConverter)
        return nullptr;

    if (!AccountQuotaTracker) {
        AccountQuotaTracker = CreateAccountQuotaTracker();
    }
    return AccountQuotaTracker.Get();
}

NActors::IActor* CreateWriteQuoter(
    const NKikimrPQ::TPQConfig& pqConfig,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionId& partition,
    TActorId tabletActor,
    ui64 tabletId,
    const std::shared_ptr<TTabletCountersBase>& counters
) {
    return new TWriteQuoter(topicConverter, config, pqConfig, partition, tabletActor, tabletId, counters);
}

} // namespace NKikimr::NPQ
