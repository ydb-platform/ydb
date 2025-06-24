#include "partition_util.h"
#include "read_quoter.h"

namespace NKikimr::NPQ  {

TWriteQuoter::TWriteQuoter(
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const NKikimrPQ::TPQConfig& pqConfig,
    const TPartitionId& partition,
    TActorId tabletActor,
    ui64 tabletId,
    const TTabletCountersBase& counters
)
    : TPartitionQuoterBase(
            topicConverter, config, partition, tabletActor, pqConfig.GetQuotingConfig().GetEnableQuoting(),
            tabletId, counters, 1
    )
    , QuotingEnabled(pqConfig.GetQuotingConfig().GetEnableQuoting())
{
}

void TWriteQuoter::Bootstrap(const TActorContext& ctx) {
    UpdateQuotaConfigImpl(true, ctx);
    TBase::Bootstrap(ctx);
}

void TWriteQuoter::OnAccountQuotaApproved(TRequestContext&& context) {
    CheckTotalPartitionQuota(std::move(context));
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
        actorId = TActivationContext::Register(
            new TAccountWriteQuoter(
                GetTabletActor(),
                SelfId(),
                GetTabletId(),
                TopicConverter,
                GetPartition(),
                Counters,
                ActorContext()
            )
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

} //namespace

