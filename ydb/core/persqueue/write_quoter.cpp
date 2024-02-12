#include "partition_util.h"
#include "read_quoter.h"

namespace NKikimr::NPQ  {

TWriteQuoter::TWriteQuoter(
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionId& partition,
    TActorId tabletActor,
    const TActorId& parent,
    ui64 tabletId,
    bool isLocalDc,
    const TTabletCountersBase& counters,
    const TActorContext& ctx
)
    : TPartitionQuoterBase(
            topicConverter, config, partition, tabletActor, parent,
            AppData()->PQConfig.GetQuotingConfig().GetEnableQuoting() ? TMaybe<TQuotaTracker>{CreatePartitionTotalQuotaTracker(config, ctx)}
                                                                      : Nothing(),
            tabletId, counters, 1
    )
    , IsLocalDC(isLocalDc)
    , QuotingEnabled(AppData()->PQConfig.GetQuotingConfig().GetEnableQuoting())
    , AccountQuotingEnabled(IsQuotingEnabled(AppData()->PQConfig, isLocalDc))
{
    UpdateQuotaConfigImpl(true, ctx);
}

void TWriteQuoter::OnAccountQuotaApproved(TRequestContext&& context) {
    CheckTotalPartitionQuota(std::move(context));
}

void TWriteQuoter::HandleQuotaRequestImpl(TRequestContext& context) {
    Y_UNUSED(context);
    //ToDo !! - check, do nothing?
}

void TWriteQuoter::HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr& ev) {
    if (AccountQuotaTracker) {
        Send(
            AccountQuotaTracker->Actor,
            new NAccountQuoterEvents::TEvConsumed(ev->Get()->ConsumedBytes, ev->Get()->RequestCookie)
        );
    }
}

void TWriteQuoter::HandleWakeUpImpl() {
}

void TWriteQuoter::UpdateQuotaConfigImpl(bool, const TActorContext& ctx) {
    AccountQuotingEnabled = IsQuotingEnabled(AppData()->PQConfig, IsLocalDC);
    if (PartitionTotalQuotaTracker.Defined()) {
        ctx.Send(GetParent(), NReadQuoterEvents::TEvQuotaCountersUpdated::WriteCounters(PartitionTotalQuotaTracker->GetTotalSpeed()));
    }
}

THolder<TAccountQuoterHolder> TWriteQuoter::CreateAccountQuotaTracker(const TString&, const TActorContext& ctx) const {
    TActorId actorId;
    if (GetTabletActor() && AccountQuotingEnabled) {
        actorId = TActivationContext::Register(
            new TAccountWriteQuoter(
                GetTabletActor(),
                SelfId(),
                GetTabletId(),
                TopicConverter,
                GetPartition(),
                Counters,
                ctx
            ),
            GetParent()
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

THolder<TAccountQuoterHolder>& TWriteQuoter::GetAccountQuotaTracker(const THolder<TEvPQ::TEvRequestQuota>&) {
    if (!AccountQuotaTracker && AccountQuotingEnabled && QuotingEnabled)
        AccountQuotaTracker = CreateAccountQuotaTracker(TString{}, ActorContext());
    return AccountQuotaTracker;
}

} //namespace

