#include "partition_util.h"
#include "read_quoter.h"

namespace NKikimr::NPQ  {

TWriteQuoter::TWriteQuoter(
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    ui32 partition,
    TActorId tabletActor,
    const TActorId& parent,
    ui64 tabletId,
    bool isLocalDc,
    const TTabletCountersBase& counters,
    const TActorContext& ctx
)
    : TPartitionQuoterBase(
            topicConverter, config, partition, tabletActor, parent,
            IsQuotingEnabled(AppData()->PQConfig, isLocalDc) ? TMaybe<TQuotaTracker>{CreatePartitionTotalQuotaTracker(config, ctx)} : Nothing(),
            tabletId, counters,
            //ToDo: discuss - 1 inflight request for write quota - ?
            1
    )
    , QuotingEnabled(IsQuotingEnabled(AppData()->PQConfig, isLocalDc))
{
    if (QuotingEnabled)
        AccountQuotaTracker = CreateAccountQuotaTracker(TString{});
    UpdateQuotaConfigImpl(true, ctx);
}

void TWriteQuoter::OnAccountQuotaApproved(TRequestContext& context) {
    CheckTotalPartitionQuota(context);
}

void TWriteQuoter::HandleQuotaRequestImpl(TRequestContext& context) {
    Y_UNUSED(context);
    //ToDo !! - check, do nothing?
}

void TWriteQuoter::HandleConsumedImpl(TEvPQ::TEvConsumed::TPtr&) {
}

void TWriteQuoter::HandleWakeUpImpl() {
}

void TWriteQuoter::UpdateQuotaConfigImpl(bool, const TActorContext& ctx) {
    if (PartitionTotalQuotaTracker.Defined()) {
        ctx.Send(GetParent(), NReadQuoterEvents::TEvQuotaCountersUpdated::WriteCounters(PartitionTotalQuotaTracker->GetTotalSpeed()));
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
    return new TEvPQ::TEvApproveWriteQuota(context.Request->Get()->Cookie, context.AccountQuotaWaitTime, ActorContext().Now() - context.PartitionQuotaWaitStart);
};

THolder<TAccountQuoterHolder>& TWriteQuoter::GetAccountQuotaTracker(TEvPQ::TEvRequestQuota::TPtr&) {
    return AccountQuotaTracker;
}

} //namespace