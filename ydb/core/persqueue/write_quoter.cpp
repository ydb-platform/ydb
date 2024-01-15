#include "partition_util.h"
#include "read_quoter.h"

namespace NKikimr::NPQ  {

TWriteQuoter::TWriteQuoter(
    TActorId partitionActor,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const NKikimrPQ::TPQTabletConfig& config,
    ui32 partition,
    TActorId tabletActor,
    ui64 tabletId,
    bool isLocalDc,
    const TTabletCountersBase& counters,
    const TActorContext& ctx
)
    : TPartitionQuoterBase(
            partitionActor, topicConverter, config, partition, tabletActor,
            IsQuotingEnabled(AppData()->PQConfig, isLocalDc) ? TMaybe<TQuotaTracker>{CreatePartitionTotalQuotaTracker(config, ctx)} : Nothing(),
            tabletId, counters,
            //ToDo: discuss - infinite inflight requests for write quota - ?
            Max<ui32>()
    )
    , QuotingEnabled(IsQuotingEnabled(AppData()->PQConfig, isLocalDc))
{
    if (QuotingEnabled)
        AccountQuotaTracker = CreateAccountQuotaTracker(TString{});
}

void TReadQuoter::OnAccountQuotaApproved(TRequestContext& context) {
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

void TWriteQuoter::UpdateQuotaConfigImpl(bool) {
}

ui64 TWriteQuoter::GetTotalPartitionSpeed(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext&) const {
    return pqTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
}

ui64 TWriteQuoter::GetTotalPartitionSpeedBurst(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TActorContext&) const {
        return pqTabletConfig.GetPartitionConfig().GetBurstSize();

}
IEventBase* TWriteQuoter::MakeQuotaApprovedEvent(TRequestContext& context) {
    return new TEvPQ::TEvApproveWriteQuota(IEventHandle::Downcast<TEvPQ::TEvWrite>(std::move(context.Request->Get()->Request)), context.WaitTime);
};

THolder<TAccountQuoterHolder>& TWriteQuoter::GetAccountQuotaTracker(TEvPQ::TEvRequestQuota::TPtr&) {
    return AccountQuotaTracker;
}

} //namespace