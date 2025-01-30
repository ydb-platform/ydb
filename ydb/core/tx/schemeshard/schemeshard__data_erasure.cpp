#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartDataErasure(const TPathId& pathId) {
    UpdateDataErasureQueueMetrics();

    auto ctx = ActorContext();

    auto it = SubDomains.find(pathId);
    if (it == SubDomains.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Start] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& tenantSchemeShardId = it->second->GetTenantSchemeShardID();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Start] Data erasure "
        "for pathId# " << pathId
        << ", tenant schemeshard# " << tenantSchemeShardId
        << ", next wakeup# " << DataErasureQueue->GetWakeupDelta()
        << ", rate# " << DataErasureQueue->GetRate()
        << ", in queue# " << DataErasureQueue->Size() << " tenants"
        << ", running# " << DataErasureQueue->RunningSize() << " tenants"
        << " at schemeshard " << TabletID());

    ui64 generation = 0;
    std::unique_ptr<TEvSchemeShard::TEvDataClenupRequest> request(
        new TEvSchemeShard::TEvDataClenupRequest(generation));

    /*RunningBorrowedCompactions[shardIdx] = */PipeClientCache->Send(
        ctx,
        ui64(tenantSchemeShardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnDataErasureTimeout(const TPathId& /*pathId*/) {}

void TSchemeShard::Handle(TEvSchemeShard::TEvDataCleanupResult::TPtr& /*ev*/, const TActorContext& /*ctx*/) {}

void TSchemeShard::UpdateDataErasureQueueMetrics() {
    if (!DataErasureQueue) {
        return;
    }

    TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_SIZE].Set(DataErasureQueue->Size());
    TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_RUNNING].Set(DataErasureQueue->RunningSize());
}

} // NKikimr::NSchemeShard
