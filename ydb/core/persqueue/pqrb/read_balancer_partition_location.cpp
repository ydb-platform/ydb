#include "read_balancer.h"
#include "read_balancer_log.h"

namespace NKikimr::NPQ {

void TPersQueueReadBalancer::HandleOnInit(
    TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev,
    const TActorContext& ctx)
{
    EnqueuePartitionsLocationRequest(ev, ctx);
}

void TPersQueueReadBalancer::SendPartitionsLocationError(
    const TActorId& sender,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvPersQueue::TEvGetPartitionsLocationResponse>();
    response->Record.SetStatus(false);
    ctx.Send(sender, response.release());
}

bool TPersQueueReadBalancer::AllPartitionPipesReady() const
{
    return !TabletsInfo.empty() && ReadyPartitionTablets == TabletsInfo.size();
}

void TPersQueueReadBalancer::SchedulePartitionsLocationWakeup(const TActorContext& ctx)
{
    if (PartitionsLocationWakeupScheduled || PartitionsLocationQueue.empty()) {
        return;
    }

    const auto now = TAppData::TimeProvider->Now();
    const auto& deadline = PartitionsLocationQueue.front().Deadline;
    const auto delay = deadline > now
        ? std::max(deadline - now, TDuration::MilliSeconds(50))
        : TDuration::MilliSeconds(50);

    PartitionsLocationWakeupScheduled = true;
    ctx.Schedule(delay, new TEvents::TEvWakeup(PARTITIONS_LOCATION_WAKEUP_TAG));
}

void TPersQueueReadBalancer::EnqueuePartitionsLocationRequest(
    TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev,
    const TActorContext& ctx)
{
    const auto timeout = TDuration::MilliSeconds(ev->Get()->Record.GetTimeoutMs());

    PartitionsLocationQueue.push_back(TPartitionsLocationRequest{
        .Sender = ev->Sender,
        .Record = std::move(ev->Get()->Record),
        .Deadline = TAppData::TimeProvider->Now() + timeout,
    });

    PQ_LOG_D("Enqueue GetPartitionsLocation request"
        << ", queueSize=" << PartitionsLocationQueue.size()
        << ", timeout=" << timeout
        << ", deadline=" << PartitionsLocationQueue.back().Deadline);

    SchedulePartitionsLocationWakeup(ctx);
}

void TPersQueueReadBalancer::ProcessPartitionsLocationQueue(const TActorContext& ctx)
{
    const auto now = TAppData::TimeProvider->Now();
    std::deque<TPartitionsLocationRequest> deferred;

    while (!PartitionsLocationQueue.empty()) {
        auto request = std::move(PartitionsLocationQueue.front());
        PartitionsLocationQueue.pop_front();

        // Prefer a successful answer whenever possible, even past the deadline.
        if (TryRespondPartitionsLocation(request.Sender, request.Record, ctx)) {
            continue;
        }

        if (request.Deadline <= now) {
            PQ_LOG_D("GetPartitionsLocation request expired, sender=" << request.Sender);
            SendPartitionsLocationError(request.Sender, ctx);
            continue;
        }

        deferred.push_back(std::move(request));
    }

    PartitionsLocationQueue = std::move(deferred);
    SchedulePartitionsLocationWakeup(ctx);
}

bool TPersQueueReadBalancer::TryRespondPartitionsLocation(
    const TActorId& sender,
    const NKikimrPQ::TGetPartitionsLocation& request,
    const TActorContext& ctx)
{
    auto evResponse = std::make_unique<TEvPersQueue::TEvGetPartitionsLocationResponse>();

    auto addPartitionToResponse = [&](ui64 partitionId, ui64 tabletId) {
        if (PipesRequested.contains(tabletId)) {
            return false;
        }

        auto iter = TabletPipes.find(tabletId);
        if (iter == TabletPipes.end()) {
            GetPipeClient(tabletId, ctx);
            return false;
        }

        if (!iter->second.Ready) {
            return false;
        }

        auto* pResponse = evResponse->Record.AddLocations();
        pResponse->SetPartitionId(partitionId);
        pResponse->SetNodeId(iter->second.NodeId.GetRef());
        pResponse->SetGeneration(iter->second.Generation.GetRef());

        PQ_LOG_D("The partition location was added to response: TabletId " << tabletId
            << ", PartitionId " << partitionId
            << ", NodeId " << pResponse->GetNodeId()
            << ", Generation " << pResponse->GetGeneration());

        return true;
    };

    if (request.PartitionsSize() == 0) {
        if (!AllPartitionPipesReady()) {
            return false;
        }

        for (const auto& [partitionId, partitionInfo] : PartitionsInfo) {
            if (!addPartitionToResponse(partitionId, partitionInfo.TabletId)) {
                return false;
            }
        }
    } else {
        for (const auto& partitionInRequest : request.GetPartitions()) {
            auto partitionInfoIter = PartitionsInfo.find(partitionInRequest);
            if (partitionInfoIter == PartitionsInfo.end()) {
                SendPartitionsLocationError(sender, ctx);
                return true; // answered with error, drop from queue
            }

            if (!addPartitionToResponse(partitionInRequest, partitionInfoIter->second.TabletId)) {
                return false;
            }
        }
    }

    evResponse->Record.SetStatus(true);
    ctx.Send(sender, evResponse.release());
    return true;
}

void TPersQueueReadBalancer::Handle(
    TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev,
    const TActorContext& ctx)
{
    if (TryRespondPartitionsLocation(ev->Sender, ev->Get()->Record, ctx)) {
        return;
    }

    EnqueuePartitionsLocationRequest(ev, ctx);
}

} // namespace NKikimr::NPQ
