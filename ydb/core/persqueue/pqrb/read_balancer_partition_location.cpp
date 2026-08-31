#include "read_balancer.h"
#include "read_balancer_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PERSQUEUE_READ_BALANCER

namespace NKikimr::NPQ {

void TPersQueueReadBalancer::HandleOnInit(
    TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev,
    const TActorContext& ctx)
{
    EnqueuePartitionsLocationRequest(ev, ctx);
}

void TPersQueueReadBalancer::SendPartitionsLocationError(
    const TActorId& sender,
    const TActorContext& ctx,
    ui64 cookie)
{
    auto response = std::make_unique<TEvPersQueue::TEvGetPartitionsLocationResponse>();
    response->Record.SetStatus(false);
    ctx.Send(sender, response.release(), 0, cookie);
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

    PartitionsLocationWakeupScheduled = true;
    ctx.Schedule(PARTITIONS_LOCATION_WAKEUP_QUANTUM, new TEvents::TEvWakeup(PARTITIONS_LOCATION_WAKEUP_TAG));
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
        .Cookie = ev->Cookie,
    });

    YDB_LOG_DEBUG("Enqueue GetPartitionsLocation request",
        {"logPrefix", LogPrefix()},
        {"queueSize", PartitionsLocationQueue.size()},
        {"timeout", timeout},
        {"deadline", PartitionsLocationQueue.back().Deadline});

    SchedulePartitionsLocationWakeup(ctx);
}

void TPersQueueReadBalancer::ProcessPartitionsLocationQueue(const TActorContext& ctx)
{
    const auto now = TAppData::TimeProvider->Now();
    size_t write = 0;
    for (size_t read = 0; read < PartitionsLocationQueue.size(); ++read) {
        auto& request = PartitionsLocationQueue[read];

        // Prefer a successful answer whenever possible, even past the deadline.
        if (TryRespondPartitionsLocation(request.Sender, request.Record, ctx, request.Cookie)) {
            continue;
        }

        if (request.Deadline <= now) {
            YDB_LOG_DEBUG("GetPartitionsLocation request expired",
                {"logPrefix", LogPrefix()},
                {"sender", request.Sender});
            SendPartitionsLocationError(request.Sender, ctx, request.Cookie);
            continue;
        }

        if (write != read) {
            PartitionsLocationQueue[write] = std::move(request);
        }
        ++write;
    }
    PartitionsLocationQueue.erase(PartitionsLocationQueue.begin() + write, PartitionsLocationQueue.end());
    SchedulePartitionsLocationWakeup(ctx);
}

bool TPersQueueReadBalancer::TryRespondPartitionsLocation(
    const TActorId& sender,
    const NKikimrPQ::TGetPartitionsLocation& request,
    const TActorContext& ctx,
    ui64 cookie)
{
    auto pipeIsReady = [&](ui64 tabletId) {
        if (PipesRequested.contains(tabletId)) {
            return false;
        }

        auto iter = TabletPipes.find(tabletId);
        if (iter == TabletPipes.end()) {
            GetPipeClient(tabletId, ctx);
            return false;
        }

        return iter->second.Ready
            && iter->second.NodeId.Defined()
            && iter->second.Generation.Defined();
    };

    if (request.PartitionsSize() == 0) {
        if (!AllPartitionPipesReady()) {
            return false;
        }

        for (const auto& [partitionId, partitionInfo] : PartitionsInfo) {
            if (!pipeIsReady(partitionInfo.TabletId)) {
                return false;
            }
        }
    } else {
        for (const auto& partitionInRequest : request.GetPartitions()) {
            auto partitionInfoIter = PartitionsInfo.find(partitionInRequest);
            if (partitionInfoIter == PartitionsInfo.end()) {
                SendPartitionsLocationError(sender, ctx, cookie);
                return true; // answered with error, drop from queue
            }

            if (!pipeIsReady(partitionInfoIter->second.TabletId)) {
                return false;
            }
        }
    }

    auto evResponse = std::make_unique<TEvPersQueue::TEvGetPartitionsLocationResponse>();

    auto addPartitionToResponse = [&](ui64 partitionId, ui64 tabletId) {
        auto iter = TabletPipes.find(tabletId);
        if (iter == TabletPipes.end() || !iter->second.NodeId.Defined() || !iter->second.Generation.Defined()) {
            return false;
        }
        auto* pResponse = evResponse->Record.AddLocations();
        pResponse->SetPartitionId(partitionId);
        pResponse->SetNodeId(*iter->second.NodeId);
        pResponse->SetGeneration(*iter->second.Generation);

        YDB_LOG_DEBUG("The partition location was added to response",
            {"logPrefix", LogPrefix()},
            {"tabletId", tabletId},
            {"partitionId", partitionId},
            {"nodeId", pResponse->GetNodeId()},
            {"generation", pResponse->GetGeneration()});
        return true;
    };

    bool filled = true;
    if (request.PartitionsSize() == 0) {
        for (const auto& [partitionId, partitionInfo] : PartitionsInfo) {
            if (!addPartitionToResponse(partitionId, partitionInfo.TabletId)) {
                filled = false;
                break;
            }
        }
    } else {
        for (const auto& partitionInRequest : request.GetPartitions()) {
            auto partitionInfoIter = PartitionsInfo.find(partitionInRequest);
            if (partitionInfoIter == PartitionsInfo.end()
                || !addPartitionToResponse(partitionInRequest, partitionInfoIter->second.TabletId))
            {
                filled = false;
                break;
            }
        }
    }

    if (!filled) {
        SendPartitionsLocationError(sender, ctx, cookie);
        return true;
    }

    evResponse->Record.SetStatus(true);
    // Echo the request cookie so clients can drop stale replies after retry.
    // Old clients ignore it; cookie 0 remains valid for mixed-version rollouts.
    ctx.Send(sender, evResponse.release(), 0, cookie);
    return true;
}

void TPersQueueReadBalancer::Handle(
    TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev,
    const TActorContext& ctx)
{
    if (TryRespondPartitionsLocation(ev->Sender, ev->Get()->Record, ctx, ev->Cookie)) {
        return;
    }

    EnqueuePartitionsLocationRequest(ev, ctx);
}

} // namespace NKikimr::NPQ
