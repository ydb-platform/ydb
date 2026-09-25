#include "cms_impl.h"
#include "nbs2_maintenance.h"

#include <ydb/core/base/appdata.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <utility>

namespace NKikimr::NCms {

using namespace NKikimrCms;

bool TCms::IsNbs2MaintenanceChecksEnabled(const TActorContext &ctx) const
{
    const auto* appData = AppData(ctx);
    return appData->NbsEnabled && appData->FeatureFlags.GetEnableCmsNbs2MaintenanceChecks();
}

bool TCms::CollectNbs2MaintenanceNodes(const TPermissionRequest &request,
                                       TVector<ui32> &nodeIds,
                                       TErrorInfo &error,
                                       const TActorContext &ctx) const
{
    nodeIds.clear();
    if (!ClusterInfo || ClusterInfo->IsOutdated()) {
        error.Code = TStatus::ERROR_TEMP;
        error.Reason = "Cannot collect cluster state";
        return false;
    }

    TSet<ui32> nodes;
    TDuration horizon = TDuration::Zero();
    for (const auto &action : request.GetActions()) {
        switch (action.GetType()) {
        case TAction::SHUTDOWN_HOST:
        case TAction::REBOOT_HOST:
        case TAction::RESTART_SERVICES:
            break;
        default:
            continue;
        }

        if (action.HasTenant()) {
            error.Code = TStatus::ERROR;
            error.Reason = "Tenant actions must be expanded before collecting maintenance nodes";
            return false;
        }

        const auto items = ClusterInfo->FindLockedItems(action, &ctx);
        if (items.empty()) {
            continue;
        }
        for (const auto *item : items) {
            // For the action types above FindLockedItems returns only nodes.
            nodes.insert(static_cast<const TNodeInfo *>(item)->NodeId);
        }

        // Use the same interval as CMS conflict checks. One DBSC batch must
        // cover every action, including those deferred by local quotas.
        horizon = Max(horizon, TDuration::MicroSeconds(action.GetDuration())
            + GetPermissionDuration(request, action));
    }

    if (nodes.empty()) {
        return true;
    }

    const TInstant now = ctx.Now();
    for (const auto &entry : ClusterInfo->AllNodes()) {
        const auto &node = *entry.second;
        TErrorInfo lockError;
        // Issued permissions may be in use: include all, even this task's, regardless of priority.
        // Check other restrictions with CMS time/priority rules without changing the snapshot.
        if (!node.Locks.empty()
            || node.IsLocked(lockError, State->Config.DefaultRetryTime, now, horizon, request.GetPriority()))
        {
            nodes.insert(node.NodeId);
        }
    }

    nodeIds.assign(nodes.begin(), nodes.end());
    return true;
}

void TCms::StartNbs2MaintenanceCheck(TAutoPtr<IEventHandle> request,
    const TPermissionRequest &permissionRequest, const TString &requestId,
    TVector<ui32> nodeIds, TNbs2MaintenanceContinuation continuation)
{
    Y_ABORT_UNLESS(!PendingNbs2MaintenanceCheck);
    Y_ABORT_UNLESS(request && !nodeIds.empty() && continuation);

    auto pending = MakeHolder<TPendingNbs2MaintenanceCheck>();
    pending->AttemptId = ++NextNbs2MaintenanceAttemptId;
    pending->Request = std::move(request);
    pending->PermissionRequest = permissionRequest;
    pending->NodeIds = std::move(nodeIds);
    pending->RequestId = requestId;
    if (requestId) {
        if (const auto it = State->ScheduledRequests.find(requestId); it != State->ScheduledRequests.end()) {
            // Compare freshness against the stored request, not the priority-adjusted copy.
            pending->ScheduledRequest.ConstructInPlace(it->second);
        }
        if (const auto it = State->MaintenanceRequests.find(requestId); it != State->MaintenanceRequests.end()) {
            pending->MaintenanceTaskId.ConstructInPlace(it->second);
        }
    }
    pending->Continue = std::move(continuation);
    pending->Checker = RegisterWithSameMailbox(CreateNbs2MaintenanceChecker(
        SelfId(), pending->AttemptId, pending->NodeIds, State->Config.InfoCollectionTimeout));
    PendingNbs2MaintenanceCheck = std::move(pending);
}

bool TCms::IsNbs2MaintenanceRequestCurrent(const TPendingNbs2MaintenanceCheck &pending) const
{
    if (!pending.RequestId) {
        // Another create may have persisted the same task id while we waited.
        return !pending.PermissionRequest.HasMaintenanceTaskId()
            || !State->MaintenanceTasks.contains(pending.PermissionRequest.GetMaintenanceTaskId());
    }

    const auto it = State->ScheduledRequests.find(pending.RequestId);
    if (it == State->ScheduledRequests.end() || !pending.ScheduledRequest) {
        return false;
    }
    const auto &current = it->second;
    const auto &original = *pending.ScheduledRequest;
    if (!google::protobuf::util::MessageDifferencer::Equals(current.Request, original.Request)) {
        return false;
    }

    const auto taskId = State->MaintenanceRequests.find(pending.RequestId);
    if (!pending.MaintenanceTaskId) {
        return taskId == State->MaintenanceRequests.end();
    }
    if (taskId == State->MaintenanceRequests.end() || taskId->second != *pending.MaintenanceTaskId) {
        return false;
    }
    const auto task = State->MaintenanceTasks.find(*pending.MaintenanceTaskId);
    // RequestId also distinguishes a task dropped and recreated with the same uid.
    return task != State->MaintenanceTasks.end()
        && task->second.RequestId == pending.RequestId && task->second.Owner == original.Owner;
}

void TCms::CancelNbs2MaintenanceCheck(const TActorContext &ctx)
{
    if (PendingNbs2MaintenanceCheck) {
        ctx.Send(PendingNbs2MaintenanceCheck->Checker, new TEvents::TEvPoisonPill);
        PendingNbs2MaintenanceCheck.Reset();
    }
}

void TCms::Handle(TEvPrivate::TEvNbs2MaintenanceResult::TPtr &ev, const TActorContext &ctx)
{
    if (!PendingNbs2MaintenanceCheck
        || ev->Sender != PendingNbs2MaintenanceCheck->Checker
        || ev->Get()->AttemptId != PendingNbs2MaintenanceCheck->AttemptId)
    {
        return;
    }

    auto pending = std::move(PendingNbs2MaintenanceCheck);
    auto &result = *ev->Get();
    // Manual approval bypasses DisableMaintenance, but still requires the NBS2 check.
    const bool isManualApproval = pending->Request->GetTypeRewrite() == TEvCms::TEvManageRequestRequest::EventType
        && pending->Request->Get<TEvCms::TEvManageRequestRequest>()->Record.GetCommand() == TManageRequestRequest::APPROVE;
    bool outdated = (State->Config.DisableMaintenance && !isManualApproval)
        || !IsNbs2MaintenanceChecksEnabled(ctx)
        || !IsNbs2MaintenanceRequestCurrent(*pending);

    if (!outdated) {
        // Recollect with current locks, configuration and time. Changes to
        // unrelated tasks do not invalidate an otherwise unchanged batch.
        TVector<ui32> nodeIds;
        TErrorInfo error;
        outdated = !CollectNbs2MaintenanceNodes(pending->PermissionRequest, nodeIds, error, ctx)
            || nodeIds != pending->NodeIds;
    }
    if (outdated) {
        result.Status = TStatus::ERROR_TEMP;
        result.Reason = "Maintenance request, configuration or node set changed during DBSController check; retry the request";
        result.BlockingPartitionIds.clear();
    }

    if (result.Status == TStatus::ALLOW) {
        ClusterInfo->ApplyNodeLimits(
            State->Config.ClusterLimits.GetDisabledNodesLimit(), State->Config.ClusterLimits.GetDisabledNodesRatioLimit(),
            State->Config.TenantLimits.GetDisabledNodesLimit(), State->Config.TenantLimits.GetDisabledNodesRatioLimit());
    }

    pending->Continue(pending->Request, result, ctx);
    ResumeQueue();
}

TErrorInfo TCms::GetNbs2MaintenanceError(const TEvPrivate::TEvNbs2MaintenanceResult &result,
    const TActorContext &ctx) const
{
    TErrorInfo error;
    error.Code = result.Status;
    TString reason = result.Reason;
    if (!result.BlockingPartitionIds.empty()) {
        reason += TStringBuilder() << "; BlockingPartitionIds: " << JoinSeq(", ", result.BlockingPartitionIds);
    }
    error.Reason = reason;
    error.Deadline = ctx.Now() + State->Config.DefaultRetryTime;
    return error;
}

} // namespace NKikimr::NCms
