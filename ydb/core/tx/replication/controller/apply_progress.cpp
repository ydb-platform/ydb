#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

bool TController::HasEnumeratedWorkerSets(const TReplication& replication) const {
    // Discovery installs its entire result in one tablet transaction. This
    // persisted target list is the quorum scope, including directory expansion
    // and index targets; configured paths need not name individual tables.
    bool hasTargets = false;
    for (ui64 tid = 0; tid < replication.GetNextTargetId(); ++tid) {
        const auto* target = replication.FindTarget(tid);
        if (!target || target->GetDstState() == TReplication::EDstState::Removing || target->IsIndexBuild()) {
            continue;
        }
        hasTargets = true;
        if (target->GetDstState() != TReplication::EDstState::Ready
            || target->GetStreamState() != TReplication::EStreamState::Ready
            || !target->IsWorkerSetEnumerated())
        {
            return false;
        }
    }
    return hasTargets;
}

void TController::FillApplyProgress(const TReplication& replication,
    const Ydb::Replication::SourceVersion& requested,
    Ydb::Replication::ReplicationApplyProgress& progress) const
{
    using TProgress = Ydb::Replication::ReplicationApplyProgress;
    progress.mutable_requested_version()->CopyFrom(requested);
    progress.set_pending_versions(PendingTxId.size());
    progress.set_assigned_transactions(AssignedTxIds.size());
    progress.set_commit_in_progress(CommittingTxId != 0);
    const auto block = [&](const TString& message) {
        progress.set_state(TProgress::BLOCKED);
        progress.add_issues()->set_message(message);
    };
    const auto& config = replication.GetConfig();
    const auto interval = config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
    if (!config.GetConsistencySettings().HasGlobal() || !config.HasSpecific()
        || config.GetSpecific().TargetsSize() == 0 || !interval)
    {
        return block("Boundary checks require global consistency and an explicit non-empty table scope");
    }
    if (!requested.step() || requested.tx_id() || requested.step() % interval) {
        return block("The exclusive boundary must have tx_id=0 and a positive step aligned to the commit interval");
    }
    if (replication.GetState() != TReplication::EState::Ready
        || replication.GetDesiredState() != TReplication::EState::Ready || !GetSingle())
    {
        return block("Replication must be running with no pending state change");
    }

    if (HasActiveSchemaBarrier(replication.GetId())) {
        return block("Boundary checks require completed schema changes");
    }

    bool complete = HasEnumeratedWorkerSets(replication);
    ui64 expected = 0;
    ui64 atBoundary = 0;
    ui64 withHeartbeat = 0;
    std::optional<TRowVersion> minHeartbeat;
    const TRowVersion boundary(requested.step(), requested.tx_id());
    for (ui64 tid = 0; tid < replication.GetNextTargetId(); ++tid) {
        const auto* target = replication.FindTarget(tid);
        if (!target) {
            continue;
        }
        if (target->GetKind() == TReplication::ETargetKind::Transfer || target->HasPartitionGraph() || target->IsIndexBuild()) {
            return block("Boundary checks currently require row tables with an unchanged flat CDC partition set and no index build targets");
        }
        if (target->GetDstState() == TReplication::EDstState::Error
            || target->GetStreamState() == TReplication::EStreamState::Error)
        {
            return block(target->GetIssue());
        }
        complete = complete && target->IsWorkerSetComplete();
        for (const auto wid : target->GetWorkers()) {
            ++expected;
            auto it = Workers.find(TWorkerId{replication.GetId(), tid, wid});
            if (it == Workers.end() || !it->second.HasCommand()) {
                complete = false;
                continue;
            }
            if (it->second.IsDataEnded()) {
                return block("CDC partition topology changed; a stable partition set must be established first");
            }
            if (it->second.HasHeartbeat()) {
                ++withHeartbeat;
                const auto& heartbeat = it->second.GetHeartbeat();
                if (!minHeartbeat || heartbeat < *minHeartbeat) {
                    minHeartbeat = heartbeat;
                }
                atBoundary += heartbeat >= boundary;
            }
        }
    }
    progress.set_worker_set_complete(complete && expected != 0);
    progress.set_expected_workers(expected);
    progress.set_workers_at_boundary(atBoundary);
    if (progress.worker_set_complete() && withHeartbeat == expected && minHeartbeat) {
        progress.mutable_min_applied_heartbeat()->set_step(minHeartbeat->Step);
        progress.mutable_min_applied_heartbeat()->set_tx_id(minHeartbeat->TxId);
    }
    if (!progress.worker_set_complete()) {
        progress.set_state(TProgress::INITIALIZING);
    } else if (atBoundary != expected) {
        progress.set_state(TProgress::WAITING_HEARTBEATS);
    } else if (!PendingTxId.empty() || !AssignedTxIds.empty() || CommittingTxId || AssignTxIdInFlight) {
        progress.set_state(TProgress::WAITING_COMMITS);
    } else {
        // No committed frontier is required: an empty interval can be drained
        // without allocating a write transaction. Writer heartbeats cover it.
        progress.set_state(TProgress::APPLIED);
    }
}

} // namespace NKikimr::NReplication::NController
