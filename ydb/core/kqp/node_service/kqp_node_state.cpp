#include "kqp_node_state.h"

namespace NKikimr::NKqp {

void TNodeState::AddRequest(TNodeRequest&& request) {
    auto& bucket = GetBucketByTxId(request.TxId);

    TWriteGuard guard(bucket.Mutex);
    auto txId = request.TxId;
    auto [it, requestInserted] = bucket.Requests.emplace(txId, std::move(request));

    YQL_ENSURE(requestInserted);
    if (it->second.Deadline) {
        bucket.ExpiringRequests.emplace(std::make_pair(it->second.Deadline, txId));
    }
}

bool TNodeState::HasRequest(ui64 txId) const {
    const auto& bucket = GetBucketByTxId(txId);
    TReadGuard guard(bucket.Mutex);
    return bucket.Requests.contains(txId);
}

std::vector<ui64> TNodeState::ClearExpiredRequests() {
    std::vector<ui64> requests;

    for (auto& bucket : Buckets) {
        TWriteGuard guard(bucket.Mutex);

        auto it = bucket.ExpiringRequests.begin();
        auto now = TAppData::TimeProvider->Now();
        while (it != bucket.ExpiringRequests.end() && std::get<TInstant>(*it) < now) {
            auto txId = std::get<ui64>(*it);
            auto delIt = it++;
            bucket.ExpiringRequests.erase(delIt);
            requests.emplace_back(txId);
        }
    }

    return requests;
}

bool TNodeState::OnTaskStarted(ui64 txId, ui64 taskId, TActorId computeActorId) {
    auto& bucket = GetBucketByTxId(txId);

    TWriteGuard guard(bucket.Mutex);

    if (auto requestIt = bucket.Requests.find(txId); requestIt != bucket.Requests.end()) {
        auto& request = requestIt->second;
        if (auto taskIt = request.Tasks.find(taskId); taskIt != request.Tasks.end()) {
            taskIt->second = computeActorId;
        } else {
            // If request has more tasks, then this one may already be finished and not exist.
            return false;
        }
    } else {
        // If request has a single task, then it may already be finished - and request may not exist.
        return false;
    }

    return true;
}

void TNodeState::OnTaskFinished(ui64 txId, ui64 taskId, bool success) {
    auto& bucket = GetBucketByTxId(txId);

    TWriteGuard guard(bucket.Mutex);
    auto requestIt = bucket.Requests.find(txId);
    YQL_ENSURE(requestIt != bucket.Requests.end());
    auto& request = requestIt->second;

    auto taskIt = request.Tasks.find(taskId);
    if (taskIt != request.Tasks.end()) {
        request.Tasks.erase(taskIt);
        request.ExecutionCancelled |= !success;

        if (request.Tasks.empty()) {
            bucket.ExpiringRequests.erase(request.GetExpirationInfo());

            if (requestIt->second.Query) {
                auto removeQueryEvent = MakeHolder<NScheduler::TEvRemoveQuery>();
                removeQueryEvent->QueryId = txId;
                Y_ENSURE(TlsActivationContext);
                auto* actorSystem = TlsActivationContext->ActorSystem();
                actorSystem->Send(MakeKqpSchedulerServiceId(actorSystem->NodeId), removeQueryEvent.Release());
            }

            bucket.Requests.erase(requestIt);
        }
    }
}

std::vector<TNodeRequest::TTaskInfo> TNodeState::GetTasksByTxId(ui64 txId) const {
    const auto& bucket = GetBucketByTxId(txId);

    TReadGuard guard(bucket.Mutex);
    std::vector<TNodeRequest::TTaskInfo> tasks;
    auto requestIt = bucket.Requests.find(txId);
    YQL_ENSURE(requestIt != bucket.Requests.end());
    for(const auto& [taskId, actorId] : requestIt->second.Tasks) {
        if (actorId) {
            tasks.push_back({taskId, *actorId});
        }
    }

    return tasks;
}

void TNodeState::DumpInfo(TStringStream& str) const {
    for (const auto& bucket : Buckets) {
        TReadGuard guard(bucket.Mutex);
        TMap<ui64, TVector<std::pair<const TActorId, const TNodeRequest*>>> byTx;

        for (const auto& [txId, request] : bucket.Requests) {
            byTx[txId].emplace_back(request.ExecuterId, &request);
        }
        for (const auto& [txId, requests] : byTx) {
            str << "    Requests:" << Endl;
            for (auto& [requester, request] : requests) {
                str << "      Requester: " << requester << Endl;
                str << "        StartTime: " << request->StartTime << Endl;
                str << "        Deadline: " << request->Deadline << Endl;
                str << "        In-fly tasks:" << Endl;
                for (auto& [taskId, actorId] : request->Tasks) {
                    str << "          Task: " << taskId << Endl;
                    if (actorId) {
                        str << "            Compute actor: " << *actorId << Endl;
                    } else {
                        str << "            Compute actor: (task not started yet)" << Endl;
                    }
                }
            }
        }
    }
}

} // namespace NKikimr::NKqp::NKqpNode
