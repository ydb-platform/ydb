#include "kqp_node_state.h"

namespace NKikimr::NKqp {

void TNodeState::AddRequest(TNodeRequest&& request) {
    auto& bucket = GetBucketByTxId(request.TxId);

    TWriteGuard guard(bucket.Mutex);
    auto txId = request.TxId;
    auto it = bucket.Requests.emplace(txId, std::move(request));

    if (it->second.Deadline) {
        bucket.ExpiringRequests.emplace(std::make_tuple(it->second.Deadline, txId, it->second.ExecuterId));
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

bool TNodeState::OnTaskStarted(ui64 txId, ui64 taskId, TActorId computeActorId, TActorId executerId) {
    auto& bucket = GetBucketByTxId(txId);

    TWriteGuard guard(bucket.Mutex);
    const auto [requestsBegin, requestsEnd] = bucket.Requests.equal_range(txId);

    for (auto requestIt = requestsBegin; requestIt != requestsEnd; ++requestIt) {
        if (auto& request = requestIt->second; request.ExecuterId == executerId) {
            if (auto taskIt = request.Tasks.find(taskId); taskIt != request.Tasks.end()) {
                taskIt->second = computeActorId;
                return true;
            } else {
                // If request has more tasks, then this one may already be finished and not exist.
                return false;
            }
        }
    }

    // If request(s) had a single task, then the task may already be finished - and request(s) may not exist.
    return false;
}

void TNodeState::OnTaskFinished(ui64 txId, ui64 taskId, bool success) {
    auto& bucket = GetBucketByTxId(txId);

    TWriteGuard guard(bucket.Mutex);
    const auto [requestsBegin, requestsEnd] = bucket.Requests.equal_range(txId);
    YQL_ENSURE(requestsBegin != requestsEnd);

    for (auto requestIt = requestsBegin; requestIt != requestsEnd; ++requestIt) {
        auto& request = requestIt->second;

        if (auto taskIt = request.Tasks.find(taskId); taskIt != request.Tasks.end()) {
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

            break;
        }
    }
}

std::vector<TNodeRequest::TTaskInfo> TNodeState::GetTasksByTxId(ui64 txId) const {
    std::vector<TNodeRequest::TTaskInfo> tasks;

    const auto& bucket = GetBucketByTxId(txId);
    TReadGuard guard(bucket.Mutex);

    const auto [requestsBegin, requestsEnd] = bucket.Requests.equal_range(txId);
    for (auto requestIt = requestsBegin; requestIt != requestsEnd; ++requestIt) {
        for(const auto& [taskId, actorId] : requestIt->second.Tasks) {
            if (actorId) {
                tasks.push_back({taskId, *actorId});
            }
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
