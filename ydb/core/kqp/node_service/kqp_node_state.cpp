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

bool TNodeState::AddTasksToRequest(ui64 txId, TActorId executerId, const TVector<ui64>& taskIds) {
    auto& bucket = GetBucketByTxId(txId);
    TWriteGuard guard(bucket.Mutex);

    const auto [requestsBegin, requestsEnd] = bucket.Requests.equal_range(txId);
    for (auto requestIt = requestsBegin; requestIt != requestsEnd; ++requestIt) {
        if (requestIt->second.ExecuterId == executerId) {
            YQL_ENSURE(!requestIt->second.ExecutionCancelled, "Request TxId: " << txId << " is already cancelled");
            for (ui64 taskId : taskIds) {
                auto [_, inserted] = requestIt->second.Tasks.emplace(taskId, std::nullopt);
                YQL_ENSURE(inserted, "Task " << taskId << " already exists in request TxId: " << txId);
            }
            return true;
        }
    }
    return false;
}
bool TNodeState::HasRequest(ui64 txId) const {
    const auto& bucket = GetBucketByTxId(txId);
    TReadGuard guard(bucket.Mutex);
    return bucket.Requests.contains(txId);
}

bool TNodeState::IsRequestCancelled(ui64 txId, TActorId executerId) const {
    const auto& bucket = GetBucketByTxId(txId);
    TReadGuard guard(bucket.Mutex);

    const auto [requestsBegin, requestsEnd] = bucket.Requests.equal_range(txId);
    for (auto requestIt = requestsBegin; requestIt != requestsEnd; ++requestIt) {
        if (requestIt->second.ExecuterId == executerId) {
            return requestIt->second.ExecutionCancelled;
        }
    }
    return false;
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

void TNodeState::MarkRequestAsCancelled(ui64 txId) {
    auto& bucket = GetBucketByTxId(txId);
    TWriteGuard guard(bucket.Mutex);

    const auto [requestsBegin, requestsEnd] = bucket.Requests.equal_range(txId);
    for (auto requestIt = requestsBegin; requestIt != requestsEnd; ++requestIt) {
        requestIt->second.ExecutionCancelled = true;
    }
}

void TNodeState::DumpInfo(TStringStream& str) const {
    HTML(str) {
        str << Endl << "Transactions:" << Endl;
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() {str << "TxId";}
                    TABLEH() {str << "Executer";}
                    TABLEH() {str << "StartTime";}
                    TABLEH() {str << "Deadline";}
                }
            }
            TABLEBODY() {
                for (const auto& bucket : Buckets) {
                    TReadGuard guard(bucket.Mutex);
                    TMap<ui64, TVector<std::pair<const TActorId, const TNodeRequest*>>> byTx;

                    for (const auto& [txId, request] : bucket.Requests) {
                        byTx[txId].emplace_back(request.ExecuterId, &request);
                    }

                    for (const auto& [txId, requests] : byTx) {
                        for (auto& [requester, request] : requests) {
                            TABLER() {
                                TABLED() {str << txId;}
                                TABLED() {
                                    HREF(TStringBuilder() << "/node/" << requester.NodeId() << "/actors/kqp_node?ex=" << requester)  {
                                        str << requester;
                                    }
                                }
                                TABLED() {str << request->StartTime;}
                                TABLED() {str << request->Deadline;}
                            }
                        }
                    }
                }
            }
        }

        str << Endl << "Tasks:" << Endl;
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() {str << "TxId";}
                    TABLEH() {str << "Executer";}
                    TABLEH() {str << "TaskId";}
                    TABLEH() {str << "ComputeActorId";}
                }
            }
            TABLEBODY() {
                for (const auto& bucket : Buckets) {
                    TReadGuard guard(bucket.Mutex);
                    TMap<ui64, TVector<std::pair<const TActorId, const TNodeRequest*>>> byTx;

                    for (const auto& [txId, request] : bucket.Requests) {
                        byTx[txId].emplace_back(request.ExecuterId, &request);
                    }

                    for (const auto& [txId, requests] : byTx) {
                        for (auto& [requester, request] : requests) {
                            for (auto& [taskId, actorId] : request->Tasks) {
                                TABLER() {
                                    TABLED() {str << txId;}
                                    TABLED() {
                                        HREF(TStringBuilder() << "/node/" << requester.NodeId() << "/actors/kqp_node?ex=" << requester)  {
                                            str << requester;
                                        }
                                    }
                                    TABLED() {str << taskId;}
                                    TABLED() {
                                        if (actorId) {
                                            HREF(TStringBuilder() << "/node/" << requester.NodeId() << "/actors/kqp_node?ca=" << *actorId)  {
                                                str << *actorId;
                                            }
                                        } else {
                                            str << "N/A";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

bool TNodeState::ValidateComputeActorId(const TString& computeActorId, TActorId& id) const {
    for (const auto& bucket : Buckets) {
        TReadGuard guard(bucket.Mutex);
        for (const auto& [_, request] : bucket.Requests) {
            for (auto& [_, actorId] : request.Tasks) {
                if (actorId && ToString(*actorId) == computeActorId) {
                    id = *actorId;
                    return true;
                }
            }
        }
    }
    return false;
}

bool TNodeState::ValidateKqpExecuterId(const TString& kqpExecuterId, ui32 nodeId, TActorId& id) const {
    for (const auto& bucket : Buckets) {
        TReadGuard guard(bucket.Mutex);
        for (const auto& [_, request] : bucket.Requests) {
            if (ToString(request.ExecuterId) == kqpExecuterId && request.ExecuterId.NodeId() == nodeId) {
                id = request.ExecuterId;
                return true;
            }
        }
    }
    return false;
}

} // namespace NKikimr::NKqp::NKqpNode
