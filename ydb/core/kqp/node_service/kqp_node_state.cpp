#include "kqp_node_state.h"

namespace NKikimr::NKqp {

bool TNodeState::AddRequest(TActorId executerId, TActorId queryManId, bool& cancelled, TActorId& requestQueryManId) {
    auto& bucket = GetBucketByExecuterId(executerId);
    TWriteGuard guard(bucket.Mutex);

    const auto& [it, inserted] = bucket.Requests.try_emplace(executerId, queryManId);
    if (inserted) {
        requestQueryManId = queryManId;
    } else {
        if (it->second.ExecutionCancelled) {
            cancelled = true;
        }
        requestQueryManId = it->second.QueryManId;
    }
    return inserted;
}

bool TNodeState::UpdateRequest(TActorId executerId, ui64 txId, NScheduler::NHdrf::NDynamic::TQueryPtr query, TInstant startTime, TInstant deadline, std::vector<ui64>& tasks, ui64& taskCount) {
    auto& bucket = GetBucketByExecuterId(executerId);
    TWriteGuard guard(bucket.Mutex);

    if (auto requestIt = bucket.Requests.find(executerId); requestIt != bucket.Requests.end()) {
        if (requestIt->second.ExecutionCancelled) {
            return false;
        }
        requestIt->second.TxId = txId;
        requestIt->second.Query = query;
        requestIt->second.StartTime = startTime;
        requestIt->second.Deadline = deadline;
        if (deadline) {
            bucket.ExpiringRequests.emplace(GetExpirationInfo(deadline, txId, executerId));
        }
        for(auto taskId : tasks) {
            const auto& [it, inserted] = requestIt->second.Tasks.try_emplace(taskId, std::nullopt);
            YQL_ENSURE(inserted, "Duplicated taskIds are requested");
        }
        taskCount = requestIt->second.Tasks.size();
        return true;
    }

    return false;
}

std::vector<TNodeRequest::TExpirationInfo> TNodeState::ClearExpiredRequests() {
    std::vector<TNodeRequest::TExpirationInfo> requests;

    for (auto& bucket : Buckets) {
        TWriteGuard guard(bucket.Mutex);

        auto it = bucket.ExpiringRequests.begin();
        auto now = TAppData::TimeProvider->Now();
        while (it != bucket.ExpiringRequests.end() && std::get<TInstant>(*it) < now) {
            requests.emplace_back(*it);
            it = bucket.ExpiringRequests.erase(it);
        }
    }

    return requests;
}

bool TNodeState::OnTaskStarted(TActorId executerId, ui64 taskId, TActorId computeActorId) {
    auto& bucket = GetBucketByExecuterId(executerId);

    TWriteGuard guard(bucket.Mutex);

    if (auto requestIt = bucket.Requests.find(executerId); requestIt != bucket.Requests.end()) {
        auto& request = requestIt->second;
        if (auto taskIt = request.Tasks.find(taskId); taskIt != request.Tasks.end()) {
            taskIt->second = computeActorId;
            return true;
        } else {
            // If request has more tasks, then this one may already be finished and not exist.
            return false;
        }
    }

    // If request(s) had a single task, then the task may already be finished - and request(s) may not exist.
    return false;
}

void TNodeState::OnTaskFinished(ui64 txId, TActorId executerId, ui64 taskId, bool success) {
    auto& bucket = GetBucketByExecuterId(executerId);

    TWriteGuard guard(bucket.Mutex);
    auto requestIt = bucket.Requests.find(executerId);
    YQL_ENSURE(requestIt != bucket.Requests.end());
    auto& request = requestIt->second;
    YQL_ENSURE(request.TxId == txId);

    if (auto taskIt = request.Tasks.find(taskId); taskIt != request.Tasks.end()) {
        request.Tasks.erase(taskIt);
        request.ExecutionCancelled |= !success;
    }

    if (request.Tasks.empty()) {
        bucket.ExpiringRequests.erase(GetExpirationInfo(request.Deadline, request.TxId, executerId));

        if (requestIt->second.Query) {
            auto removeQueryEvent = MakeHolder<NScheduler::TEvRemoveQuery>();
            removeQueryEvent->QueryId = txId;
            Y_ENSURE(TlsActivationContext);
            auto* actorSystem = TlsActivationContext->ActorSystem();
            actorSystem->Send(MakeKqpSchedulerServiceId(actorSystem->NodeId), removeQueryEvent.Release());
        }
        if (requestIt->second.QueryManId) {
            auto* actorSystem = TlsActivationContext->ActorSystem();
            actorSystem->Send(requestIt->second.QueryManId, new NActors::TEvents::TEvPoison());
        }
        bucket.Requests.erase(requestIt);
    }
}

std::vector<TNodeRequest::TTaskInfo> TNodeState::GetTasksByExecuterId(TActorId executerId) const {
    std::vector<TNodeRequest::TTaskInfo> tasks;

    const auto& bucket = GetBucketByExecuterId(executerId);
    TReadGuard guard(bucket.Mutex);

    if (auto requestIt = bucket.Requests.find(executerId); requestIt != bucket.Requests.end()) {
        for(const auto& [taskId, actorId] : requestIt->second.Tasks) {
            if (actorId) {
                tasks.push_back({taskId, *actorId});
            }
        }
    }

    return tasks;
}

void TNodeState::MarkRequestAsCancelled(TActorId executerId) {
    auto& bucket = GetBucketByExecuterId(executerId);
    TWriteGuard guard(bucket.Mutex);

    if (auto requestIt = bucket.Requests.find(executerId); requestIt != bucket.Requests.end()) {
        requestIt->second.ExecutionCancelled = true;
    }
}

void TNodeState::DumpInfo(TStringStream& str, const TCgiParameters& cgiParams) const {
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

                    for (const auto& [executerId, request] : bucket.Requests) {
                        TABLER() {
                            TABLED() {str << request.TxId;}
                            TABLED() {
                                HREF(NActors::NMon::BuildActorsLink("kqp_node", cgiParams, {{"ex", ToString(executerId)}, {"ca", ""}, {"sf", ""}})) {
                                    str << executerId;
                                }
                            }
                            TABLED() {str << request.StartTime;}
                            TABLED() {str << request.Deadline;}
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

                    for (const auto& [executerId, request] : bucket.Requests) {
                        for (auto& [taskId, actorId] : request.Tasks) {
                            TABLER() {
                                TABLED() {str << request.TxId;}
                                TABLED() {
                                    HREF(NActors::NMon::BuildActorsLink("kqp_node", cgiParams, {{"ex", ToString(executerId)}, {"ca", ""}, {"sf", ""}})) {
                                        str << executerId;
                                    }
                                }
                                TABLED() {str << taskId;}
                                TABLED() {
                                    if (actorId) {
                                        HREF(NActors::NMon::BuildActorsLink("kqp_node", cgiParams, {{"ca", ToString(*actorId)}, {"ex", ""}, {"sf", ""}})) {
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

bool TNodeState::ValidateComputeActorId(const TString& caId, TActorId& computeActorId) const {
    for (const auto& bucket : Buckets) {
        TReadGuard guard(bucket.Mutex);
        for (const auto& [_, request] : bucket.Requests) {
            for (auto& [_, actorId] : request.Tasks) {
                if (actorId && ToString(*actorId) == caId) {
                    computeActorId = *actorId;
                    return true;
                }
            }
        }
    }
    return false;
}

bool TNodeState::ValidateKqpExecuterId(const TString& exId, TActorId& kqpExecuterId) const {
    for (const auto& bucket : Buckets) {
        TReadGuard guard(bucket.Mutex);
        for (const auto& [executerId, request] : bucket.Requests) {
            if (ToString(executerId) == exId) {
                kqpExecuterId = executerId;
                return true;
            }
        }
    }
    return false;
}

} // namespace NKikimr::NKqp::NKqpNode
