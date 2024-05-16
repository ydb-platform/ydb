#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/util/tuples.h>

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/str_stl.h>

namespace NKikimr {
namespace NKqp {
namespace NKqpNode {

// Task information.
struct TTaskContext {
    ui64 TaskId = 0;
    ui64 Memory = 0;
    ui32 Channels = 0;
    ui64 ChannelSize = 0;
    TActorId ComputeActorId;
};

// describes single TEvStartKqpTasksRequest request
struct TTasksRequest {
    // when task is finished it will be removed from this map
    using TTaskExpirationInfo = std::tuple<TInstant, ui64, TActorId>;
    THashMap<ui64, TTaskContext> InFlyTasks;
    ui64 TxId = 0;
    TInstant Deadline;
    TActorId Executer;
    TActorId TimeoutTimer;
    bool ExecutionCancelled = false;
    TInstant StartTime;

    explicit TTasksRequest(ui64 txId, TActorId executer, TInstant startTime)
        : TxId(txId)
        , Executer(executer)
        , StartTime(startTime)
    {
    }

    ui64 CalculateTotalMemory() const {
        ui64 result = 0;
        for(auto [id, task]: InFlyTasks) {
            result += task.Memory;
        }
        return result;
    }

    TTaskExpirationInfo GetExpritationInfo() const {
        return std::make_tuple(Deadline, TxId, Executer);
    }
};


class TState {
public:
    struct TRemoveTaskContext {
        ui64 ComputeActorsNumber = 0;
    };

    struct TExpiredRequestContext {
        ui64 TxId;
        TActorId ExecuterId;
    };

    bool Exists(ui64 txId, const TActorId& requester) const {
        TReadGuard guard(RWLock);
        return Requests.contains(std::make_pair(txId, requester));
    }

    void NewRequest(TTasksRequest&& request) {
        TTasksRequest cur(std::move(request));
        ui64 txId = cur.TxId;
        TActorId executer = cur.Executer;
        TWriteGuard guard(RWLock);
        auto [it, requestInserted] = Requests.emplace(std::make_pair(txId, executer), std::move(cur));
        auto inserted = SenderIdsByTxId.insert(std::make_pair(txId, executer))->second;
        YQL_ENSURE(requestInserted && inserted);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        if (it->second.Deadline) {
            ExpiringRequests.emplace(std::make_tuple(it->second.Deadline, txId, executer));
        }
    }

    TMaybe<TRemoveTaskContext> RemoveTask(ui64 txId, ui64 taskId, bool success)
    {
        TWriteGuard guard(RWLock);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());
            auto& request = requestIt->second;

            auto taskIt = request.InFlyTasks.find(taskId);
            if (taskIt != request.InFlyTasks.end()) {
                request.InFlyTasks.erase(taskIt);
                request.ExecutionCancelled |= !success;

                auto ret = TRemoveTaskContext{request.InFlyTasks.size()};

                if (request.InFlyTasks.empty()) {
                    auto expireIt = ExpiringRequests.find(request.GetExpritationInfo());
                    if (expireIt != ExpiringRequests.end()) {
                        ExpiringRequests.erase(expireIt);
                    }
                    Requests.erase(*senderIt);
                    SenderIdsByTxId.erase(senderIt);
                    YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
                }

                return ret;
            }
        }

        return Nothing();
    }

    TMaybe<TTasksRequest> RemoveRequest(ui64 txId, const TActorId& requester) {
        TWriteGuard guard(RWLock);
        return RemoveRequestImpl(txId, requester);
    }

    // return the vector of pairs where the first element is a taskId
    // and the second one is the compute actor id associated with this task.
    std::vector<std::pair<ui64, TActorId>> GetTasksByTxId(ui64 txId) {
        TWriteGuard guard(RWLock);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);
        std::vector<std::pair<ui64, TActorId>> ret;
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());
            for(const auto& [taskId, task] : requestIt->second.InFlyTasks) {
                ret.push_back({taskId, task.ComputeActorId});
            }
        }

        return ret;
    }

    std::vector<TExpiredRequestContext> ClearExpiredRequests() {
        TWriteGuard guard(RWLock);
        std::vector<TExpiredRequestContext> ret;
        auto it = ExpiringRequests.begin();
        auto now = TAppData::TimeProvider->Now();
        while (it != ExpiringRequests.end() && std::get<TInstant>(*it) < now) {
            auto txId = std::get<ui64>(*it);
            auto executerId = std::get<TActorId>(*it);
            auto delIt = it++;
            ExpiringRequests.erase(delIt);
            ret.push_back({txId, executerId});
        }

        return ret;
    }

    void GetInfo(TStringStream& str) {
        TReadGuard guard(RWLock);
        TMap<ui64, TVector<std::pair<const TActorId, const NKqpNode::TTasksRequest*>>> byTx;
        for (auto& [key, request] : Requests) {
            byTx[key.first].emplace_back(key.second, &request);
        }
        for (auto& [txId, requests] : byTx) {
            str << "    Requests:" << Endl;
            for (auto& [requester, request] : requests) {
                str << "      Requester: " << requester << Endl;
                str << "        StartTime: " << request->StartTime << Endl;
                str << "        Deadline: " << request->Deadline << Endl;
                str << "        In-fly tasks:" << Endl;
                for (auto& [taskId, task] : request->InFlyTasks) {
                    str << "          Task: " << taskId << Endl;
                    str << "            Memory: " << task.Memory << Endl;
                    str << "            Channels: " << task.Channels << Endl;
                    str << "            Compute actor: " << task.ComputeActorId << Endl;
                }
            }
        }
    }
private:

    TMaybe<TTasksRequest> RemoveRequestImpl(ui64 txId, const TActorId& requester) {
        auto key = std::make_pair(txId, requester);
        auto* request = Requests.FindPtr(key);
        if (!request) {
            return Nothing();
        }

        TMaybe<TTasksRequest> ret = std::move(*request);
        Requests.erase(key);

        const auto senders = SenderIdsByTxId.equal_range(txId);
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            if (senderIt->second == requester) {
                SenderIdsByTxId.erase(senderIt);
                break;
            }
        }

        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        return ret;
    }

private:

    TRWMutex RWLock; // Lock for state bucket

    std::set<std::tuple<TInstant, ui64, TActorId>> ExpiringRequests;

    THashMap<std::pair<ui64, const TActorId>, TTasksRequest> Requests;
    THashMultiMap<ui64, const TActorId> SenderIdsByTxId;
};

} // namespace NKqpNode
} // namespace NKqp
} // namespace NKikimr
