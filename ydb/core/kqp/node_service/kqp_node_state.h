#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/util/tuples.h>

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/str_stl.h>
#include <util/thread/lfstack.h>

namespace NKikimr {
namespace NKqp {
namespace NKqpNode {

// Task information.
struct TTaskContext {
    ui64 TaskId = 0;
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
    TInstant StartTime;

    explicit TTasksRequest(ui64 txId, TActorId executer, TInstant startTime)
        : TxId(txId)
        , Executer(executer)
        , StartTime(startTime)
    {
    }

    TTaskExpirationInfo GetExpritationInfo() const {
        return std::make_tuple(Deadline, TxId, Executer);
    }
};


class TState {
public:
    struct TCompletedTask {
        ui64 TxId;
        ui64 TaskId;
    };

    struct TExpiredRequestContext {
        ui64 TxId;
        TActorId ExecuterId;
    };

    bool Exists(ui64 txId, const TActorId& requester) const {
        return Requests.contains(std::make_pair(txId, requester));
    }

    void NewRequest(TTasksRequest&& request) {
        TTasksRequest cur(std::move(request));
        ui64 txId = cur.TxId;
        TActorId executer = cur.Executer;
        auto [it, requestInserted] = Requests.emplace(std::make_pair(txId, executer), std::move(cur));
        auto inserted = SenderIdsByTxId.insert(std::make_pair(txId, executer))->second;
        YQL_ENSURE(requestInserted && inserted);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        if (it->second.Deadline) {
            ExpiringRequests.emplace(std::make_tuple(it->second.Deadline, txId, executer));
        }
    }

    void RemoveTask(ui64 txId, ui64 taskId, bool) {
        CompletedTasks.Enqueue(TCompletedTask{.TxId=txId, .TaskId=taskId});
        return;
    }

    // return the vector of pairs where the first element is a taskId
    // and the second one is the compute actor id associated with this task.
    std::vector<std::pair<ui64, TActorId>> GetTasksByTxId(ui64 txId) {
        CleanCompletedTasks();

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
        CleanCompletedTasks();

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

private:

    void CleanCompletedTasks() {
        TVector<TCompletedTask> completedTasks;
        CompletedTasks.DequeueAllSingleConsumer(&completedTasks);
        for(const auto& c: completedTasks) {
            RemoveTaskImpl(c.TxId, c.TaskId);
        }
    }

    void RemoveTaskImpl(ui64 txId, ui64 taskId) {
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());
            auto& request = requestIt->second;

            auto taskIt = request.InFlyTasks.find(taskId);
            if (taskIt != request.InFlyTasks.end()) {
                request.InFlyTasks.erase(taskIt);

                if (request.InFlyTasks.empty()) {
                    auto expireIt = ExpiringRequests.find(request.GetExpritationInfo());
                    if (expireIt != ExpiringRequests.end()) {
                        ExpiringRequests.erase(expireIt);
                    }
                    Requests.erase(*senderIt);
                    SenderIdsByTxId.erase(senderIt);
                    YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
                }

                return;
            }
        }
    }

    std::set<std::tuple<TInstant, ui64, TActorId>> ExpiringRequests;

    THashMap<std::pair<ui64, const TActorId>, TTasksRequest> Requests;
    THashMultiMap<ui64, const TActorId> SenderIdsByTxId;
    TLockFreeStack<TCompletedTask> CompletedTasks;
};

} // namespace NKqpNode
} // namespace NKqp
} // namespace NKikimr
