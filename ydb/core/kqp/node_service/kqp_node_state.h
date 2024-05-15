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
    THashMap<ui64, TTaskContext> InFlyTasks;
    TInstant Deadline;
    TActorId Executer;
    TActorId TimeoutTimer;
    bool ExecutionCancelled = false;

    ui64 CalculateTotalMemory() const {
        ui64 result = 0;
        for(auto [id, task]: InFlyTasks) {
            result += task.Memory;
        }
        return result;
    }
};

struct TTxMeta {
    NRm::EKqpMemoryPool MemoryPool = NRm::EKqpMemoryPool::Unspecified;
    ui32 TotalComputeActors = 0;
    TInstant StartTime;
};

class TState {
public:
    struct TRequestId {
        ui64 TxId = 0;
        TActorId Requester;
    };

    struct TRemoveTaskContext {
        ui64 ComputeActorsNumber = 0;
        bool FinixTx = false;
        TActorId Requester;
    };

    struct ExpiredRequestContext {
        TRequestId RequestId;
        bool Exists;
    };

    bool Exists(ui64 txId, const TActorId& requester) const {
        TReadGuard guard(RWLock);
        return Requests.contains(std::make_pair(txId, requester));
    }

    void NewRequest(ui64 txId, const TActorId& requester, TTasksRequest&& request, NRm::EKqpMemoryPool memoryPool) {
        TWriteGuard guard(RWLock);
        auto& meta = Meta[txId];
        meta.TotalComputeActors += request.InFlyTasks.size();
        if (!meta.StartTime) {
            meta.StartTime = TAppData::TimeProvider->Now();
            meta.MemoryPool = memoryPool;
        } else {
            YQL_ENSURE(meta.MemoryPool == memoryPool);
        }
        auto ret = Requests.emplace(std::make_pair(txId, requester), std::move(request));
        auto inserted = SenderIdsByTxId.insert(std::make_pair(txId, requester))->second;
        YQL_ENSURE(ret.second && inserted);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
    }

    TMaybe<TRemoveTaskContext> RemoveTask(ui64 txId, ui64 taskId, bool success)
    {
        TWriteGuard guard(RWLock);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());

            auto taskIt = requestIt->second.InFlyTasks.find(taskId);
            if (taskIt != requestIt->second.InFlyTasks.end()) {
                requestIt->second.InFlyTasks.erase(taskIt);
                requestIt->second.ExecutionCancelled |= !success;

                auto& meta = Meta[txId];
                Y_DEBUG_ABORT_UNLESS(meta.TotalComputeActors >= 1);
                meta.TotalComputeActors--;

                auto ret = TRemoveTaskContext{
                    requestIt->second.InFlyTasks.size(), meta.TotalComputeActors == 0, senderIt->second
                };

                if (requestIt->second.InFlyTasks.empty()) {
                    auto bounds = ExpiringRequests.equal_range(requestIt->second.Deadline);
                    for (auto it = bounds.first; it != bounds.second; ) {
                        if (it->second.TxId == txId && it->second.Requester == senderIt->second) {
                            auto delIt = it++;
                            ExpiringRequests.erase(delIt);
                        } else {
                            ++it;
                        }
                    }
                    Requests.erase(*senderIt);
                    SenderIdsByTxId.erase(senderIt);
                    YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
                }

                if (meta.TotalComputeActors == 0) {
                    Meta.erase(txId);
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

    void InsertExpiringRequest(TInstant deadline, ui64 txId, TActorId requester) {
        TWriteGuard guard(RWLock);
        ExpiringRequests.emplace(deadline, TRequestId{txId, requester});
    }

    std::vector<ExpiredRequestContext> ClearExpiredRequests() {
        TWriteGuard guard(RWLock);
        std::vector<ExpiredRequestContext> ret;
        auto it = ExpiringRequests.begin();
        auto now = TAppData::TimeProvider->Now();
        while (it != ExpiringRequests.end() && it->first < now) {
            auto reqId = it->second;
            auto delIt = it++;
            ExpiringRequests.erase(delIt);

            auto request = RemoveRequestImpl(reqId.TxId, reqId.Requester);
            ret.push_back({reqId, bool(request)});
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
            auto& meta = Meta[txId];
            str << "  TxId: " << txId << Endl;
            str << "    MemoryPool: " << (ui32) meta.MemoryPool << Endl;
            str << "    Compute actors: " << meta.TotalComputeActors << Endl;
            str << "    Start time: " << meta.StartTime << Endl;
            str << "    Requests:" << Endl;
            for (auto& [requester, request] : requests) {
                str << "      Requester: " << requester << Endl;
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

        auto& meta = Meta[txId];
        Y_DEBUG_ABORT_UNLESS(meta.TotalComputeActors >= 1);
        meta.TotalComputeActors -= ret->InFlyTasks.size();

        if (meta.TotalComputeActors == 0) {
            Meta.erase(txId);
        }

        return ret;
    }

private:

    TRWMutex RWLock; // Lock for state bucket

    std::multimap<TInstant, TRequestId> ExpiringRequests;

    THashMap<std::pair<ui64, const TActorId>, TTasksRequest> Requests;
    THashMultiMap<ui64, const TActorId> SenderIdsByTxId;
    THashMap<ui64, TTxMeta> Meta;
};

} // namespace NKqpNode
} // namespace NKqp
} // namespace NKikimr
