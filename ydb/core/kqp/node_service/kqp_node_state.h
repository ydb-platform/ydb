#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/util/tuples.h>

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <library/cpp/actors/core/actorid.h>

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
    ui64 TotalMemory = 0;
    TActorId Executer;
    TActorId TimeoutTimer;
    bool ExecutionCancelled = false;
};

struct TTxMeta {
    ui64 TotalMemory = 0;
    NRm::EKqpMemoryPool MemoryPool = NRm::EKqpMemoryPool::Unspecified;
    ui32 TotalComputeActors = 0;
    TInstant StartTime;
};

struct TState {
    THashMap<std::pair<ui64, const TActorId>, TTasksRequest> Requests;
    THashMultiMap<ui64, const TActorId> SenderIdsByTxId;
    THashMap<ui64, TTxMeta> Meta;

    bool Exists(ui64 txId, const TActorId& requester) const {
        return Requests.contains(std::make_pair(txId, requester));
    }

    TTasksRequest* GetRequest(ui64 txId, const TActorId& requester) {
        return Requests.FindPtr(std::make_pair(txId, requester));
    }

    ui64 GetTxMemory(ui64 txId, NRm::EKqpMemoryPool memoryPool) const {
        if (auto* meta = Meta.FindPtr(txId)) {
            return meta->MemoryPool == memoryPool ? meta->TotalMemory : 0;
        }
        return 0;
    }

    void NewRequest(ui64 txId, const TActorId& requester, TTasksRequest&& request, NRm::EKqpMemoryPool memoryPool) {
        auto& meta = Meta[txId];
        meta.TotalMemory += request.TotalMemory;
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

    std::tuple<TTaskContext*, TActorId, TTasksRequest*, TTxMeta*> GetTask(ui64 txId, ui64 taskId) {
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);

        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());

            auto taskIt = requestIt->second.InFlyTasks.find(taskId);
            if (taskIt != requestIt->second.InFlyTasks.end()) {
                return std::make_tuple(&taskIt->second, senderIt->second, &requestIt->second, Meta.FindPtr(txId));
            }
        }

        return std::make_tuple(nullptr, TActorId(), nullptr, nullptr);
    }

    TMaybe<TTaskContext> RemoveTask(ui64 txId, ui64 taskId, bool success,
        std::function<void(const TActorId&, const TTasksRequest&, const TTaskContext&, bool)>&& cb)
    {
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());

            auto taskIt = requestIt->second.InFlyTasks.find(taskId);
            if (taskIt != requestIt->second.InFlyTasks.end()) {
                auto task = std::move(taskIt->second);
                requestIt->second.InFlyTasks.erase(taskIt);

                Y_VERIFY_DEBUG(requestIt->second.TotalMemory >= task.Memory);
                requestIt->second.TotalMemory -= task.Memory;
                requestIt->second.ExecutionCancelled |= !success;

                auto& meta = Meta[txId];
                Y_VERIFY_DEBUG(meta.TotalMemory >= task.Memory);
                Y_VERIFY_DEBUG(meta.TotalComputeActors >= 1);
                meta.TotalMemory -= task.Memory;
                meta.TotalComputeActors--;

                cb(senderIt->second, requestIt->second, task, meta.TotalComputeActors == 0);

                if (requestIt->second.InFlyTasks.empty()) {
                    Requests.erase(*senderIt);
                    SenderIdsByTxId.erase(senderIt);
                    YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
                }
                if (meta.TotalComputeActors == 0) {
                    Meta.erase(txId);
                }

                return std::move(task);
            }
        }

        return Nothing();
    }

    TMaybe<TTasksRequest> RemoveRequest(ui64 txId, const TActorId& requester) {
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
        Y_VERIFY_DEBUG(meta.TotalMemory >= ret->TotalMemory);
        Y_VERIFY_DEBUG(meta.TotalComputeActors >= 1);
        meta.TotalMemory -= ret->TotalMemory;
        meta.TotalComputeActors -= ret->InFlyTasks.size();

        if (meta.TotalComputeActors == 0) {
            Meta.erase(txId);
        }

        return ret;
    }

    bool RemoveTx(ui64 txId, std::function<void(const TTasksRequest&)>&& cb) {
        Meta.erase(txId);

        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());
        const auto senders = SenderIdsByTxId.equal_range(txId);
        for (auto senderIt = senders.first; senderIt != senders.second; ++senderIt) {
            auto requestIt = Requests.find(*senderIt);
            YQL_ENSURE(requestIt != Requests.end());

            cb(requestIt->second);
            Requests.erase(requestIt);
        }

        auto erased = SenderIdsByTxId.erase(txId);
        YQL_ENSURE(Requests.size() == SenderIdsByTxId.size());

        return erased > 0;
    }

    ui64 UsedMemory(NRm::EKqpMemoryPool memoryPool) const {
        ui64 mem = 0;
        for (auto& [_, meta] : Meta) {
            if (meta.MemoryPool == memoryPool) {
                mem += meta.TotalMemory;
            }
        }
        return mem;
    }
};

} // namespace NKqpNode
} // namespace NKqp
} // namespace NKikimr
