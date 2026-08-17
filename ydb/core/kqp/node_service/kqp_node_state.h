#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/util/tuples.h>

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/str_stl.h>

namespace NKikimr::NKqp {

// The request from TEvStartKqpTasksRequest
struct TNodeRequest : TMoveOnly {
    using TPtr = std::shared_ptr<TNodeRequest>;
    using TTaskInfo = std::pair<ui64 /* taskId */, TActorId /* computeActorId */>;
    using TExpirationInfo = std::tuple<TInstant, ui64 /* txId */, TActorId /* executerId */>;
    // NOTE: in case there are multiple executers for a single txId - separate them by executerId.
    // TODO: is it even possible to have multiple executers to send the same TxId?

    explicit TNodeRequest(TActorId queryManId) : QueryManId(queryManId) {}

    const TActorId QueryManId;

    ui64 TxId = 0;
    NScheduler::NHdrf::NDynamic::TQueryPtr Query;
    TInstant StartTime;
    TInstant Deadline;
    std::unordered_map<ui64 /* taskId */, std::optional<TActorId>> Tasks;

    bool ExecutionCancelled = false;
};

inline TNodeRequest::TExpirationInfo GetExpirationInfo(TInstant deadline, ui64 txId, TActorId executerId) {
    return std::make_tuple(deadline, txId, executerId);
}

class TNodeState {
    static constexpr ui64 BucketsCount = 64;

public:
    bool AddRequest(TActorId executerId, TActorId queryManId, bool& cancelled, TActorId& requestQueryManId);
    bool UpdateRequest(TActorId executerId, ui64 txId, NScheduler::NHdrf::NDynamic::TQueryPtr query, TInstant startTime, TInstant deadline, std::vector<ui64>& tasks, ui64& taskCount);
    std::vector<TNodeRequest::TExpirationInfo> ClearExpiredRequests();

    bool OnTaskStarted(TActorId executerId, ui64 taskId, TActorId computeActorId);
    void OnTaskFinished(ui64 txId, TActorId executerId, ui64 taskId, bool success);

    // Returns only started tasks
    std::vector<TNodeRequest::TTaskInfo> GetTasksByExecuterId(TActorId executerId) const;

    void MarkRequestAsCancelled(TActorId executerId);
    void DumpInfo(TStringStream& str, const TCgiParameters& cgiParams) const;
    bool ValidateComputeActorId(const TString& caId, TActorId& computeActorId) const;
    bool ValidateKqpExecuterId(const TString& exId, TActorId& kqpExecuterId) const;

private:
    inline auto& GetBucketByExecuterId(TActorId executerId) {
        return Buckets.at(executerId.Hash() % Buckets.size());
    }

    inline const auto& GetBucketByExecuterId(TActorId executerId) const {
        return Buckets.at(executerId.Hash() % Buckets.size());
    }

private:
    // Split all requests into buckets - for better concurrent access.
    struct TBucket {
        TRWMutex Mutex;
        std::set<TNodeRequest::TExpirationInfo> ExpiringRequests; // protected by Mutex
        std::unordered_map<TActorId /* executerId */, TNodeRequest> Requests;    // protected by Mutex
        // TODO: is it even possible to have multiple executers to send the same TxId?
    };
    std::array<TBucket, BucketsCount> Buckets;
};

} // namespace NKikimr::NKqp
