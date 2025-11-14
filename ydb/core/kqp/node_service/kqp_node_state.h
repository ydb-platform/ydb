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
    using TExpirationInfo = std::pair<TInstant, ui64 /* txId */>;
    using TTaskInfo = std::pair<ui64 /* taskId */, TActorId>;

    explicit TNodeRequest(ui64 txId, NScheduler::NHdrf::NDynamic::TQueryPtr query, TActorId executerId, TInstant startTime)
        : TxId(txId)
        , Query(query)
        , ExecuterId(executerId)
        , StartTime(startTime)
    {
    }

    TExpirationInfo GetExpirationInfo() const {
        return std::make_tuple(Deadline, TxId);
    }

    const ui64 TxId = 0;
    THashMap<ui64 /* taskId */, std::optional<TActorId>> Tasks;
    NScheduler::NHdrf::NDynamic::TQueryPtr Query;
    const TActorId ExecuterId;
    const TInstant StartTime;

    TInstant Deadline;
    bool ExecutionCancelled = false;
};

class TNodeState {
    static constexpr ui64 BucketsCount = 64;

public:
    void AddRequest(TNodeRequest&& request);
    bool HasRequest(ui64 txId) const;
    std::vector<ui64 /* txId */> ClearExpiredRequests();

    bool OnTaskStarted(ui64 txId, ui64 taskId, TActorId computeActorId);
    void OnTaskFinished(ui64 txId, ui64 taskId, bool success);

    // Returns only started tasks
    std::vector<TNodeRequest::TTaskInfo> GetTasksByTxId(ui64 txId) const;

    void DumpInfo(TStringStream& str) const;

private:
    inline auto& GetBucketByTxId(ui64 txId) {
        return Buckets.at(txId % Buckets.size());
    }

    inline const auto& GetBucketByTxId(ui64 txId) const {
        return Buckets.at(txId % Buckets.size());
    }

private:
    // Split all requests into buckets - for better concurrent access.
    struct TBucket {
        TRWMutex Mutex;
        THashMap<ui64 /* txId */, TNodeRequest> Requests;         // protected by Mutex
        std::set<TNodeRequest::TExpirationInfo> ExpiringRequests; // protected by Mutex
        // TODO: is it even possible to have multiple executers send the same TxId?
    };
    std::array<TBucket, BucketsCount> Buckets;
};

} // namespace NKikimr::NKqp
