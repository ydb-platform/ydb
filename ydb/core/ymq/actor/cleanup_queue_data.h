#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/core/ymq/base/counters.h>


#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NSQS {

class TCleanupQueueDataActor : public TActorBootstrapped<TCleanupQueueDataActor> {
private:
    enum class EState {
        LockQueue,
        UpdateLockQueue,
        GetQueue,
        GetQueueAfterLockUpdate,
        RemoveData,
        Finish
    };

public:
    TCleanupQueueDataActor(TIntrusivePtr<TMonitoringCounters> monitoringCounters);

    void Bootstrap(const TActorContext& ctx);

    STRICT_STFUNC(StateFunc,
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
        HFunc(NKqp::TEvKqp::TEvProcessResponse, HandleProcessResponse);
        IgnoreFunc(NKqp::TEvKqp::TEvCloseSessionResponse);
    )

    void RunGetQueuesQuery(EState state, TDuration sendAfter, const TActorContext& ctx);
    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void HandleProcessResponse(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx);

    void HandleError(const TString& error, const TActorContext& ctx);
    void LockQueueToRemove(TDuration runAfter, const TActorContext& ctx);
    void UpdateLock(const TActorContext& ctx);
    void ContinueRemoveData(const NKikimrMiniKQL::TValue& queueRow, const TActorContext& ctx);
    void StartRemoveData(const NKikimrMiniKQL::TValue& queueRow, const TActorContext& ctx);

    std::optional<std::pair<TString, bool>> GetNextTable() const;
    void ClearNextTable(const TActorContext& ctx);
    void OnRemovedData(ui64 removedRows, const TActorContext& ctx);
    void RunRemoveData(const TActorContext& ctx);

    void Finish(const TActorContext& ctx);


private:
    TIntrusivePtr<TMonitoringCounters> MonitoringCounters;
    TDuration RetryPeriod;

    TString LockQueueQuery;
    TString UpdateLockQueueQuery;
    TString SelectQueuesQuery;
    TString RemoveQueueFromListQuery;
    TString RemoveDataQuery;

    EState State;
    TInstant StartProcessTimestamp;

    // Queue info
    ui64 RemoveQueueTimetsamp;
    ui64 QueueIdNumber;
    bool IsFifoQueue;
    ui32 Shards;
    ui32 TablesFormat;

    ui32 ClearedTablesCount;
    ui32 ShardsToRemove;
};

} // namespace NKikimr::NSQS
