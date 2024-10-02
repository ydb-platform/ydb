#pragma once

#include <ydb/core/kqp/common/events/workload_service.h>

#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/core/protos/kqp.pb.h>


namespace NKikimr::NKqp::NWorkload {

struct TPoolStateDescription {
    ui64 DelayedRequests = 0;
    ui64 RunningRequests = 0;

    ui64 AmountRequests() const;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvRefreshPoolState = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvResolvePoolResponse,
        EvFetchPoolResponse,
        EvFetchDatabaseResponse,
        EvCreatePoolResponse,
        EvPrepareTablesRequest,
        EvPlaceRequestIntoPoolResponse,
        EvFinishRequestInPool,
        EvResignPoolHandler,
        EvStopPoolHandler,
        EvCancelRequest,

        EvCpuQuotaRequest,
        EvCpuQuotaResponse,
        EvCpuLoadResponse,
        EvNodesInfoRequest,
        EvNodesInfoResponse,

        EvTablesCreationFinished,
        EvCleanupTableResponse,
        EvCleanupTablesFinished,
        EvRefreshPoolStateResponse,
        EvDelayRequestResponse,
        EvStartRequestResponse,
        EvCleanupRequestsResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Workload manager events
    struct TEvRefreshPoolState : public NActors::TEventPB<TEvRefreshPoolState, NKikimrKqp::TEvRefreshPoolState, EvRefreshPoolState> {
    };

    struct TEvResolvePoolResponse : public NActors::TEventLocal<TEvResolvePoolResponse, EvResolvePoolResponse> {
        TEvResolvePoolResponse(Ydb::StatusIds::StatusCode status, const NResourcePool::TPoolSettings& poolConfig, TPathId pathId, bool defaultPoolCreated, TEvPlaceRequestIntoPool::TPtr event, NYql::TIssues issues = {})
            : Status(status)
            , PoolConfig(poolConfig)
            , PathId(pathId)
            , DefaultPoolCreated(defaultPoolCreated)
            , Event(std::move(event))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NResourcePool::TPoolSettings PoolConfig;
        const TPathId PathId;
        const bool DefaultPoolCreated;
        TEvPlaceRequestIntoPool::TPtr Event;
        const NYql::TIssues Issues;
    };

    struct TEvFetchPoolResponse : public NActors::TEventLocal<TEvFetchPoolResponse, EvFetchPoolResponse> {
        TEvFetchPoolResponse(Ydb::StatusIds::StatusCode status, const NResourcePool::TPoolSettings& poolConfig, TPathId pathId, NYql::TIssues issues)
            : Status(status)
            , PoolConfig(poolConfig)
            , PathId(pathId)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NResourcePool::TPoolSettings PoolConfig;
        const TPathId PathId;
        const NYql::TIssues Issues;
    };

    struct TEvFetchDatabaseResponse : public NActors::TEventLocal<TEvFetchDatabaseResponse, EvFetchDatabaseResponse> {
        TEvFetchDatabaseResponse(Ydb::StatusIds::StatusCode status, const TString& database, bool serverless, NYql::TIssues issues)
            : Status(status)
            , Database(database)
            , Serverless(serverless)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString Database;
        const bool Serverless;
        const NYql::TIssues Issues;
    };

    struct TEvCreatePoolResponse : public NActors::TEventLocal<TEvCreatePoolResponse, EvCreatePoolResponse> {
        TEvCreatePoolResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
    };

    struct TEvPrepareTablesRequest : public NActors::TEventLocal<TEvPrepareTablesRequest, EvPrepareTablesRequest> {
        TEvPrepareTablesRequest(const TString& database, const TString& poolId)
            : Database(database)
            , PoolId(poolId)
        {}

        const TString Database;
        const TString PoolId;
    };

    struct TEvPlaceRequestIntoPoolResponse : public NActors::TEventLocal<TEvPlaceRequestIntoPoolResponse, EvPlaceRequestIntoPoolResponse> {
        TEvPlaceRequestIntoPoolResponse(const TString& database, const TString& poolId)
            : Database(database)
            , PoolId(poolId)
        {}

        const TString Database;
        const TString PoolId;
    };

    struct TEvFinishRequestInPool : public NActors::TEventLocal<TEvFinishRequestInPool, EvFinishRequestInPool> {
        TEvFinishRequestInPool(const TString& database, const TString& poolId, TDuration duration, TDuration cpuConsumed, bool adjustCpuQuota)
            : Database(database)
            , PoolId(poolId)
            , Duration(duration)
            , CpuConsumed(cpuConsumed)
            , AdjustCpuQuota(adjustCpuQuota)
        {}

        const TString Database;
        const TString PoolId;
        const TDuration Duration;
        const TDuration CpuConsumed;
        const bool AdjustCpuQuota;
    };

    struct TEvResignPoolHandler : public NActors::TEventLocal<TEvResignPoolHandler, EvResignPoolHandler> {
        TEvResignPoolHandler(const TString& database, const TString& poolId, const TActorId& newHandler)
            : Database(database)
            , PoolId(poolId)
            , NewHandler(newHandler)
        {}

        const TString Database;
        const TString PoolId;
        const TActorId NewHandler;
    };

    struct TEvStopPoolHandler : public NActors::TEventLocal<TEvStopPoolHandler, EvStopPoolHandler> {
    };

    struct TEvCancelRequest : public NActors::TEventLocal<TEvCancelRequest, EvCancelRequest> {
        explicit TEvCancelRequest(const TString& sessionId)
            : SessionId(sessionId)
        {}

        const TString SessionId;
    };

    // Cpu load requests
    struct TEvCpuQuotaRequest : public NActors::TEventLocal<TEvCpuQuotaRequest, EvCpuQuotaRequest> {
        explicit TEvCpuQuotaRequest(double maxClusterLoad)
            : MaxClusterLoad(maxClusterLoad)
        {}

        const double MaxClusterLoad;
    };

    struct TEvCpuQuotaResponse : public NActors::TEventLocal<TEvCpuQuotaResponse, EvCpuQuotaResponse> {
        explicit TEvCpuQuotaResponse(bool quotaAccepted)
            : QuotaAccepted(quotaAccepted)
        {}

        const bool QuotaAccepted;
    };

    struct TEvCpuLoadResponse : public NActors::TEventLocal<TEvCpuLoadResponse, EvCpuLoadResponse> {
        TEvCpuLoadResponse(Ydb::StatusIds::StatusCode status, double instantLoad, ui64 cpuNumber, NYql::TIssues issues)
            : Status(status)
            , InstantLoad(instantLoad)
            , CpuNumber(cpuNumber)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const double InstantLoad;
        const ui64 CpuNumber;
        const NYql::TIssues Issues;
    };

    struct TEvNodesInfoRequest : public NActors::TEventLocal<TEvNodesInfoRequest, EvNodesInfoRequest> {
    };

    struct TEvNodesInfoResponse : public NActors::TEventLocal<TEvNodesInfoResponse, EvNodesInfoResponse> {
        explicit TEvNodesInfoResponse(ui32 nodeCount)
            : NodeCount(nodeCount)
        {}

        const ui32 NodeCount;
    };

    // Tables queries events
    struct TEvTablesCreationFinished : public NActors::TEventLocal<TEvTablesCreationFinished, EvTablesCreationFinished> {
        TEvTablesCreationFinished(bool success, NYql::TIssues issues)
            : Success(success)
            , Issues(std::move(issues))
        {}

        const bool Success;
        const NYql::TIssues Issues;
    };

    struct TEvCleanupTableResponse : public NActors::TEventLocal<TEvCleanupTableResponse, EvCleanupTableResponse> {
        TEvCleanupTableResponse(Ydb::StatusIds::StatusCode status, const TString& path, NYql::TIssues issues)
            : Status(status)
            , Path(path)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString Path;
        const NYql::TIssues Issues;
    };

    struct TEvCleanupTablesFinished : public NActors::TEventLocal<TEvCleanupTablesFinished, EvCleanupTablesFinished> {
        TEvCleanupTablesFinished(bool success, bool tablesExists, NYql::TIssues issues)
            : Success(success)
            , TablesExists(tablesExists)
            , Issues(std::move(issues))
        {}

        const bool Success;
        const bool TablesExists;
        const NYql::TIssues Issues;
    };

    struct TEvRefreshPoolStateResponse : public NActors::TEventLocal<TEvRefreshPoolStateResponse, EvRefreshPoolStateResponse> {
        TEvRefreshPoolStateResponse(Ydb::StatusIds::StatusCode status, const TPoolStateDescription& poolState, NYql::TIssues issues)
            : Status(status)
            , PoolState(poolState)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TPoolStateDescription PoolState;
        const NYql::TIssues Issues;
    };

    struct TEvDelayRequestResponse : public NActors::TEventLocal<TEvDelayRequestResponse, EvDelayRequestResponse> {
        TEvDelayRequestResponse(Ydb::StatusIds::StatusCode status, const TString& sessionId, NYql::TIssues issues)
            : Status(status)
            , SessionId(sessionId)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString SessionId;
        const NYql::TIssues Issues;
    };

    struct TEvStartRequestResponse : public NActors::TEventLocal<TEvStartRequestResponse, EvStartRequestResponse> {
        TEvStartRequestResponse(Ydb::StatusIds::StatusCode status, ui32 nodeId, const TString& sessionId, NYql::TIssues issues)
            : Status(status)
            , NodeId(nodeId)
            , SessionId(sessionId)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const ui32 NodeId;
        const TString SessionId;
        const NYql::TIssues Issues;
    };

    struct TEvCleanupRequestsResponse : public NActors::TEventLocal<TEvCleanupRequestsResponse, EvCleanupRequestsResponse> {
        TEvCleanupRequestsResponse(Ydb::StatusIds::StatusCode status, const std::vector<TString>& sesssionIds, NYql::TIssues issues)
            : Status(status)
            , SesssionIds(sesssionIds)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const std::vector<TString> SesssionIds;
        const NYql::TIssues Issues;
    };
};

}  // NKikimr::NKqp::NWorkload
