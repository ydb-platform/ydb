#pragma once

#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr::NKqp::NWorkload {

struct TPoolStateDescription {
    ui64 DelayedRequests = 0;
    ui64 RunningRequests = 0;

    ui64 AmountRequests() const {
        return DelayedRequests + RunningRequests;
    }
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvCancelRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvUpdatePoolsLeases,
        EvRefreshPoolState,

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

    // Events
    struct TEvCancelRequest : public NActors::TEventLocal<TEvCancelRequest, EvCancelRequest> {
        TEvCancelRequest(const TString& poolId, const TString& sessionId)
            : PoolId(poolId)
            , SessionId(sessionId)
        {}

        const TString PoolId;
        const TString SessionId;
    };

    struct TEvUpdatePoolsLeases : public NActors::TEventLocal<TEvUpdatePoolsLeases, EvUpdatePoolsLeases> {
    };

    struct TEvRefreshPoolState : public NActors::TEventPB<TEvRefreshPoolState, NKikimrKqp::TEvRefreshPoolState, EvRefreshPoolState> {
    };


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
        TEvRefreshPoolStateResponse(Ydb::StatusIds::StatusCode status, const TString& poolId, const TPoolStateDescription& poolState, NYql::TIssues issues)
            : Status(status)
            , PoolId(poolId)
            , PoolState(poolState)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString PoolId;
        const TPoolStateDescription PoolState;
        const NYql::TIssues Issues;
    };

    struct TEvDelayRequestResponse : public NActors::TEventLocal<TEvDelayRequestResponse, EvDelayRequestResponse> {
        TEvDelayRequestResponse(Ydb::StatusIds::StatusCode status, const TString& poolId, const TString& sessionId, NYql::TIssues issues)
            : Status(status)
            , PoolId(poolId)
            , SessionId(sessionId)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString PoolId;
        const TString SessionId;
        const NYql::TIssues Issues;
    };

    struct TEvStartRequestResponse : public NActors::TEventLocal<TEvStartRequestResponse, EvStartRequestResponse> {
        TEvStartRequestResponse(Ydb::StatusIds::StatusCode status, const TString& poolId, ui32 nodeId, const TString& sessionId, NYql::TIssues issues)
            : Status(status)
            , PoolId(poolId)
            , NodeId(nodeId)
            , SessionId(sessionId)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString PoolId;
        const ui32 NodeId;
        const TString SessionId;
        const NYql::TIssues Issues;
    };

    struct TEvCleanupRequestsResponse : public NActors::TEventLocal<TEvCleanupRequestsResponse, EvCleanupRequestsResponse> {
        TEvCleanupRequestsResponse(Ydb::StatusIds::StatusCode status, const TString& poolId, const std::vector<TString>& sesssionIds, NYql::TIssues issues)
            : Status(status)
            , PoolId(poolId)
            , SesssionIds(sesssionIds)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const TString PoolId;
        const std::vector<TString> SesssionIds;
        const NYql::TIssues Issues;
    };
};

}  // NKikimr::NKqp::NWorkload
