#pragma once
#include "event_ids.h"

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/yq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/public/api/protos/draft/yq_private.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yq/scope.h>

#include <library/cpp/actors/core/events.h> 
 
#include <util/digest/multi.h>

namespace NYq {

using NYdb::NYq::TScope; 
 
enum class DatabaseType {
    Ydb,
    ClickHouse,
    DataStreams,
    ObjectStorage
};
 
struct TQueryResult {
    TVector<Ydb::ResultSet> Sets;
    TInstant ExpirationDeadline;
    TMaybe<TString> StatsYson;
};

struct TEvents {
    // Events.
    struct TEvAnalyticsBase { 
        TString AuthToken;
        TString UserId;
        bool HasPerm = false;
    };

    struct TEvPingTaskRequest : NActors::TEventLocal<TEvPingTaskRequest, TEventIds::EvPingTaskRequest>, TEvAnalyticsBase {
        Yq::Private::PingTaskRequest Record;
    };

    struct TEvPingTaskResponse : NActors::TEventLocal<TEvPingTaskResponse, TEventIds::EvPingTaskResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Yq::Private::PingTaskResult> Record;
    };

    struct TEvGetTaskRequest : NActors::TEventLocal<TEvGetTaskRequest, TEventIds::EvGetTaskRequest>, TEvAnalyticsBase {
        Yq::Private::GetTaskRequest Record;
    };

    struct TEvGetTaskResponse : NActors::TEventLocal<TEvGetTaskResponse, TEventIds::EvGetTaskResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Yq::Private::GetTaskResult> Record;
    };

    struct TEvWriteTaskResultRequest : NActors::TEventLocal<TEvWriteTaskResultRequest, TEventIds::EvWriteTaskResultRequest>, TEvAnalyticsBase {
        Yq::Private::WriteTaskResultRequest Record;
    };

    struct TEvWriteTaskResultResponse : NActors::TEventLocal<TEvWriteTaskResultResponse, TEventIds::EvWriteTaskResultResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Yq::Private::WriteTaskResultResult> Record;
    };

    struct TEvNodesHealthCheckRequest : NActors::TEventLocal<TEvNodesHealthCheckRequest, TEventIds::EvNodesHealthCheckRequest>{
        Yq::Private::NodesHealthCheckRequest Record;
    };

    struct TEvNodesHealthCheckResponse : NActors::TEventLocal<TEvNodesHealthCheckResponse, TEventIds::EvNodesHealthCheckResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Yq::Private::NodesHealthCheckResult> Record;
    };

    struct TEvAsyncContinue : NActors::TEventLocal<TEvAsyncContinue, TEventIds::EvAsyncContinue> {
        const NYql::TProgram::TFutureStatus Future;

        explicit TEvAsyncContinue(const NYql::TProgram::TFutureStatus& future)
            : Future(future)
        {}
    };

    struct TEvDbRequest : NActors::TEventLocal<TEvDbRequest, TEventIds::EvDbRequest> {
        TString Sql;
        NYdb::TParams Params;
        bool Idempotent;

        TEvDbRequest(const TString& sql, NYdb::TParams&& params, bool idempotent = true)
            : Sql(sql)
            , Params(std::move(params))
            , Idempotent(idempotent)
        {}
    };

    struct TEvDbResponse : NActors::TEventLocal<TEvDbResponse, TEventIds::EvDbResponse> {
        NYdb::TStatus Status;
        TVector<NYdb::TResultSet> ResultSets;

        TEvDbResponse(NYdb::TStatus status, const TVector<NYdb::TResultSet>& resultSets)
            : Status(status)
            , ResultSets(resultSets)
        {}
    };
 
    struct TEvDbFunctionRequest : NActors::TEventLocal<TEvDbFunctionRequest, TEventIds::EvDbFunctionRequest> {
        using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
        TFunction Handler;

        explicit TEvDbFunctionRequest(const TFunction& handler)
            : Handler(handler)
        {}
    };

    struct TEvDbFunctionResponse : NActors::TEventLocal<TEvDbFunctionResponse, TEventIds::EvDbFunctionResponse> {
        NYdb::TStatus Status;

        explicit TEvDbFunctionResponse(NYdb::TStatus status)
            : Status(status)
        {}
    };

    struct TEvEndpointResponse : NActors::TEventLocal<TEvEndpointResponse, TEventIds::EvEndpointResponse> {
        struct TEndpoint {
            TString Endpoint;
            TString Database;
            bool Secure = false;
        };
        THashMap<std::pair<TString, DatabaseType>, TEndpoint> DatabaseId2Endpoint;
        bool Success;

        TEvEndpointResponse(THashMap<std::pair<TString, DatabaseType>, TEndpoint>&& res, bool success)
            : DatabaseId2Endpoint(std::move(res))
            , Success(success)
        { }
    };

    struct TDatabaseAuth {
        TString StructuredToken;
        bool AddBearerToToken = false;

        bool operator==(const TDatabaseAuth& other) const {
            return std::tie(StructuredToken, AddBearerToToken) == std::tie(other.StructuredToken, other.AddBearerToToken);
        }
        bool operator!=(const TDatabaseAuth& other) const {
            return !(*this == other);
        }
    };

    struct TEvEndpointRequest : NActors::TEventLocal<TEvEndpointRequest, TEventIds::EvEndpointRequest> {
        THashMap<std::pair<TString, DatabaseType>, TDatabaseAuth> DatabaseIds; // DbId, DatabaseType => database auth
        TString YdbMvpEndpoint;
        TString MdbGateway; 
        TString TraceId; 
        bool MdbTransformHost;
 
        TEvEndpointRequest( 
            const THashMap<std::pair<TString, DatabaseType>, TDatabaseAuth>& databaseIds,
            const TString& ydbMvpEndpoint, 
            const TString& mdbGateway, 
            const TString& traceId, 
            bool mdbTransformHost)
            : DatabaseIds(databaseIds) 
            , YdbMvpEndpoint(ydbMvpEndpoint)
            , MdbGateway(mdbGateway) 
            , TraceId(traceId) 
            , MdbTransformHost(mdbTransformHost)
        { } 
    }; 
 
    struct TEvDataStreamsReadRulesCreationResult : NActors::TEventLocal<TEvDataStreamsReadRulesCreationResult, TEventIds::EvDataStreamsReadRulesCreationResult> {
        explicit TEvDataStreamsReadRulesCreationResult(NYql::TIssues issues)
            : Issues(std::move(issues))
        {
        }

        NYql::TIssues Issues;
    };

    struct TEvDataStreamsReadRulesDeletionResult : NActors::TEventLocal<TEvDataStreamsReadRulesDeletionResult, TEventIds::EvDataStreamsReadRulesDeletionResult> {
        explicit TEvDataStreamsReadRulesDeletionResult(NYql::TIssues transientIssues)
            : TransientIssues(std::move(transientIssues))
        {
        }

        NYql::TIssues TransientIssues;
    };

    struct TEvQueryActionResult : NActors::TEventLocal<TEvQueryActionResult, TEventIds::EvQueryActionResult> {
        explicit TEvQueryActionResult(YandexQuery::QueryAction action)
            : Action(action)
        {
        }

        YandexQuery::QueryAction Action;
    };
 
    struct TEvForwardPingRequest : NActors::TEventLocal<TEvForwardPingRequest, TEventIds::EvForwardPingRequest> {
        explicit TEvForwardPingRequest(const Yq::Private::PingTaskRequest& request, bool final = false)
            : Request(request) 
            , Final(final)
        { } 
 
        Yq::Private::PingTaskRequest Request;
        bool Final; // Is this the last ping request.
    }; 

    struct TEvForwardPingResponse : NActors::TEventLocal<TEvForwardPingResponse, TEventIds::EvForwardPingResponse> {
        TEvForwardPingResponse(bool success, YandexQuery::QueryAction action)
            : Success(success)
            , Action(action)
        { }

        bool Success;
        YandexQuery::QueryAction Action;
    };

    struct TEvGraphParams : public NActors::TEventLocal<TEvGraphParams, TEventIds::EvGraphParams> {
        explicit TEvGraphParams(const NProto::TGraphParams& params)
            : GraphParams(params)
        { }

        explicit TEvGraphParams(NProto::TGraphParams&& params)
            : GraphParams(std::move(params))
        { }

        NProto::TGraphParams GraphParams;
        NThreading::TPromise<NYql::IDqGateway::TResult> Result;
    };
};

} // namespace NYq

template<>
struct THash<NYq::TEvents::TDatabaseAuth> {
    inline ui64 operator()(const NYq::TEvents::TDatabaseAuth& x) const noexcept {
        return MultiHash(x.StructuredToken, x.AddBearerToToken);
    }
};
