#pragma once
#include "event_ids.h"

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/yq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/yq/libs/protos/fq_private.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yq/scope.h>

#include <library/cpp/actors/core/events.h>

#include <util/digest/multi.h>

namespace NYq {

using NYdb::NYq::TScope;

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
        Fq::Private::PingTaskRequest Record;
        TString User;
    };

    struct TEvPingTaskResponse : NActors::TEventLocal<TEvPingTaskResponse, TEventIds::EvPingTaskResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Fq::Private::PingTaskResult> Record;
    };

    struct TEvGetTaskRequest : NActors::TEventLocal<TEvGetTaskRequest, TEventIds::EvGetTaskRequest>, TEvAnalyticsBase {
        Fq::Private::GetTaskRequest Record;
        TString User;
    };

    struct TEvGetTaskResponse : NActors::TEventLocal<TEvGetTaskResponse, TEventIds::EvGetTaskResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Fq::Private::GetTaskResult> Record;
    };

    struct TEvWriteTaskResultRequest : NActors::TEventLocal<TEvWriteTaskResultRequest, TEventIds::EvWriteTaskResultRequest>, TEvAnalyticsBase {
        Fq::Private::WriteTaskResultRequest Record;
        TString User;
    };

    struct TEvWriteTaskResultResponse : NActors::TEventLocal<TEvWriteTaskResultResponse, TEventIds::EvWriteTaskResultResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Fq::Private::WriteTaskResultResult> Record;
    };

    struct TEvNodesHealthCheckRequest : NActors::TEventLocal<TEvNodesHealthCheckRequest, TEventIds::EvNodesHealthCheckRequest> {
        Fq::Private::NodesHealthCheckRequest Record;
        TString User;
    };

    struct TEvNodesHealthCheckResponse : NActors::TEventLocal<TEvNodesHealthCheckResponse, TEventIds::EvNodesHealthCheckResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Fq::Private::NodesHealthCheckResult> Record;
    };

    struct TEvCreateRateLimiterResourceRequest : NActors::TEventLocal<TEvCreateRateLimiterResourceRequest, TEventIds::EvCreateRateLimiterResourceRequest>, TEvAnalyticsBase {
        Fq::Private::CreateRateLimiterResourceRequest Record;
        TString User;
    };

    struct TEvCreateRateLimiterResourceResponse : NActors::TEventLocal<TEvCreateRateLimiterResourceResponse, TEventIds::EvCreateRateLimiterResourceResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Fq::Private::CreateRateLimiterResourceResult> Record;
    };

    struct TEvDeleteRateLimiterResourceRequest : NActors::TEventLocal<TEvDeleteRateLimiterResourceRequest, TEventIds::EvDeleteRateLimiterResourceRequest>, TEvAnalyticsBase {
        Fq::Private::DeleteRateLimiterResourceRequest Record;
        TString User;
    };

    struct TEvDeleteRateLimiterResourceResponse : NActors::TEventLocal<TEvDeleteRateLimiterResourceResponse, TEventIds::EvDeleteRateLimiterResourceResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TMaybe<Fq::Private::DeleteRateLimiterResourceResult> Record;
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
        NYql::TDbResolverResponse DbResolverResponse;
        explicit TEvEndpointResponse(NYql::TDbResolverResponse&& response) noexcept : DbResolverResponse(std::move(response)) {}
    };


    struct TEvEndpointRequest : NActors::TEventLocal<TEvEndpointRequest, TEventIds::EvEndpointRequest> {
        THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDatabaseAuth> DatabaseIds; // DbId, DatabaseType => database auth
        TString YdbMvpEndpoint;
        TString MdbGateway;
        TString TraceId;
        bool MdbTransformHost;

        TEvEndpointRequest(
            const THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDatabaseAuth>& databaseIds,
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
        NYql::TIssues TransientIssues;
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
        explicit TEvForwardPingRequest(const Fq::Private::PingTaskRequest& request, bool final = false)
            : Request(request)
            , Final(final)
        { }

        Fq::Private::PingTaskRequest Request;
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
        bool IsEvaluation = false;
        NThreading::TPromise<NYql::IDqGateway::TResult> Result;
    };

    struct TEvRaiseTransientIssues : public NActors::TEventLocal<TEvRaiseTransientIssues, TEventIds::EvRaiseTransientIssues> {
        TEvRaiseTransientIssues() = default;

        explicit TEvRaiseTransientIssues(NYql::TIssues issues)
            : TransientIssues(std::move(issues))
        {
        }

        NYql::TIssues TransientIssues;
    };

    struct TEvSchemaCreated : public NActors::TEventLocal<TEvSchemaCreated, TEventIds::EvSchemaCreated> {
        explicit TEvSchemaCreated(NYdb::TStatus result)
            : Result(std::move(result))
        {
        }

        NYdb::TStatus Result;
    };

    struct TEvSchemaDeleted : public NActors::TEventLocal<TEvSchemaDeleted, TEventIds::EvSchemaDeleted> {
        explicit TEvSchemaDeleted(NYdb::TStatus result)
            : Result(std::move(result))
        {
        }

        NYdb::TStatus Result;
    };

    struct TEvSchemaUpdated : public NActors::TEventLocal<TEvSchemaUpdated, TEventIds::EvSchemaUpdated> {
        explicit TEvSchemaUpdated(NYdb::TStatus result)
            : Result(std::move(result))
        {
        }

        NYdb::TStatus Result;
    };

    struct TEvCallback : public NActors::TEventLocal<TEvCallback, TEventIds::EvCallback> {
        explicit TEvCallback(std::function<void()> callback)
            : Callback(callback)
        {
        }

        std::function<void()> Callback;
    };
};

} // namespace NYq

template<>
struct THash<NYql::TDatabaseAuth> {
    inline ui64 operator()(const NYql::TDatabaseAuth& x) const noexcept {
        return MultiHash(x.StructuredToken, x.AddBearerToToken);
    }
};
