#pragma once
#include "event_ids.h"

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/mdb_endpoint_generator.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/actors/core/events.h>

#include <util/digest/multi.h>

namespace NFq {

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

    struct TEvEndpointResponse : NActors::TEventLocal<TEvEndpointResponse, TEventIds::EvEndpointResponse> {
        NYql::TDatabaseResolverResponse DbResolverResponse;
        explicit TEvEndpointResponse(NYql::TDatabaseResolverResponse&& response) noexcept : DbResolverResponse(std::move(response)) {}
    };


    struct TEvEndpointRequest : NActors::TEventLocal<TEvEndpointRequest, TEventIds::EvEndpointRequest> {
        const NYql::IDatabaseAsyncResolver::TDatabaseAuthMap DatabaseIds; 
        TString YdbMvpEndpoint;
        TString MdbGateway;
        TString TraceId;
        const NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;

        TEvEndpointRequest(
            const NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseIds,
            const TString& ydbMvpEndpoint,
            const TString& mdbGateway,
            const TString& traceId,
            const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator)
            : DatabaseIds(databaseIds)
            , YdbMvpEndpoint(ydbMvpEndpoint)
            , MdbGateway(mdbGateway)
            , TraceId(traceId)
            , MdbEndpointGenerator(mdbEndpointGenerator)
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
        explicit TEvQueryActionResult(FederatedQuery::QueryAction action)
            : Action(action)
        {
        }

        FederatedQuery::QueryAction Action;
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
        TEvForwardPingResponse(bool success, FederatedQuery::QueryAction action)
            : Success(success)
            , Action(action)
        { }

        bool Success;
        FederatedQuery::QueryAction Action;
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

    struct TEvEffectApplicationResult : public NActors::TEventLocal<TEvEffectApplicationResult, TEventIds::EvEffectApplicationResult> {
        explicit TEvEffectApplicationResult(const NYql::TIssues& issues, bool fataError = false)
            : Issues(issues), FatalError(fataError) {
        }
        NYql::TIssues Issues;
        const bool FatalError;
    };
};

NActors::TActorId MakeYqPrivateProxyId();

} // namespace NFq

template<>
struct THash<NYql::TDatabaseAuth> {
    inline ui64 operator()(const NYql::TDatabaseAuth& x) const noexcept {
        return MultiHash(x.StructuredToken, x.AddBearerToToken);
    }
};
