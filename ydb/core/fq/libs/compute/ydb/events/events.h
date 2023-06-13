#pragma once

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_query/query.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvExecuteScriptRequest = EvBegin,
        EvExecuteScriptResponse,
        EvGetOperationRequest,
        EvGetOperationResponse,
        EvFetchScriptResultRequest,
        EvFetchScriptResultResponse,
        EvCancelOperationRequest,
        EvCancelOperationResponse,
        EvForgetOperationRequest,
        EvForgetOperationResponse,

        EvExecuterResponse,
        EvStatusTrackerResponse,
        EvResultWriterResponse,
        EvResourcesCleanerResponse,
        EvFinalizerResponse,
        EvStopperResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvExecuteScriptRequest : public NActors::TEventLocal<TEvExecuteScriptRequest, EvExecuteScriptRequest> {
        TEvExecuteScriptRequest(TString sql, TString idempotencyKey)
            : Sql(std::move(sql))
            , IdempotencyKey(std::move(idempotencyKey))
        {}

        TString Sql;
        TString IdempotencyKey;
    };

    struct TEvExecuteScriptResponse : public NActors::TEventLocal<TEvExecuteScriptResponse, EvExecuteScriptResponse> {
        TEvExecuteScriptResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        TEvExecuteScriptResponse(NYdb::TOperation::TOperationId operationId, const TString& executionId)
            : OperationId(operationId)
            , ExecutionId(executionId)
            , Status(NYdb::EStatus::SUCCESS)
        {}

        NYdb::TOperation::TOperationId OperationId;
        TString ExecutionId;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvGetOperationRequest : public NActors::TEventLocal<TEvGetOperationRequest, EvGetOperationRequest> {
        explicit TEvGetOperationRequest(const NYdb::TOperation::TOperationId& operationId)
            : OperationId(operationId)
        {}

        NYdb::TOperation::TOperationId OperationId;
    };

    struct TEvGetOperationResponse : public NActors::TEventLocal<TEvGetOperationResponse, EvGetOperationResponse> {
        TEvGetOperationResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        TEvGetOperationResponse(NYdb::NQuery::EExecStatus execStatus, NYql::TIssues issues)
            : ExecStatus(execStatus)
            , Issues(issues)
            , Status(NYdb::EStatus::SUCCESS)
        {}

        NYdb::NQuery::EExecStatus ExecStatus = NYdb::NQuery::EExecStatus::Unspecified;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvFetchScriptResultRequest : public NActors::TEventLocal<TEvFetchScriptResultRequest, EvFetchScriptResultRequest> {
        TEvFetchScriptResultRequest(int64_t rowOffset, TString executionId)
            : RowOffset(rowOffset)
            , ExecutionId(std::move(executionId))
        {}

        int64_t RowOffset = 0;
        TString ExecutionId;
    };

    struct TEvFetchScriptResultResponse : public NActors::TEventLocal<TEvFetchScriptResultResponse, EvFetchScriptResultResponse> {
        TEvFetchScriptResultResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        explicit TEvFetchScriptResultResponse(NYdb::TResultSet resultSet)
            : ResultSet(std::move(resultSet))
            , Status(NYdb::EStatus::SUCCESS)
        {}

        TMaybe<NYdb::TResultSet> ResultSet;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvCancelOperationRequest : public NActors::TEventLocal<TEvCancelOperationRequest, EvCancelOperationRequest> {
        explicit TEvCancelOperationRequest(const NYdb::TOperation::TOperationId& operationId)
            : OperationId(operationId)
        {}

        NYdb::TOperation::TOperationId OperationId;
    };

    struct TEvCancelOperationResponse : public NActors::TEventLocal<TEvCancelOperationResponse, EvCancelOperationResponse> {
        TEvCancelOperationResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvForgetOperationRequest : public NActors::TEventLocal<TEvForgetOperationRequest, EvForgetOperationRequest> {
        explicit TEvForgetOperationRequest(const NYdb::TOperation::TOperationId& operationId)
            : OperationId(operationId)
        {}

        NYdb::TOperation::TOperationId OperationId;
    };

    struct TEvForgetOperationResponse : public NActors::TEventLocal<TEvForgetOperationResponse, EvForgetOperationResponse> {
        TEvForgetOperationResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvExecuterResponse : public NActors::TEventLocal<TEvExecuterResponse, EvExecuterResponse> {
        TEvExecuterResponse(NYdb::TOperation::TOperationId operationId, const TString& executionId)
            : OperationId(operationId)
            , ExecutionId(executionId)
            , Success(true)
        {}

        explicit TEvExecuterResponse(const NYql::TIssues& issues)
            : Success(false)
            , Issues(issues)
        {}

        NYdb::TOperation::TOperationId OperationId;
        TString ExecutionId;
        bool Success = true;
        NYql::TIssues Issues;
    };

    struct TEvStatusTrackerResponse : public NActors::TEventLocal<TEvStatusTrackerResponse, EvStatusTrackerResponse> {
        TEvStatusTrackerResponse(NYql::TIssues issues, NYdb::EStatus status, NYdb::NQuery::EExecStatus execStatus)
            : Issues(issues)
            , Status(status)
            , ExecStatus(execStatus)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
        NYdb::NQuery::EExecStatus ExecStatus;
    };

    struct TEvResultWriterResponse : public NActors::TEventLocal<TEvResultWriterResponse, EvResultWriterResponse> {
        TEvResultWriterResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvResourcesCleanerResponse : public NActors::TEventLocal<TEvResourcesCleanerResponse, EvResourcesCleanerResponse> {
        TEvResourcesCleanerResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvFinalizerResponse : public NActors::TEventLocal<TEvFinalizerResponse, EvFinalizerResponse> {
        TEvFinalizerResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvStopperResponse : public NActors::TEventLocal<TEvStopperResponse, EvStopperResponse> {
        TEvStopperResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(issues)
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };
};

}
