#pragma once
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <library/cpp/actors/core/event_local.h>

#include <util/generic/maybe.h>

#include <google/protobuf/any.pb.h>

namespace NKikimr::NKqp {

struct TEvGetScriptExecutionOperation : public NActors::TEventLocal<TEvGetScriptExecutionOperation, TKqpScriptExecutionEvents::EvGetScriptExecutionOperation> {
    explicit TEvGetScriptExecutionOperation(const TString& database, const NOperationId::TOperationId& id)
        : Database(database)
        , OperationId(id)
    {
    }

    TString Database;
    NOperationId::TOperationId OperationId;
};

struct TEvGetScriptExecutionOperationResponse : public NActors::TEventLocal<TEvGetScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvGetScriptExecutionOperationResponse> {
    TEvGetScriptExecutionOperationResponse(bool ready, Ydb::StatusIds::StatusCode status, NYql::TIssues issues, TMaybe<google::protobuf::Any> metadata)
        : Ready(ready)
        , Status(status)
        , Issues(std::move(issues))
        , Metadata(std::move(metadata))
    {
    }

    bool Ready;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    TMaybe<google::protobuf::Any> Metadata;
};

struct TEvListScriptExecutionOperations : public NActors::TEventLocal<TEvListScriptExecutionOperations, TKqpScriptExecutionEvents::EvListScriptExecutionOperations> {
    TEvListScriptExecutionOperations(const TString& database, const ui64 pageSize, const TString& pageToken)
        : Database(database)
        , PageSize(pageSize)
        , PageToken(pageToken)
    {}

    TString Database;
    ui64 PageSize;
    TString PageToken;
};

struct TEvListScriptExecutionOperationsResponse : public NActors::TEventLocal<TEvListScriptExecutionOperationsResponse, TKqpScriptExecutionEvents::EvListScriptExecutionOperationsResponse> {
    TEvListScriptExecutionOperationsResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues, const TString& nextPageToken, std::vector<Ydb::Operations::Operation> operations)
        : Status(status)
        , Issues(std::move(issues))
        , NextPageToken(nextPageToken)
        , Operations(std::move(operations))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    TString NextPageToken;
    std::vector<Ydb::Operations::Operation> Operations;
};

struct TEvScriptLeaseUpdateResponse : public NActors::TEventLocal<TEvScriptLeaseUpdateResponse, TKqpScriptExecutionEvents::EvScriptLeaseUpdateResponse> {
    TEvScriptLeaseUpdateResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvCancelScriptExecutionOperation : public NActors::TEventLocal<TEvCancelScriptExecutionOperation, TKqpScriptExecutionEvents::EvCancelScriptExecutionOperation> {
    explicit TEvCancelScriptExecutionOperation(const TString& database, const NOperationId::TOperationId& id)
        : Database(database)
        , OperationId(id)
    {
    }

    TString Database;
    NOperationId::TOperationId OperationId;
};

struct TEvCancelScriptExecutionOperationResponse : public NActors::TEventLocal<TEvCancelScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvCancelScriptExecutionOperationResponse> {
    TEvCancelScriptExecutionOperationResponse() = default;

    TEvCancelScriptExecutionOperationResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvScriptExecutionFinished : public NActors::TEventLocal<TEvScriptExecutionFinished, TKqpScriptExecutionEvents::EvScriptExecutionFinished> {
    TEvScriptExecutionFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

} // namespace NKikimr::NKqp
