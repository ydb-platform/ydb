#pragma once

#include "query.h"

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/shutdown/events.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_load.h>
#include <contrib/libs/protobuf/src/google/protobuf/map.h>

namespace NKikimr::NKqp {

struct TEvKqp {
    using TEvQueryRequestRemote = NPrivateEvents::TEvQueryRequestRemote;

    using TEvQueryRequest = NPrivateEvents::TEvQueryRequest;

    struct TEvCloseSessionRequest : public TEventPB<TEvCloseSessionRequest,
        NKikimrKqp::TEvCloseSessionRequest, TKqpEvents::EvCloseSessionRequest> {};

    struct TEvCreateSessionRequest : public TEventPB<TEvCreateSessionRequest,
        NKikimrKqp::TEvCreateSessionRequest, TKqpEvents::EvCreateSessionRequest> {};

    struct TEvPingSessionRequest : public TEventPB<TEvPingSessionRequest,
        NKikimrKqp::TEvPingSessionRequest, TKqpEvents::EvPingSessionRequest> {};

    struct TEvCancelQueryRequest : public TEventPB<TEvCancelQueryRequest,
        NKikimrKqp::TEvCancelQueryRequest, TKqpEvents::EvCancelQueryRequest> {};


    using TEvCompileRequest = NPrivateEvents::TEvCompileRequest;
    using TEvRecompileRequest = NPrivateEvents::TEvRecompileRequest;
    using TEvCompileResponse = NPrivateEvents::TEvCompileResponse;
    using TEvParseResponse = NPrivateEvents::TEvParseResponse;
    using TEvSplitResponse = NPrivateEvents::TEvSplitResponse;
    using TEvCompileInvalidateRequest = NPrivateEvents::TEvCompileInvalidateRequest;

    using TEvInitiateSessionShutdown = NKikimr::NKqp::NPrivateEvents::TEvInitiateSessionShutdown;
    using TEvContinueShutdown = NKikimr::NKqp::NPrivateEvents::TEvContinueShutdown;

    using TEvDataQueryStreamPart = NPrivateEvents::TEvDataQueryStreamPart;

    struct TEvDataQueryStreamPartAck : public TEventLocal<TEvDataQueryStreamPartAck, TKqpEvents::EvDataQueryStreamPartAck> {};

    template <typename TProto>
    using TProtoArenaHolder = NPrivateEvents::TProtoArenaHolder<TProto>;

    using TEvQueryResponse = NPrivateEvents::TEvQueryResponse;

    struct TEvListSessionsRequest: public TEventPB<TEvListSessionsRequest, NKikimrKqp::TEvListSessionsRequest,
        TKqpEvents::EvListSessionsRequest>
    {};

    struct TEvListSessionsResponse: public TEventPB<TEvListSessionsResponse, NKikimrKqp::TEvListSessionsResponse,
        TKqpEvents::EvListSessionsResponse>
    {};

    struct TEvListProxyNodesRequest : public TEventLocal<TEvListProxyNodesRequest, TKqpEvents::EvListProxyNodesRequest>
    {};

    struct TEvListProxyNodesResponse : public TEventLocal<TEvListProxyNodesResponse, TKqpEvents::EvListProxyNodesResponse>
    {
        std::vector<ui32> ProxyNodes;
    };

    struct TEvCreateSessionResponse : public TEventPB<TEvCreateSessionResponse,
        NKikimrKqp::TEvCreateSessionResponse, TKqpEvents::EvCreateSessionResponse> {};

    struct TEvContinueProcess : public TEventLocal<TEvContinueProcess, TKqpEvents::EvContinueProcess> {
        TEvContinueProcess(ui32 queryId, bool finished)
            : QueryId(queryId)
            , Finished(finished) {}

        ui32 QueryId;
        bool Finished;
    };

    using TEvQueryTimeout = NPrivateEvents::TEvQueryTimeout;

    struct TEvIdleTimeout : public TEventLocal<TEvIdleTimeout, TKqpEvents::EvIdleTimeout> {
        TEvIdleTimeout(ui32 timerId)
            : TimerId(timerId) {}

        ui32 TimerId;
    };

    struct TEvCloseSessionResponse : public TEventPB<TEvCloseSessionResponse,
        NKikimrKqp::TEvCloseSessionResponse, TKqpEvents::EvCloseSessionResponse> {};

    struct TEvPingSessionResponse : public TEventPB<TEvPingSessionResponse,
        NKikimrKqp::TEvPingSessionResponse, TKqpEvents::EvPingSessionResponse> {};

    struct TEvCancelQueryResponse : public TEventPB<TEvCancelQueryResponse,
        NKikimrKqp::TEvCancelQueryResponse, TKqpEvents::EvCancelQueryResponse> {};

    struct TEvKqpProxyPublishRequest :
        public TEventLocal<TEvKqpProxyPublishRequest, TKqpEvents::EvKqpProxyPublishRequest> {};

    using TEvInitiateShutdownRequest = NPrivateEvents::TEvInitiateShutdownRequest;

    struct TEvScriptRequest : public TEventLocal<TEvScriptRequest, TKqpEvents::EvScriptRequest> {
        TEvScriptRequest() = default;

        const TString& GetDatabase() const {
            return Record.GetRequest().GetDatabase();
        }

        const TString& GetDatabaseId() const {
            return Record.GetRequest().GetDatabaseId();
        }

        void SetDatabaseId(const TString& databaseId) {
            Record.MutableRequest()->SetDatabaseId(databaseId);
        }

        mutable NKikimrKqp::TEvQueryRequest Record;
        TDuration ForgetAfter;
        TDuration ResultsTtl;
    };

    struct TEvScriptResponse : public TEventLocal<TEvScriptResponse, TKqpEvents::EvScriptResponse> {
        TEvScriptResponse(TString operationId, TString executionId, Ydb::Query::ExecStatus execStatus, Ydb::Query::ExecMode execMode)
            : Status(Ydb::StatusIds::SUCCESS)
            , OperationId(std::move(operationId))
            , ExecutionId(std::move(executionId))
            , ExecStatus(execStatus)
            , ExecMode(execMode)
        {}

        TEvScriptResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
            , ExecStatus(Ydb::Query::EXEC_STATUS_FAILED)
            , ExecMode(Ydb::Query::EXEC_MODE_UNSPECIFIED)
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        const TString OperationId;
        const TString ExecutionId;
        const Ydb::Query::ExecStatus ExecStatus;
        const Ydb::Query::ExecMode ExecMode;
    };

    using TEvAbortExecution = NYql::NDq::TEvDq::TEvAbortExecution;

    struct TEvCancelScriptExecutionRequest : public TEventPB<TEvCancelScriptExecutionRequest, NKikimrKqp::TEvCancelScriptExecutionRequest, TKqpEvents::EvCancelScriptExecutionRequest> {
    };

    struct TEvCancelScriptExecutionResponse : public TEventPB<TEvCancelScriptExecutionResponse, NKikimrKqp::TEvCancelScriptExecutionResponse, TKqpEvents::EvCancelScriptExecutionResponse> {
        TEvCancelScriptExecutionResponse() = default;

        explicit TEvCancelScriptExecutionResponse(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
            Record.SetStatus(status);
            NYql::IssuesToMessage(issues, Record.MutableIssues());
        }

        TEvCancelScriptExecutionResponse(Ydb::StatusIds::StatusCode status, const TString& message)
            : TEvCancelScriptExecutionResponse(status, TextToIssues(message))
        {}

    private:
        static NYql::TIssues TextToIssues(const TString& message) {
            NYql::TIssues issues;
            issues.AddIssue(message);
            return issues;
        }
    };

    struct TEvUpdateDatabaseInfo : public TEventLocal<TEvUpdateDatabaseInfo, TKqpEvents::EvUpdateDatabaseInfo> {
        TEvUpdateDatabaseInfo(const TString& database, Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Database(database)
            , Issues(std::move(issues))
        {}

        TEvUpdateDatabaseInfo(const TString& database, const TString& databaseId, bool serverless)
            : Status(Ydb::StatusIds::SUCCESS)
            , Database(database)
            , DatabaseId(databaseId)
            , Serverless(serverless)
            , Issues({})
        {}

        Ydb::StatusIds::StatusCode Status;
        TString Database;
        TString DatabaseId;
        bool Serverless = false;
        NYql::TIssues Issues;
    };

    struct TEvDelayedRequestError : public TEventLocal<TEvDelayedRequestError, TKqpEvents::EvDelayedRequestError> {
        TEvDelayedRequestError(THolder<IEventHandle> requestEvent, Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : RequestEvent(std::move(requestEvent))
            , Status(status)
            , Issues(std::move(issues))
        {}

        THolder<IEventHandle> RequestEvent;
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
    };
};

} // namespace NKikimr::NKqp
