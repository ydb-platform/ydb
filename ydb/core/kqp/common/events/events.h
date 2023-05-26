#pragma once

#include "process_response.h"
#include "query.h"
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/shutdown/events.h>
#include <ydb/public/api/protos/draft/ydb_query.pb.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/event_load.h>
#include <contrib/libs/protobuf/src/google/protobuf/map.h>

namespace NKikimr::NKqp {

struct TEvKqp {
    using TEvQueryRequestRemote = NPrivateEvents::TEvQueryRequestRemote;

    using TEvProcessResponse = NPrivateEvents::TEvProcessResponse;

    using TEvQueryRequest = NPrivateEvents::TEvQueryRequest;

    struct TEvCloseSessionRequest : public TEventPB<TEvCloseSessionRequest,
        NKikimrKqp::TEvCloseSessionRequest, TKqpEvents::EvCloseSessionRequest> {};

    struct TEvCreateSessionRequest : public TEventPB<TEvCreateSessionRequest,
        NKikimrKqp::TEvCreateSessionRequest, TKqpEvents::EvCreateSessionRequest> {};

    struct TEvPingSessionRequest : public TEventPB<TEvPingSessionRequest,
        NKikimrKqp::TEvPingSessionRequest, TKqpEvents::EvPingSessionRequest> {};


    using TEvCompileRequest = NPrivateEvents::TEvCompileRequest;
    using TEvRecompileRequest = NPrivateEvents::TEvRecompileRequest;
    using TEvCompileResponse = NPrivateEvents::TEvCompileResponse;
    using TEvCompileInvalidateRequest = NPrivateEvents::TEvCompileInvalidateRequest;

    using TEvInitiateSessionShutdown = NKikimr::NKqp::NPrivateEvents::TEvInitiateSessionShutdown;
    using TEvContinueShutdown = NKikimr::NKqp::NPrivateEvents::TEvContinueShutdown;

    using TEvDataQueryStreamPart = NPrivateEvents::TEvDataQueryStreamPart;

    struct TEvDataQueryStreamPartAck : public TEventLocal<TEvDataQueryStreamPartAck, TKqpEvents::EvDataQueryStreamPartAck> {};

    template <typename TProto>
    using TProtoArenaHolder = NPrivateEvents::TProtoArenaHolder<TProto>;

    using TEvQueryResponse = NPrivateEvents::TEvQueryResponse;

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

    struct TEvKqpProxyPublishRequest :
        public TEventLocal<TEvKqpProxyPublishRequest, TKqpEvents::EvKqpProxyPublishRequest> {};

    using TEvInitiateShutdownRequest = NPrivateEvents::TEvInitiateShutdownRequest;

    struct TEvScriptRequest : public TEventLocal<TEvScriptRequest, TKqpEvents::EvScriptRequest> {
        TEvScriptRequest() = default;

        mutable NKikimrKqp::TEvQueryRequest Record;
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

    struct TEvFetchScriptResultsRequest : public TEventPB<TEvFetchScriptResultsRequest, NKikimrKqp::TEvFetchScriptResultsRequest, TKqpEvents::EvFetchScriptResultsRequest> {
    };

    struct TEvFetchScriptResultsResponse : public TEventPB<TEvFetchScriptResultsResponse, NKikimrKqp::TEvFetchScriptResultsResponse, TKqpEvents::EvFetchScriptResultsResponse> {
    };
};

} // namespace NKikimr::NKqp
