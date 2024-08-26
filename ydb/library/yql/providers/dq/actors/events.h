#pragma once

#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>

namespace NYql::NDqs {
    using TDqExecuterEvents = NDq::TBaseDqExecuterEvents<NActors::TEvents::EEventSpace::ES_USERSPACE>;

    struct TEvDqTask : NActors::TEventPB<TEvDqTask, NDqProto::TDqTaskRequest, TDqExecuterEvents::ES_DQ_TASK> {
        TEvDqTask() = default;
        explicit TEvDqTask(NDqProto::TDqTask task);
    };

    struct TEvDqFailure : NActors::TEventPB<TEvDqFailure, NDqProto::TDqFailure, TDqExecuterEvents::ES_DQ_FAILURE> {
        TEvDqFailure() = default;
        explicit TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode);
        TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues);
        TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssue& issue);
        TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& error);
    };

    struct TEvDqStats : NActors::TEventPB<TEvDqStats, NDqProto::TDqStats, TDqExecuterEvents::ES_STATS> {
        TEvDqStats() = default;
        TEvDqStats(const TIssues& issues);
    };

    struct TEvQueryResponse
        : NActors::TEventPB<TEvQueryResponse, NDqProto::TQueryResponse, TDqExecuterEvents::ES_RESULT_SET> {
        TEvQueryResponse() = default;
        explicit TEvQueryResponse(NDqProto::TQueryResponse&& queryResult);
    };

    struct TEvGraphRequest : NActors::TEventPB<TEvGraphRequest, NDqProto::TGraphRequest, TDqExecuterEvents::ES_GRAPH> {
        TEvGraphRequest() = default;
        TEvGraphRequest(const Yql::DqsProto::ExecuteGraphRequest& request, NActors::TActorId controlId, NActors::TActorId resultId);
    };

    struct TEvReadyState : NActors::TEventPB<TEvReadyState, NDqProto::TReadyState, TDqExecuterEvents::ES_READY_TO_PULL> {
        TEvReadyState() = default;
        TEvReadyState(NActors::TActorId sourceId, TString type, NYql::NDqProto::EDqStatsMode statsMode);
        explicit TEvReadyState(NDqProto::TReadyState&& proto);
    };

    struct TEvPullResult : NActors::TEventBase<TEvPullResult, TDqExecuterEvents::ES_PULL_RESULT> {
        DEFINE_SIMPLE_NONLOCAL_EVENT(TEvPullResult, "");
    };

    struct TEvGraphExecutionEvent
            : NActors::TEventPB<TEvGraphExecutionEvent, NYql::NDqProto::TGraphExecutionEvent, TDqExecuterEvents::ES_GRAPH_EXECUTION_EVENT> {
        TEvGraphExecutionEvent() = default;
        explicit TEvGraphExecutionEvent(NDqProto::TGraphExecutionEvent& evt);
    };

    using TDqDataEvents = NDq::TBaseDqDataEvents<NActors::TEvents::EEventSpace::ES_USERSPACE>;

    struct TEvPullDataRequest
        : NActors::TEventPB<TEvPullDataRequest, NYql::NDqProto::TPullRequest, TDqDataEvents::ES_PULL_REQUEST> {
        TEvPullDataRequest() = default;
        explicit TEvPullDataRequest(ui32 rowThreshold);
    };

    struct TEvPullDataResponse
        : NActors::TEventPB<TEvPullDataResponse, NYql::NDqProto::TPullResponse, TDqDataEvents::ES_PULL_RESPONSE> {
        TEvPullDataResponse() = default;
        explicit TEvPullDataResponse(NDqProto::TPullResponse& data);
    };

    struct TEvPingRequest
        : NActors::TEventPB<TEvPingRequest, NYql::NDqProto::TPingRequest, TDqDataEvents::ES_PING_REQUEST> {
        TEvPingRequest() = default;
    };

    struct TEvPingResponse
        : NActors::TEventPB<TEvPingResponse, NYql::NDqProto::TPingResponse, TDqDataEvents::ES_PING_RESPONSE> {
        TEvPingResponse() = default;
    };

    // legacy
    struct TEvFullResultWriterStatusRequest
        : NActors::TEventPB<TEvFullResultWriterStatusRequest, NYql::NDqProto::TFullResultWriterStatusRequest,
        TDqDataEvents::ES_FULL_RESULT_WRITER_STATUS_REQUEST> {
        TEvFullResultWriterStatusRequest() = default;
    };

    // legacy
    struct TEvFullResultWriterStatusResponse
        : NActors::TEventPB<TEvFullResultWriterStatusResponse, NYql::NDqProto::TFullResultWriterStatusResponse,
        TDqDataEvents::ES_FULL_RESULT_WRITER_STATUS_RESPONSE> {
        TEvFullResultWriterStatusResponse() = default;
        explicit TEvFullResultWriterStatusResponse(NDqProto::TFullResultWriterStatusResponse& data);
    };

    struct TEvGraphFinished : NActors::TEventBase<TEvGraphFinished, TDqExecuterEvents::ES_GRAPH_FINISHED> {
        DEFINE_SIMPLE_NONLOCAL_EVENT(TEvGraphFinished, "");
    };

    struct TEvFullResultWriterWriteRequest
        : NActors::TEventPB<TEvFullResultWriterWriteRequest, NYql::NDqProto::TFullResultWriterWriteRequest,
        TDqDataEvents::ES_FULL_RESULT_WRITER_WRITE_REQUEST> {
        TEvFullResultWriterWriteRequest() = default;
        explicit TEvFullResultWriterWriteRequest(NDqProto::TFullResultWriterWriteRequest&& data);
    };

    struct TEvFullResultWriterAck
        : NActors::TEventPB<TEvFullResultWriterAck, NYql::NDqProto::TFullResultWriterAck,
        TDqDataEvents::ES_FULL_RESULT_WRITER_ACK> {
        TEvFullResultWriterAck() = default;
        explicit TEvFullResultWriterAck(NDqProto::TFullResultWriterAck& data);
    };

    struct TEvMessageProcessed : NActors::TEventLocal<TEvMessageProcessed, TDqDataEvents::ES_MESSAGE_PROCESSED> {
        explicit TEvMessageProcessed(const TString& messageId);

        const TString MessageId;
    };
}
