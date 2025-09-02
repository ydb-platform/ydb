#include "events.h"

#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NYql::NDqs {
    TEvDqTask::TEvDqTask(NDqProto::TDqTask task) {
        *Record.MutableTask() = std::move(task);
    }

    TEvDqFailure::TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode) {
        Record.SetStatusCode(statusCode);
    }

    TEvDqFailure::TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues) {
        IssuesToMessage(issues, Record.MutableIssues());
        Record.SetStatusCode(statusCode);
    }

    TEvDqFailure::TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssue& issue)
        : TEvDqFailure(statusCode, TIssues({issue}))
    {
    }

    TEvDqFailure::TEvDqFailure(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& error)
        : TEvDqFailure(statusCode, TIssue{error}) {
    }

    TEvDqStats::TEvDqStats(const TIssues& issues) {
        IssuesToMessage(issues, Record.mutable_issues());
    }

    TEvQueryResponse::TEvQueryResponse(NDqProto::TQueryResponse&& queryResult) {
        Record = std::move(queryResult);
    }

    TEvGraphRequest::TEvGraphRequest(const Yql::DqsProto::ExecuteGraphRequest& request, NActors::TActorId controlId, NActors::TActorId resultId)
    {
        *Record.MutableRequest() = request;
        NActors::ActorIdToProto(controlId, Record.MutableControlId());
        NActors::ActorIdToProto(resultId, Record.MutableResultId());
    }

    TEvReadyState::TEvReadyState(NActors::TActorId sourceId, TString type, NYql::NDqProto::EDqStatsMode statsMode) {
        NActors::ActorIdToProto(sourceId, Record.MutableSourceId());
        *Record.MutableResultType() = std::move(type);
        Record.SetStatsMode(statsMode);
    }

    TEvReadyState::TEvReadyState(NDqProto::TReadyState&& proto) {
        Record = std::move(proto);
    }

    TEvGraphExecutionEvent::TEvGraphExecutionEvent(NDqProto::TGraphExecutionEvent& evt) {
        Record = evt;
    }

    TEvPullDataRequest::TEvPullDataRequest(ui32 rowThreshold) {
        Record.SetRowThreshold(rowThreshold);
    }

    TEvPullDataResponse::TEvPullDataResponse(NYql::NDqProto::TPullResponse& data) {
        Record.Swap(&data);
    }

    TEvFullResultWriterStatusResponse::TEvFullResultWriterStatusResponse(NDqProto::TFullResultWriterStatusResponse& data) {
        Record.CopyFrom(data);
    }

    TEvFullResultWriterWriteRequest::TEvFullResultWriterWriteRequest(NDqProto::TFullResultWriterWriteRequest&& data) {
        Record.Swap(&data);
    }

    TEvFullResultWriterAck::TEvFullResultWriterAck(NDqProto::TFullResultWriterAck& data) {
        Record.CopyFrom(data);
    }

    TEvMessageProcessed::TEvMessageProcessed(const TString& messageId) : MessageId(messageId) {
    }
}
