#include "events.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NYql::NDqs {
    TEvDqTask::TEvDqTask(NDqProto::TDqTask task) { 
        *Record.MutableTask() = std::move(task); 
    } 
 
    TEvDqFailure::TEvDqFailure(const TIssue& issue, bool retriable, bool needFallback) {
        IssuesToMessage({issue}, Record.MutableIssues());
        Record.SetRetriable(retriable); 
        Record.SetNeedFallback(needFallback); 
    } 
 
    TEvQueryResponse::TEvQueryResponse(NDqProto::TQueryResponse&& queryResult) {
        Record = std::move(queryResult);
    }

    TEvGraphRequest::TEvGraphRequest(const Yql::DqsProto::ExecuteGraphRequest& request, NActors::TActorId controlId, NActors::TActorId resultId, NActors::TActorId checkPointCoordinatorId)
    { 
        *Record.MutableRequest() = request; 
        NActors::ActorIdToProto(controlId, Record.MutableControlId());
        NActors::ActorIdToProto(resultId, Record.MutableResultId());
        if (checkPointCoordinatorId) {
            NActors::ActorIdToProto(checkPointCoordinatorId, Record.MutableCheckPointCoordinatorId());
        }
    } 
 
    TEvReadyState::TEvReadyState(NActors::TActorId sourceId, TString type) {
        NActors::ActorIdToProto(sourceId, Record.MutableSourceId());
        *Record.MutableResultType() = std::move(type);
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
}
