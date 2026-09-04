#include "viewer_computation_graph.h"

#include <ydb/library/computation_graph_renderer/computation_graph_renderer.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

namespace NKikimr::NViewer {

void TJsonComputationGraph::Bootstrap() {
    if (NeedToRedirect()) {
        return;
    }
    if (Event->Get()->Request.GetMethod() == HTTP_METHOD_POST) {
        TStringBuf body = Event->Get()->Request.GetPostContent();
        NJson::TJsonValue plan;
        if (!NJson::ReadJsonTree(body, &plan)) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "invalid plan json"));
        }
        const auto graph = NComputationGraphRenderer::BuildGraph(plan);
        if (!graph) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "not a query plan"));
        }
        return ReplyAndPassAway(GetHTTPOK("image/svg+xml", NComputationGraphRenderer::ToSvg(*graph)));
    }
    const TString path = Params.Get("path");
    if (!path) {
        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'path' is required"));
    }
    Json = (Params.Get("format") == "json");

    auto event = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
    auto& request = *event->Record.MutableRequest();
    request.SetDatabase(Database);
    request.SetQuery(
        "DECLARE $path AS Utf8; "
        "SELECT Status, Issues, Plan FROM `.sys/streaming_queries` WHERE Path = $path");
    request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    request.SetKeepSession(false);
    request.SetIsInternalCall(true);
    auto& pathParam = (*request.MutableYdbParameters())["$path"];
    pathParam.mutable_type()->set_type_id(Ydb::Type::UTF8);
    pathParam.mutable_value()->set_text_value(std::string(path));
    if (const TString userToken = GetRequest().GetUserTokenObject()) {
        event->Record.SetUserToken(userToken);
    }
    Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
    Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
}

void TJsonComputationGraph::ReplyAndPassAway() {
    TBase::ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT());
}

void TJsonComputationGraph::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
    const auto ydbStatus = ev->Get()->Record.GetYdbStatus();
    if (ydbStatus != Ydb::StatusIds::SUCCESS) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(ev->Get()->Record.GetResponse().GetQueryIssues(), issues);
        const TString msg = TStringBuilder()
            << Ydb::StatusIds_StatusCode_Name(ydbStatus) << ": " << issues.ToOneLineString();
        if (ydbStatus == Ydb::StatusIds::TIMEOUT) {
            return ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT("text/plain", msg));
        }
        if (ydbStatus == Ydb::StatusIds::UNAUTHORIZED) {
            return ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", msg));
        }
        if (ydbStatus == Ydb::StatusIds::INTERNAL_ERROR) {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", msg));
        }
        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", msg));
    }
    const auto& response = ev->Get()->Record.GetResponse();
    if (response.YdbResultsSize() == 0) {
        return ReplyAndPassAway(GetHTTPNOTFOUND());
    }
    NYdb::TResultSetParser parser(NYdb::TResultSet(response.GetYdbResults(0)));
    if (!parser.TryNextRow()) {
        return ReplyAndPassAway(GetHTTPNOTFOUND());
    }
    const TString status = parser.ColumnParser("Status").GetOptionalUtf8().value_or("");
    const TString issuesText = parser.ColumnParser("Issues").GetOptionalUtf8().value_or("");
    const TString planText = parser.ColumnParser("Plan").GetOptionalUtf8().value_or("");

    NJson::TJsonValue plan(NJson::JSON_MAP);
    if (!planText.empty() && !NJson::ReadJsonTree(planText, &plan)) {
        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "invalid plan json"));
    }

    if (Json) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["status"] = status;
        if (!issuesText.empty()) {
            json["issues"] = issuesText;
        }
        json["plan"] = plan;
        return ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    const auto graph = NComputationGraphRenderer::BuildGraph(plan);
    ReplyAndPassAway(GetHTTPOK("image/svg+xml",
        NComputationGraphRenderer::ToSvg(graph.value_or(NComputationGraphRenderer::TGraph{}))));
}

YAML::Node TJsonComputationGraph::GetSwagger() {
    return YAML::Load(R"___(
        get:
            tags:
              - viewer
            summary: Computation graph
            description: Renders computation graph of a streaming query from .sys/streaming_queries
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: true
              - name: path
                in: query
                description: streaming query path
                type: string
                required: true
              - name: format
                in: query
                description: output format
                type: string
                enum: [svg, json]
                default: svg
                required: false
              - name: timeout
                in: query
                description: timeout in ms
                type: integer
                required: false
            responses:
                200:
                    description: OK
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                404:
                    description: Not Found
                504:
                    description: Gateway Timeout
        post:
            tags:
              - viewer
            summary: Computation graph from plan
            description: Renders computation graph SVG from a plan JSON document in the request body
            consumes:
              - application/json
            parameters:
              - name: body
                in: body
                description: plan JSON document
                required: true
                schema:
                    type: object
            responses:
                200:
                    description: OK
                400:
                    description: Bad Request
        )___");
}

} // namespace NKikimr::NViewer
