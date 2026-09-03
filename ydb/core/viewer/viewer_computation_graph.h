#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/library/computation_graph/computation_graph.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonComputationGraph : public TViewerPipeClient {
    using TThis = TJsonComputationGraph;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    bool Json = false;

public:
    TJsonComputationGraph(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        TStringBuf body = Event->Get()->Request.GetPostContent();
        if (!body.empty()) {
            NJson::TJsonValue plan;
            if (!NJson::ReadJsonTree(body, &plan)) {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "invalid plan json"));
            }
            return ReplyAndPassAway(GetHTTPOK("image/svg+xml",
                NComputationGraph::ToSvg(NComputationGraph::BuildGraph(plan))));
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
        auto& pathParam = (*request.MutableYdbParameters())["$path"];
        pathParam.mutable_type()->set_type_id(Ydb::Type::UTF8);
        pathParam.mutable_value()->set_text_value(std::string(path));
        if (const TString userToken = GetRequest().GetUserTokenObject()) {
            event->Record.SetUserToken(userToken);
        }
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
        Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default:
                return TBase::StateWork(ev);
        }
    }

    void ReplyAndPassAway() override {
        PassAway();
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
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

        if (Json) {
            NJson::TJsonValue plan;
            if (!planText.empty() && !NJson::ReadJsonTree(planText, &plan)) {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "invalid plan json"));
            }
            NJson::TJsonValue json(NJson::JSON_MAP);
            json["status"] = status;
            if (!issuesText.empty()) {
                json["issues"] = issuesText;
            }
            json["plan"] = plan;
            return ReplyAndPassAway(GetHTTPOKJSON(json));
        }

        NJson::TJsonValue plan;
        if (!planText.empty() && !NJson::ReadJsonTree(planText, &plan)) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "invalid plan json"));
        }
        ReplyAndPassAway(GetHTTPOK("image/svg+xml",
            NComputationGraph::ToSvg(NComputationGraph::BuildGraph(plan))));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
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
                    required: false
                  - name: format
                    in: query
                    description: svg (default) or json
                    type: string
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
                    404:
                        description: Not Found
                    504:
                        description: Gateway Timeout
            post:
                tags:
                  - viewer
                summary: Computation graph from plan
                description: Renders computation graph SVG from a plan JSON document in the request body
                responses:
                    200:
                        description: OK
                    400:
                        description: Bad Request
            )___");
        return node;
    }
};

} // namespace NKikimr::NViewer
