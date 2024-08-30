#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TQueryExecuteScriptRpc = TJsonLocalRpc<Ydb::Query::ExecuteScriptRequest,
                                       Ydb::Operations::Operation,
                                       Ydb::Operations::Operation,
                                       Ydb::Query::V1::QueryService,
                                       NKikimr::NGRpcService::TGrpcRequestNoOperationCall<Ydb::Query::ExecuteScriptRequest, Ydb::Operations::Operation>>;

class TQueryExecuteScript : public TQueryExecuteScriptRpc {
public:
    using TBase = TQueryExecuteScriptRpc;

    TQueryExecuteScript(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        OperationMode = Ydb::Operations::OperationParams::ASYNC;
    }

    void Bootstrap() override {
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only POST method is allowed"));
        }

        TStringBuf content = Event->Get()->Request.GetPostContent();
        static NJson::TJsonReaderConfig JsonConfig;
        NJson::TJsonValue params;
        bool success = NJson::ReadJsonTree(content, &JsonConfig, &params);
        if (!success) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Empty content received"));
        }
        if (params.Has("database")) {
            Database = params["database"].GetStringRobust();
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'database' is required"));
        }
        Request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
        if (params.Has("query")) {
            auto query = params["query"].GetStringRobust();
            Request.mutable_script_content()->set_text(query);
            auto syntax = params["syntax"].GetStringRobust();
            if (syntax == "yql_v1") {
                Request.mutable_script_content()->set_syntax(Ydb::Query::SYNTAX_YQL_V1);
            } else if (syntax == "pg") {
                Request.mutable_script_content()->set_syntax(Ydb::Query::SYNTAX_PG);
            } else {
                Request.mutable_script_content()->set_syntax(Ydb::Query::SYNTAX_UNSPECIFIED);
            }
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'query' is required"));
        }

        if (params.Has("stats")) {
            auto Stats = params["stats"].GetStringRobust();
            if (Stats == "none"){
                Request.set_stats_mode(Ydb::Query::STATS_MODE_NONE);
            } else if (Stats == "basic") {
                Request.set_stats_mode(Ydb::Query::STATS_MODE_BASIC);
            } else if (Stats == "profile") {
                Request.set_stats_mode(Ydb::Query::STATS_MODE_FULL);
            } else if (Stats == "full") {
                Request.set_stats_mode(Ydb::Query::STATS_MODE_PROFILE);
            }
        } else {
            Request.set_stats_mode(Ydb::Query::STATS_MODE_NONE);
        }

        if (params.Has("exec_mode")) {
            auto execMode = params["exec_mode"].GetStringRobust();
            if (execMode == "parse") {
                Request.set_exec_mode(Ydb::Query::EXEC_MODE_PARSE);
            } else if (execMode == "validate") {
                Request.set_exec_mode(Ydb::Query::EXEC_MODE_VALIDATE);
            } else if (execMode == "explain") {
                Request.set_exec_mode(Ydb::Query::EXEC_MODE_EXPLAIN);
            } else if (execMode== "execute") {
                Request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
            }
        } else {
            Request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
        }

        TBase::Bootstrap();
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                    - operation
                summary: Execute script
                description: Execute script
                parameters:
                  - name: database
                    in: query
                    description: database name
                    required: true
                    type: string
                  - name: query
                    in: query
                    description: query
                    required: true
                    type: string
                  - name: syntax
                    in: query
                    description: syntax
                    required: false
                    type: string
                  - name: stats
                    in: query
                    description: stats
                    required: false
                    type: string
                  - name: exec_mode
                    in: query
                    description: exec_mode
                    required: false
                    type: string
                responses:
                    200:
                        description: OK
                        content:
                            application/json:
                                schema: {}
                    400:
                        description: Bad Request
                    403:
                        description: Forbidden
                    504:
                        description: Gateway Timeout
            )___");
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Operations::Operation>();
        return node;
    }
};

}
