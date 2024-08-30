#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TQueryFetchScriptRpc = TJsonLocalRpc<Ydb::Query::FetchScriptResultsRequest,
                                       Ydb::Query::FetchScriptResultsResponse,
                                       Ydb::Query::FetchScriptResultsResponse,
                                       Ydb::Query::V1::QueryService,
                                       NKikimr::NGRpcService::TGrpcRequestNoOperationCall<Ydb::Query::FetchScriptResultsRequest, Ydb::Query::FetchScriptResultsResponse>>;

class TQueryFetchScript : public TQueryFetchScriptRpc {
public:
    using TBase = TQueryFetchScriptRpc;

    TQueryFetchScript(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_GET) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only GET method is allowed"));
        }
        const auto& params(Event->Get()->Request.GetParams());
        if (params.Has("database")) {
            Database = params.Get("database");
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'database' is required"));
        }

        if (params.Has("id")) {
            Request.set_operation_id(params.Get("id"));
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'id' is required"));
        }

        if (params.Has("result_set_index")) {
            Request.set_result_set_index(FromStringWithDefault<ui32>(params.Get("result_set_index"), 0));
        }
        if (params.Has("fetch_token")) {
            Request.set_fetch_token(params.Get("fetch_token"));
        }
        ui64 rowsLimit = 10000;
        if (params.Has("rows_limit")) {
            rowsLimit = FromStringWithDefault<ui32>(params.Get("rows_limit"), rowsLimit);
        }
        Request.set_rows_limit(rowsLimit);

        TBase::Bootstrap();
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                  - operation
                summary: Get operation
                description: Check status for a given operation
                parameters:
                  - name: database
                    in: query
                    description: database name
                    required: true
                    type: string
                  - name: id
                    in: query
                    description: operation id
                    required: true
                    type: string
                  - name: result_set_index
                    in: query
                    description: result set index
                    required: false
                    type: string
                  - name: fetch_token
                    in: query
                    description: fetch token
                    required: false
                    type: string
                  - name: rows_limit
                    in: query
                    description: rows limit
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
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Query::FetchScriptResultsResponse>();
        return node;
    }
};

}
