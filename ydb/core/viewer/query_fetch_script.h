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
    {
        AllowedMethods = {HTTP_METHOD_GET};
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                  - script query
                summary: Get operation
                description: Check status for a given operation
                parameters:
                  - name: database
                    in: query
                    description: database name
                    required: true
                    type: string
                  - name: operation_id
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
                    description: rows limit (less than 1000 allowed)
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
