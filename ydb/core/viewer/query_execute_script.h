#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NKikimr {

namespace NRpcService {

template<>
void SetRequestSyncOperationMode<Ydb::Query::ExecuteScriptRequest>(Ydb::Query::ExecuteScriptRequest& request) {
    request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
}

}

namespace NViewer {

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
        AllowedMethods = {HTTP_METHOD_POST};
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            post:
                tags:
                  - script query
                summary: Execute script
                description: Execute script
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                type: object
                                properties:
                                    database:
                                        type: string
                                        required: true
                                    script_content:
                                        type: object
                                        properties:
                                            text:
                                                type: string
                                                description: query text
                                                required: true
                                            syntax:
                                                type: string
                                                description: |
                                                    syntax:
                                                      * `SYNTAX_YQL_V1`
                                                      * `SYNTAX_PG`
                                                required: false
                                    exec_mode:
                                        type: string
                                        description: |
                                            exec_mode:
                                              * `EXEC_MODE_PARSE`
                                              * `EXEC_MODE_VALIDATE`
                                              * `EXEC_MODE_EXPLAIN`
                                              * `EXEC_MODE_EXECUTE`
                                            required: true
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
}
