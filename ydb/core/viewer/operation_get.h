#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TOperationGetRpc = TJsonLocalRpc<Ydb::Operations::GetOperationRequest,
                                       Ydb::Operations::GetOperationResponse,
                                       Ydb::Operations::Operation,
                                       Ydb::Operation::V1::OperationService,
                                       NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Operations::GetOperationRequest, Ydb::Operations::GetOperationResponse>>;

class TOperationGet : public TOperationGetRpc {
public:
    using TBase = TOperationGetRpc;

    TOperationGet(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
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
            Request.set_id(params.Get("id"));
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'id' is required"));
        }

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
