#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TOperationForgetRpc = TJsonLocalRpc<Ydb::Operations::ForgetOperationRequest,
                                          Ydb::Operations::ForgetOperationResponse,
                                          Ydb::Operations::ForgetOperationResponse,
                                          Ydb::Operation::V1::OperationService,
                                          NKikimr::NGRpcService::TGrpcRequestNoOperationCall<Ydb::Operations::ForgetOperationRequest, Ydb::Operations::ForgetOperationResponse>>;

class TOperationForget : public TOperationForgetRpc {
public:
    using TBase = TOperationForgetRpc;

    TOperationForget(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only POST method is allowed"));
        }

        if (!PostToRequest()) {
            return;
        }

        const auto& params(Event->Get()->Request.GetParams());
        if (params.Has("database")) {
            Database = params.Get("database");
        }

        if (Database.empty()) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'database' is required"));
        }

        if (params.Has("id")) {
            Request.set_id(params.Get("id"));
        }

        if (Request.id().empty()) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'id' is required"));
        }

        TBase::Bootstrap();
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            post:
                tags:
                  - operation
                summary: Forgets operation
                description: >
                    Forgets long-running operation. It does not cancel the operation and returns
                    an error if operation was not completed.
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
                requestBody:
                    content:
                        application/json:
                            schema: {}
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
        node["post"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Operations::ForgetOperationResponse>();
        node["post"]["requestBody"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Operations::ForgetOperationRequest>();
        return node;
    }
};

}
