#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TOperationCancelRpc = TJsonLocalRpc<Ydb::Operations::CancelOperationRequest,
                                          Ydb::Operations::CancelOperationResponse,
                                          Ydb::Operations::CancelOperationResponse,
                                          Ydb::Operation::V1::OperationService,
                                          NKikimr::NGRpcService::TGrpcRequestNoOperationCall<Ydb::Operations::CancelOperationRequest, Ydb::Operations::CancelOperationResponse>>;

class TOperationCancel : public TOperationCancelRpc {
public:
    using TBase = TOperationCancelRpc;

    TOperationCancel(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        AllowedMethods = {HTTP_METHOD_POST};
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        post:
            tags:
              - operation
            summary: Cancels operation
            description: >
                Starts cancellation of a long-running operation,
                Clients can use GetOperation to check whether the cancellation succeeded
                or whether the operation completed despite cancellation.
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
        node["post"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Operations::CancelOperationResponse>();
        node["post"]["requestBody"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Operations::CancelOperationRequest>();
        return node;
    }
};

}

