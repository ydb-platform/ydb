#pragma once
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TOperationListRpc = TJsonLocalRpc<Ydb::Operations::ListOperationsRequest,
                                        Ydb::Operations::ListOperationsResponse,
                                        Ydb::Operations::ListOperationsResponse,
                                        Ydb::Operation::V1::OperationService,
                                        NKikimr::NGRpcService::TGrpcRequestNoOperationCall<Ydb::Operations::ListOperationsRequest, Ydb::Operations::ListOperationsResponse>>;

class TOperationList : public TOperationListRpc {
public:
    using TBase = TOperationListRpc;

    TOperationList(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        AllowedMethods = {HTTP_METHOD_GET};
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                  - operation
                summary: List operations
                description: Lists operations that match the specified filter in the request
                parameters:
                  - name: database
                    in: query
                    description: database name
                    required: true
                    type: string
                  - name: kind
                    in: query
                    description: >
                        kind:
                          * `ss/backgrounds`
                          * `export`
                          * `import`
                          * `buildindex`
                          * `scriptexec`
                    required: true
                    type: string
                  - name: page_size
                    in: query
                    description: page size
                    required: false
                    type: integer
                  - name: page_token
                    in: query
                    description: page token
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
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Operations::ListOperationsResponse>();
        TProtoToYaml::FillEnum(node["get"]["parameters"][1]["enum"], NProtoBuf::GetEnumDescriptor<Ydb::TOperationId::EKind>(), {
            .ConvertToLowerCase = true,
            .SkipDefaultValue = true
        });
        return node;
    }
};

}
