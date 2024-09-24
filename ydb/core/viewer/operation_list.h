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

        if (params.Has("kind")) {
            Request.set_kind(params.Get("kind"));
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'kind' is required"));
        }

        if (params.Has("page_size")) {
            Request.set_page_size(FromStringWithDefault<ui32>(params.Get("page_size"), 0));
        }

        if (params.Has("page_token")) {
            Request.set_page_token(params.Get("page_token"));
        }

        TBase::Bootstrap();
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
                    description: kind
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
