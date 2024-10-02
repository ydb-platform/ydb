#pragma once
#include "json_handlers.h"
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

namespace NKikimr::NViewer {

class TSchemeDirectory : public IActor {
public:
    TSchemeDirectory(IViewer*, NMon::TEvHttpInfo::TPtr&) {}
};

using TSchemeDirectoryGetRpc = TJsonLocalRpc<Ydb::Scheme::ListDirectoryRequest,
                                             Ydb::Scheme::ListDirectoryResponse,
                                             Ydb::Scheme::ListDirectoryResult,
                                             Ydb::Scheme::V1::SchemeService,
                                             NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scheme::ListDirectoryRequest, Ydb::Scheme::ListDirectoryResponse>>;

using TSchemeDirectoryPostRpc = TJsonLocalRpc<Ydb::Scheme::MakeDirectoryRequest,
                                              Ydb::Scheme::MakeDirectoryResponse,
                                              Ydb::Scheme::MakeDirectoryResponse,
                                              Ydb::Scheme::V1::SchemeService,
                                              NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scheme::MakeDirectoryRequest, Ydb::Scheme::MakeDirectoryResponse>>;

using TSchemeDirectoryDeleteRpc = TJsonLocalRpc<Ydb::Scheme::RemoveDirectoryRequest,
                                                Ydb::Scheme::RemoveDirectoryResponse,
                                                Ydb::Scheme::RemoveDirectoryResponse,
                                                Ydb::Scheme::V1::SchemeService,
                                                NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scheme::RemoveDirectoryRequest, Ydb::Scheme::RemoveDirectoryResponse>>;


template<typename LocalRpcType>
class TSchemeDirectoryRequest : public LocalRpcType {
protected:
    using TBase = LocalRpcType;
    using TRequestProtoType = TBase::TRequestProtoType;
    using TBase::Database;

public:
    TSchemeDirectoryRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    bool ValidateRequest(TRequestProtoType& request) override {
        if (TBase::ValidateRequest(request)) {
            if (Database && request.path()) {
                TString path = request.path();
                if (!path.empty() && path[0] != '/') {
                    path = Database + "/" + path;
                    request.set_path(path);
                }
            }
            return true;
        }
        return false;
    }
};

class TJsonSchemeDirectoryHandler : public TJsonHandler<TSchemeDirectory> {
public:
    TJsonSchemeDirectoryHandler()
        : TJsonHandler<TSchemeDirectory>(GetSwagger())
    {}

    IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) override {
        switch (event->Get()->Request.GetMethod()) {
            case HTTP_METHOD_GET:
                return new TSchemeDirectoryRequest<TSchemeDirectoryGetRpc>(viewer, event);
            case HTTP_METHOD_POST:
                return new TSchemeDirectoryRequest<TSchemeDirectoryPostRpc>(viewer, event);
            case HTTP_METHOD_DELETE:
                return new TSchemeDirectoryRequest<TSchemeDirectoryDeleteRpc>(viewer, event);
            default:
                throw std::logic_error("Bad request method");
        }
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        get:
            tags:
              - scheme
            summary: List directory
            description: Returns information about given directory and objects inside it
            parameters:
              - name: database
                in: query
                description: database name
                required: true
                type: string
              - name: path
                in: query
                description: path to directory
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
        post:
            tags:
              - scheme
            summary: Make directory
            description: Makes directory
            parameters:
              - name: database
                in: query
                description: database name
                required: true
                type: string
              - name: path
                in: query
                description: path to directory
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
        delete:
            tags:
              - scheme
            summary: Remove directory
            description: Removes directory
            parameters:
              - name: database
                in: query
                description: database name
                required: true
                type: string
              - name: path
                in: query
                description: path to directory
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

        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Scheme::ListDirectoryResult>();
        node["post"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Scheme::MakeDirectoryResponse>();
        node["delete"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Scheme::RemoveDirectoryResponse>();
        return node;
    }
};

}
