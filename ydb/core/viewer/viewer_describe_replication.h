#include "json_handlers.h"
#include "json_local_rpc.h"

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_replication.h>
#include <ydb/services/replication/grpc_service.h>

namespace NKikimr::NGRpcService {
using TEvDescribeReplicationRequest = TGrpcRequestOperationCall<Ydb::Replication::DescribeReplicationRequest, Ydb::Replication::DescribeReplicationResponse>;
}

namespace NKikimr::NViewer {

using TDescribeReplicationRpc = TJsonLocalRpc<Ydb::Replication::DescribeReplicationRequest,
                                        Ydb::Replication::DescribeReplicationResponse,
                                        Ydb::Replication::DescribeReplicationResult,
                                        Ydb::Replication::V1::ReplicationService,
                                        NKikimr::NGRpcService::TEvDescribeReplicationRequest>;

class TJsonDescribeReplication : public TDescribeReplicationRpc {
public:
    using TBase = TDescribeReplicationRpc;

    TJsonDescribeReplication(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        AllowedMethods = {HTTP_METHOD_GET};
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Replication schema detailed information",
            .Description = "Returns detailed information about replication",
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "include_stats",
            .Description = "include stat flag",
            .Type = "bool",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Required = false,
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<Ydb::Replication::DescribeReplicationResult>());
        return yaml;
    }
};

}
