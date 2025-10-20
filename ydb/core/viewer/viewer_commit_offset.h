#include "json_handlers.h"
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TCommitOffsetRpc = TJsonLocalRpc<Ydb::Topic::CommitOffsetRequest,
                                        Ydb::Topic::CommitOffsetResponse,
                                        Ydb::Topic::CommitOffsetResult,
                                        Ydb::Topic::V1::TopicService,
                                        NKikimr::NGRpcService::TEvCommitOffsetRequest>;

class TJsonCommitOffset : public TCommitOffsetRpc {
public:
    using TBase = TCommitOffsetRpc;

    TJsonCommitOffset(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        AllowedMethods = {HTTP_METHOD_POST};
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        if (params.Has("database")) {
            Database = params.Get("database");
        }
        TBase::Bootstrap();
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "post",
            .Tag = "viewer",
            .Summary = "Commit offset handler",
            .Description = "Commiting offsets for specific topic partition and consumer",
        });
        yaml.AddParameter({
            .Name = "database",
            .Description = "database name",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "topic path",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "consumer",
            .Description = "consumer name",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "partition_id",
            .Description = "partition id",
            .Type = "integer",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "offset",
            .Description = "offset",
            .Type = "integer",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "read_session_id",
            .Description = "read session id",
            .Type = "string",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<Ydb::Topic::CommitOffsetResult>());
        return yaml;
    }
};
}
