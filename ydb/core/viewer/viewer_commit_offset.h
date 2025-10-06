#include "json_handlers.h"
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TCommitOffsetRpc = TJsonLocalRpc<Ydb::Topic::CommitOffsetRequest,
                                        Ydb::Topic::CommitOffsetResponse,
                                        Ydb::Topic::CommitOffsetResult,
                                        Ydb::Topic::V1::TopicService,
                                        NKikimr::NGRpcService::TEvCommitOffsetTopicRequest>;

class TJsonCommitOffset : public TCommitOffsetRpc {
public:
    using TBase = TCommitOffsetRpc;

    TJsonCommitOffset(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        AllowedMethods = {HTTP_METHOD_GET, HTTP_METHOD_PUT};
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        if (params.Has("database_path")) {
            Database = params.Get("database_path");
        }
        TBase::Bootstrap();
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Topic schema detailed information",
            .Description = "Returns detailed information about topic",
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
            .Name = "consumer",
            .Description = "consumer name",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "partition_id",
            .Description = "partition id",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "offset",
            .Description = "offset",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "read_session_id",
            .Description = "read session if",
            .Type = "string", // optional?
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<Ydb::Topic::CommitOffsetResult>());
        return yaml;
    }
};

}
