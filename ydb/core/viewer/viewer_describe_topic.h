#include "json_handlers.h"
#include "json_local_rpc.h"
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

namespace NKikimr::NViewer {

using TDescribeTopicRpc = TJsonLocalRpc<Ydb::Topic::DescribeTopicRequest,
                                         Ydb::Topic::DescribeTopicResponse,
                                         Ydb::Topic::DescribeTopicResult,
                                         Ydb::Topic::V1::TopicService,
                                         NKikimr::NGRpcService::TEvDescribeTopicRequest>;

class TJsonDescribeTopic : public TDescribeTopicRpc {
public:
    using TBase = TDescribeTopicRpc;

    TJsonDescribeTopic(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_GET) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only GET method is allowed"));
        }
        const auto& params(Event->Get()->Request.GetParams());
        if (params.Has("database")) {
            Database = params.Get("database");
        } else if (params.Has("database_path")) {
            Database = params.Get("database_path");
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'database' is required"));
        }

        if (params.Has("path")) {
            Request.set_path(params.Get("path"));
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'path' is required"));
        }

        if (params.Has("include_stats")) {
            Request.set_include_stats(FromStringWithDefault<bool>(params.Get("include_stats"), false));
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
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<Ydb::Topic::DescribeTopicResult>());
        return yaml;
    }
};

}
