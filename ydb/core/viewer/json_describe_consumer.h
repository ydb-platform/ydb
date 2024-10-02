#pragma once
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include "json_local_rpc.h"

namespace NKikimr {
namespace NViewer {

using TDescribeConsumerRpc = TJsonLocalRpc<Ydb::Topic::DescribeConsumerRequest,
                                         Ydb::Topic::DescribeConsumerResponse,
                                         Ydb::Topic::DescribeConsumerResult,
                                         Ydb::Topic::V1::TopicService,
                                         NKikimr::NGRpcService::TEvDescribeConsumerRequest>;

class TJsonDescribeConsumer : public TDescribeConsumerRpc {
public:
    using TBase = TDescribeConsumerRpc;

    TJsonDescribeConsumer(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
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

        if (params.Has("consumer")) {
            Request.set_consumer(params.Get("consumer"));
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'consumer' is required"));
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
};

template<>
YAML::Node TJsonRequestSwagger<TJsonDescribeConsumer>::GetSwagger() {
    YAML::Node node = YAML::Load(R"___(
        get:
          tags:
          - viewer
          summary: Topic schema detailed information
          description: Returns detailed information about topic
          parameters:
          - name: database
            in: query
            description: database name
            required: true
            type: string
          - name: consumer
            in: query
            description: consumer name
            required: true
            type: string
          - name: include_stats
            in: query
            description: include stat flag
            required: false
            type: bool
          - name: timeout
            in: query
            description: timeout in ms
            required: false
            type: integer
          - name: enums
            in: query
            description: convert enums to strings
            required: false
            type: boolean
          - name: ui64
            in: query
            description: return ui64 as number
            required: false
            type: boolean
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
    node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Topic::DescribeConsumerResult>();
    return node;
}

}
}
