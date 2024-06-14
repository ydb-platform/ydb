#pragma once
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include "json_local_rpc.h"

namespace NKikimr {
namespace NViewer {

using TJsonDescribeTopic = TJsonLocalRpc<Ydb::Topic::DescribeTopicRequest,
                                         Ydb::Topic::DescribeTopicResponse,
                                         Ydb::Topic::DescribeTopicResult,
                                         Ydb::Topic::V1::TopicService,
                                         NKikimr::NGRpcService::TEvDescribeTopicRequest>;

template<>
YAML::Node TJsonRequestSwagger<TJsonDescribeTopic>::GetSwagger() {
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
          - name: path
            in: query
            description: schema path
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
    node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<Ydb::Topic::DescribeTopicResult>();
    return node;
}

}
}
