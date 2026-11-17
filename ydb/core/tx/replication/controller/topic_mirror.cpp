#include "topic_mirror.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

#include <util/generic/algorithm.h>
#include <util/string/cast.h>

namespace NKikimr::NReplication::NController {

namespace {

TString ParseEndpoint(TStringBuf endpoint, TStringBuf& host, ui32& parsedPort) {
    TStringBuf port;
    if (endpoint.StartsWith('[')) {
        const auto close = endpoint.find(']');
        if (close == TStringBuf::npos || close == 1 || close + 1 >= endpoint.size() || endpoint[close + 1] != ':') {
            return "Invalid source endpoint";
        }
        host = endpoint.substr(0, close + 1);
        port = endpoint.substr(close + 2);
    } else {
        const auto colon = endpoint.rfind(':');
        if (colon == TStringBuf::npos) {
            return "Source endpoint must include a port";
        }
        host = endpoint.substr(0, colon);
        port = endpoint.substr(colon + 1);
    }

    if (host.empty() || !TryFromString<ui32>(port, parsedPort) || !parsedPort || parsedPort > 65535) {
        return "Invalid source endpoint";
    }

    return {};
}

TString PatchMirrorConfig(NKikimrPQ::TMirrorPartitionConfig& config,
        const NKikimrReplication::TConnectionParams& params, const TString& srcPath, const TString& consumer)
{
    if (params.HasCaCert()) {
        return "Custom source CA certificates are not supported by Topic mirroring";
    }

    TStringBuf host;
    ui32 parsedPort = 0;
    if (auto error = ParseEndpoint(params.GetEndpoint(), host, parsedPort)) {
        return error;
    }

    config.SetEndpoint(TString(host));
    config.SetEndpointPort(parsedPort);
    config.SetTopic(srcPath);
    config.SetDatabase(params.GetDatabase());
    config.SetConsumer(consumer);
    config.SetUseSecureConnection(params.GetEnableSsl());
    config.SetMirrorConsumerOffsets(true);

    auto& credentials = *config.MutableCredentials();
    switch (params.GetCredentialsCase()) {
    case NKikimrReplication::TConnectionParams::CREDENTIALS_NOT_SET:
        credentials.SetOauthToken("");
        break;
    case NKikimrReplication::TConnectionParams::kOAuthToken: {
        const auto& token = params.GetOAuthToken();
        if (!token.GetTokenSecretName().empty()) {
            credentials.SetOauthTokenSecretName(token.GetTokenSecretName());
        } else if (token.HasToken()) {
            credentials.SetOauthToken(token.GetToken());
        } else {
            return "Source OAuth credentials must contain a token or secret reference";
        }
        break;
    }
    default:
        return "Topic mirroring supports OAuth source credentials only";
    }

    return {};
}

} // anonymous namespace

Ydb::Topic::CreateTopicRequest BuildTopicMirrorRequest(const NYdb::NTopic::TTopicDescription& description,
        const TString& consumer)
{
    Ydb::Topic::CreateTopicRequest request;
    description.SerializeTo(request);
    request.mutable_partitioning_settings()->set_min_active_partitions(description.GetPartitions().size());

    const auto& source = NYdb::TProtoAccessor::GetProto(description);
    request.set_partition_total_read_speed_bytes_per_second(source.partition_total_read_speed_bytes_per_second());
    request.set_partition_write_burst_messages(source.partition_write_burst_messages());
    request.set_partition_total_read_speed_messages_per_second(source.partition_total_read_speed_messages_per_second());
    request.set_content_based_deduplication(source.content_based_deduplication());
    request.set_partition_write_speed_messages_per_second(source.partition_write_speed_messages_per_second());
    request.set_partition_read_without_consumer_speed_bytes_per_second(source.partition_read_without_consumer_speed_bytes_per_second());
    request.set_partition_read_without_consumer_speed_messages_per_second(source.partition_read_without_consumer_speed_messages_per_second());

    EraseIf(*request.mutable_consumers(), [&](const auto& item) {
        return item.name() == consumer;
    });

    return request;
}

TTopicMirrorSettings::TTopicMirrorSettings(const NKikimrReplication::TConnectionParams& params,
        const TString& srcPath, const TString& consumer)
{
    Error = PatchMirrorConfig(Config, params, srcPath, consumer);
}

}
