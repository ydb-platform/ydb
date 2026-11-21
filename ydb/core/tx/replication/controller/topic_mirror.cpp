#include "topic_mirror.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

#include <library/cpp/string_utils/url/url.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NReplication::NController {

namespace {

TString ParseEndpoint(TStringBuf endpoint, TStringBuf& host, ui16& port) {
    TStringBuf scheme;
    if (!TryGetSchemeHostAndPort(endpoint, scheme, host, port)
        || !scheme.empty()
        || host.empty()
        || !port
        || GetHostAndPort(endpoint) != endpoint)
    {
        return "Invalid source endpoint";
    }

    if (host.StartsWith('[') && (host.size() <= 2 || host.find(']') != host.size() - 1)) {
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
    ui16 parsedPort = 0;
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

    const auto& source = NYdb::TProtoAccessor::GetProto(description);
    *request.mutable_consumers() = source.consumers();
    request.set_partition_total_read_speed_bytes_per_second(source.partition_total_read_speed_bytes_per_second());
    request.set_partition_write_burst_messages(source.partition_write_burst_messages());
    request.set_partition_total_read_speed_messages_per_second(source.partition_total_read_speed_messages_per_second());
    request.set_content_based_deduplication(source.content_based_deduplication());
    request.set_partition_write_speed_messages_per_second(source.partition_write_speed_messages_per_second());
    request.set_partition_read_without_consumer_speed_bytes_per_second(source.partition_read_without_consumer_speed_bytes_per_second());
    request.set_partition_read_without_consumer_speed_messages_per_second(source.partition_read_without_consumer_speed_messages_per_second());

    request.mutable_attributes()->erase("_ydb_replication_consumer");
    // Source write restrictions must not block writes by the destination mirrorer.
    request.mutable_attributes()->erase("_write_mode");
    EraseIf(*request.mutable_consumers(), [&](const auto& item) {
        return item.name() == consumer;
    });

    return request;
}

TTopicMirrorSettings::TTopicMirrorSettings()
    : Config(std::make_unique<NKikimrPQ::TMirrorPartitionConfig>())
{
}

TTopicMirrorSettings::TTopicMirrorSettings(const NKikimrReplication::TConnectionParams& params,
        const TString& srcPath, const TString& consumer)
    : TTopicMirrorSettings()
{
    Error = PatchMirrorConfig(*Config, params, srcPath, consumer);
}

TTopicMirrorSettings::TTopicMirrorSettings(TTopicMirrorSettings&&) noexcept = default;
TTopicMirrorSettings::~TTopicMirrorSettings() = default;

}
