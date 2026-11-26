#include "topic_mirror.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

#include <library/cpp/string_utils/url/url.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#include <algorithm>

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

std::optional<ui64> GetTopicDataLagOffsets(const Ydb::Topic::DescribeTopicResult& source,
        const Ydb::Topic::DescribeTopicResult& destination)
{
    if (source.partitions().empty() || source.partitions_size() != destination.partitions_size()) {
        return std::nullopt;
    }

    THashMap<i64, const Ydb::Topic::DescribeTopicResult::PartitionInfo*> partitions;
    for (const auto& part : source.partitions()) {
        if (!part.partition_stats().has_partition_offsets()
            || !partitions.emplace(part.partition_id(), &part).second)
        {
            return std::nullopt;
        }
    }

    ui64 maxLag = 0;
    for (const auto& part : destination.partitions()) {
        auto it = partitions.find(part.partition_id());
        if (it == partitions.end() || !part.partition_stats().has_partition_offsets()) {
            return std::nullopt;
        }
        const auto& srcPart = *it->second;
        if (part.active() != srcPart.active()
            || part.key_range().from_bound() != srcPart.key_range().from_bound()
            || part.key_range().to_bound() != srcPart.key_range().to_bound()
            || !std::ranges::is_permutation(part.parent_partition_ids(), srcPart.parent_partition_ids())
            || !std::ranges::is_permutation(part.child_partition_ids(), srcPart.child_partition_ids()))
        {
            return std::nullopt;
        }
        const auto& src = srcPart.partition_stats().partition_offsets();
        const auto& dst = part.partition_stats().partition_offsets();
        // A hole between retained ranges, or a missing retained prefix, precludes a complete sample.
        if (src.start() < 0 || src.end() < src.start() || dst.start() < 0 || dst.end() < dst.start()
            || dst.end() < src.start() || dst.start() > src.start())
        {
            return std::nullopt;
        }
        if (src.end() > dst.end()) {
            maxLag = Max(maxLag, static_cast<ui64>(src.end() - dst.end()));
        }
        partitions.erase(it);
    }

    return maxLag;
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
