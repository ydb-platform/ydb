#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>
#include <ydb/core/kafka_proxy/actors/kafka_read_session_utils.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

TConsumerProtocolSubscription BuildSubscription(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TConsumerProtocolSubscription subscription;

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        subscription.Topics.push_back(NFuzz::ConsumeString(fdp));
    }

    if (version >= 1) {
        for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
            TConsumerProtocolSubscription::TopicPartition topicPartition;
            topicPartition.Topic = NFuzz::ConsumeString(fdp);
            for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
                topicPartition.Partitions.push_back(fdp.ConsumeIntegral<TKafkaInt32>());
            }
            subscription.OwnedPartitions.push_back(std::move(topicPartition));
        }
    }

    if (version >= 2) {
        subscription.GenerationId = fdp.ConsumeIntegral<TKafkaInt32>();
    }
    if (version >= 3) {
        subscription.RackId = NFuzz::ConsumeOptionalString(fdp);
    }

    return subscription;
}

void TryGetSubscriptionsFromRawMetadata(const TString& metadata) {
    TJoinGroupRequestData request;
    request.ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;

    TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
    protocol.Name = metadata.size() % 2 == 0
        ? ASSIGN_STRATEGY_ROUNDROBIN
        : ASSIGN_STRATEGY_SERVER;
    protocol.Metadata = NFuzz::BytesView(metadata);
    request.Protocols.push_back(std::move(protocol));

    (void)GetSubscriptions(request);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 3);
    const TConsumerProtocolSubscription subscription = BuildSubscription(fdp, version);

    const TString serialized = NFuzz::SerializeMessage(subscription, version);
    const TString truncated = NFuzz::MaybeTruncate(fdp, serialized);

    try {
        NFuzz::ParseMessage<TConsumerProtocolSubscription>(truncated, version);
    } catch (...) {
    }

    const TString versioned = NFuzz::SerializeVersionedMessage(subscription, version);
    const TString truncatedVersioned = NFuzz::MaybeTruncate(fdp, versioned);

    TJoinGroupRequestData request;
    request.ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;

    TJoinGroupRequestData::TJoinGroupRequestProtocol protocol;
    protocol.Name = fdp.ConsumeBool()
        ? ASSIGN_STRATEGY_ROUNDROBIN
        : ASSIGN_STRATEGY_SERVER;
    protocol.Metadata = NFuzz::BytesView(truncatedVersioned);
    request.Protocols.push_back(std::move(protocol));

    try {
        (void)GetSubscriptions(request);
    } catch (...) {
    }

    const TString rawInput(reinterpret_cast<const char*>(data), size);

    try {
        NFuzz::ParseVersionedEnvelope<TConsumerProtocolSubscription>(rawInput, 0, 3);
    } catch (...) {
    }

    try {
        TryGetSubscriptionsFromRawMetadata(rawInput);
    } catch (...) {
    }

    return 0;
}
