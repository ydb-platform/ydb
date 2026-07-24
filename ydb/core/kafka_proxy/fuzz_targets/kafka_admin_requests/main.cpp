#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

TFindCoordinatorRequestData BuildFindCoordinatorRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TFindCoordinatorRequestData request;
    if (version <= 3) {
        request.Key = NFuzz::ConsumeString(fdp);
    }
    if (version >= 1) {
        request.KeyType = fdp.ConsumeIntegral<TKafkaInt8>();
    }
    if (version >= 4) {
        const size_t keyCount = std::max<size_t>(1, NFuzz::ConsumeCount(fdp));
        for (size_t i = 0; i < keyCount; ++i) {
            request.CoordinatorKeys.push_back(NFuzz::ConsumeString(fdp));
        }
    }
    return request;
}

TSaslHandshakeRequestData BuildSaslHandshakeRequest(TFuzzedDataProvider& fdp) {
    TSaslHandshakeRequestData request;
    request.Mechanism = NFuzz::ConsumeString(fdp);
    return request;
}

TApiVersionsRequestData BuildApiVersionsRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TApiVersionsRequestData request;
    if (version >= 3) {
        request.ClientSoftwareName = NFuzz::ConsumeString(fdp);
        request.ClientSoftwareVersion = NFuzz::ConsumeString(fdp);
    }
    return request;
}

TCreateTopicsRequestData BuildCreateTopicsRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TCreateTopicsRequestData request;
    const size_t topicCount = std::max<size_t>(1, NFuzz::ConsumeCount(fdp));

    for (size_t i = 0; i < topicCount; ++i) {
        TCreateTopicsRequestData::TCreatableTopic topic;
        topic.Name = NFuzz::ConsumeString(fdp);
        topic.NumPartitions = fdp.ConsumeBool() ? -1 : NFuzz::ConsumeInRange<TKafkaInt32>(fdp, 1, 16);
        topic.ReplicationFactor = fdp.ConsumeBool() ? -1 : NFuzz::ConsumeInRange<TKafkaInt16>(fdp, 1, 3);

        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment assignment;
            assignment.PartitionIndex = fdp.ConsumeIntegral<TKafkaInt32>();
            for (size_t k = 0; k < NFuzz::ConsumeCount(fdp); ++k) {
                assignment.BrokerIds.push_back(fdp.ConsumeIntegral<TKafkaInt32>());
            }
            topic.Assignments.push_back(std::move(assignment));
        }

        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig config;
            config.Name = NFuzz::ConsumeString(fdp);
            config.Value = NFuzz::ConsumeOptionalString(fdp);
            topic.Configs.push_back(std::move(config));
        }

        request.Topics.push_back(std::move(topic));
    }

    request.TimeoutMs = NFuzz::ConsumeInRange<TKafkaInt32>(fdp, 0, 120000);
    if (version >= 1) {
        request.ValidateOnly = fdp.ConsumeBool();
    }

    return request;
}

TSaslAuthenticateRequestData BuildSaslAuthenticateRequest(TFuzzedDataProvider& fdp, NFuzz::TBytesStorage& bytesStorage) {
    TSaslAuthenticateRequestData request;
    request.AuthBytes = bytesStorage.Hold(NFuzz::ConsumeBytes(fdp, 96));
    return request;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    NFuzz::TBytesStorage bytesStorage;

    switch (fdp.ConsumeIntegralInRange<int>(0, 4)) {
        case 0: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 4);
            NFuzz::ParseGeneratedRequest(fdp, BuildFindCoordinatorRequest(fdp, version), version);
            break;
        }
        case 1: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 1);
            NFuzz::ParseGeneratedRequest(fdp, BuildSaslHandshakeRequest(fdp), version);
            break;
        }
        case 2: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 4);
            NFuzz::ParseGeneratedRequest(fdp, BuildApiVersionsRequest(fdp, version), version);
            break;
        }
        case 3: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 7);
            NFuzz::ParseGeneratedRequest(fdp, BuildCreateTopicsRequest(fdp, version), version);
            break;
        }
        case 4: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 2);
            NFuzz::ParseGeneratedRequest(fdp, BuildSaslAuthenticateRequest(fdp, bytesStorage), version);
            break;
        }
    }

    return 0;
}
