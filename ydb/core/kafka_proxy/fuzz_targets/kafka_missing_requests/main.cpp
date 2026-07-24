#include <ydb/core/kafka_proxy/kafka_constants.h>

#include <ydb/core/kafka_proxy/fuzz_targets/kafka_fuzz_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

template <class T>
void ParseRequest(TFuzzedDataProvider& fdp, const T& request, TKafkaVersion version) {
    const TString serialized = NFuzz::SerializeMessage(request, version);
    const TString truncatedMessage = NFuzz::MaybeTruncate(fdp, serialized);

    try {
        NFuzz::ParseMessage<T>(truncatedMessage, version);
    } catch (...) {
    }

    TRequestHeaderData header;
    header.RequestApiKey = request.ApiKey();
    header.RequestApiVersion = version;
    header.CorrelationId = fdp.ConsumeIntegral<TKafkaInt32>();
    header.ClientId = NFuzz::ConsumeOptionalString(fdp);

    const TString frame = NFuzz::SerializeRequestFrame(header, request, version);
    const TString truncatedFrame = NFuzz::MaybeTruncate(fdp, frame);

    try {
        NFuzz::ParseRequestFrame(truncatedFrame, request.ApiKey(), version);
    } catch (...) {
    }
}

TListGroupsRequestData BuildListGroupsRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TListGroupsRequestData request;
    if (version >= 4) {
        for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
            request.StatesFilter.push_back(NFuzz::ConsumeString(fdp));
        }
    }
    return request;
}

TInitProducerIdRequestData BuildInitProducerIdRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TInitProducerIdRequestData request;
    request.TransactionalId = NFuzz::ConsumeOptionalString(fdp);
    request.TransactionTimeoutMs = fdp.ConsumeIntegral<TKafkaInt32>();
    if (version >= 3) {
        request.ProducerId = fdp.ConsumeIntegral<TKafkaInt64>();
        request.ProducerEpoch = fdp.ConsumeIntegral<TKafkaInt16>();
    }
    return request;
}

TAddPartitionsToTxnRequestData BuildAddPartitionsToTxnRequest(TFuzzedDataProvider& fdp) {
    TAddPartitionsToTxnRequestData request;
    request.TransactionalId = NFuzz::ConsumeString(fdp);
    request.ProducerId = fdp.ConsumeIntegral<TKafkaInt64>();
    request.ProducerEpoch = fdp.ConsumeIntegral<TKafkaInt16>();

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic topic;
        topic.Name = NFuzz::ConsumeString(fdp);
        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            topic.Partitions.push_back(fdp.ConsumeIntegral<TKafkaInt32>());
        }
        request.Topics.push_back(std::move(topic));
    }

    return request;
}

TAddOffsetsToTxnRequestData BuildAddOffsetsToTxnRequest(TFuzzedDataProvider& fdp) {
    TAddOffsetsToTxnRequestData request;
    request.TransactionalId = NFuzz::ConsumeString(fdp);
    request.ProducerId = fdp.ConsumeIntegral<TKafkaInt64>();
    request.ProducerEpoch = fdp.ConsumeIntegral<TKafkaInt16>();
    request.GroupId = NFuzz::ConsumeString(fdp);
    return request;
}

TEndTxnRequestData BuildEndTxnRequest(TFuzzedDataProvider& fdp) {
    TEndTxnRequestData request;
    request.TransactionalId = NFuzz::ConsumeString(fdp);
    request.ProducerId = fdp.ConsumeIntegral<TKafkaInt64>();
    request.ProducerEpoch = fdp.ConsumeIntegral<TKafkaInt16>();
    request.Committed = fdp.ConsumeBool();
    return request;
}

TTxnOffsetCommitRequestData BuildTxnOffsetCommitRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TTxnOffsetCommitRequestData request;
    request.TransactionalId = NFuzz::ConsumeString(fdp);
    request.GroupId = NFuzz::ConsumeString(fdp);
    request.ProducerId = fdp.ConsumeIntegral<TKafkaInt64>();
    request.ProducerEpoch = fdp.ConsumeIntegral<TKafkaInt16>();

    if (version >= 3) {
        request.GenerationId = fdp.ConsumeIntegral<TKafkaInt32>();
        request.MemberId = NFuzz::ConsumeString(fdp);
        request.GroupInstanceId = NFuzz::ConsumeOptionalString(fdp);
    }

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic topic;
        topic.Name = NFuzz::ConsumeString(fdp);

        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition partition;
            partition.PartitionIndex = fdp.ConsumeIntegral<TKafkaInt32>();
            partition.CommittedOffset = fdp.ConsumeIntegral<TKafkaInt64>();
            if (version >= 2) {
                partition.CommittedLeaderEpoch = fdp.ConsumeIntegral<TKafkaInt32>();
            }
            partition.CommittedMetadata = NFuzz::ConsumeOptionalString(fdp);
            topic.Partitions.push_back(std::move(partition));
        }

        request.Topics.push_back(std::move(topic));
    }

    return request;
}

TDescribeConfigsRequestData BuildDescribeConfigsRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TDescribeConfigsRequestData request;

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TDescribeConfigsRequestData::TDescribeConfigsResource resource;
        resource.ResourceType = TOPIC_RESOURCE_TYPE;
        resource.ResourceName = NFuzz::ConsumeString(fdp);
        if (fdp.ConsumeBool()) {
            for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
                resource.ConfigurationKeys.push_back(NFuzz::ConsumeString(fdp));
            }
        }
        request.Resources.push_back(std::move(resource));
    }

    if (version >= 1) {
        request.IncludeSynonyms = fdp.ConsumeBool();
    }
    if (version >= 3) {
        request.IncludeDocumentation = fdp.ConsumeBool();
    }

    return request;
}

TAlterConfigsRequestData BuildAlterConfigsRequest(TFuzzedDataProvider& fdp) {
    TAlterConfigsRequestData request;
    request.ValidateOnly = fdp.ConsumeBool();

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TAlterConfigsRequestData::TAlterConfigsResource resource;
        resource.ResourceType = TOPIC_RESOURCE_TYPE;
        resource.ResourceName = NFuzz::ConsumeString(fdp);

        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig config;
            config.Name = NFuzz::ConsumeString(fdp);
            config.Value = NFuzz::ConsumeOptionalString(fdp);
            resource.Configs.push_back(std::move(config));
        }

        request.Resources.push_back(std::move(resource));
    }

    return request;
}

TCreatePartitionsRequestData BuildCreatePartitionsRequest(TFuzzedDataProvider& fdp) {
    TCreatePartitionsRequestData request;
    request.TimeoutMs = fdp.ConsumeIntegral<TKafkaInt32>();
    request.ValidateOnly = fdp.ConsumeBool();

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TCreatePartitionsRequestData::TCreatePartitionsTopic topic;
        topic.Name = NFuzz::ConsumeString(fdp);
        topic.Count = NFuzz::ConsumeInRange<TKafkaInt32>(fdp, 1, 32);

        if (fdp.ConsumeBool()) {
            for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
                TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment assignment;
                for (size_t k = 0; k < NFuzz::ConsumeCount(fdp); ++k) {
                    assignment.BrokerIds.push_back(fdp.ConsumeIntegral<TKafkaInt32>());
                }
                topic.Assignments.push_back(std::move(assignment));
            }
        }

        request.Topics.push_back(std::move(topic));
    }

    return request;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);

    switch (fdp.ConsumeIntegralInRange<int>(0, 8)) {
        case 0: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 4);
            ParseRequest(fdp, BuildListGroupsRequest(fdp, version), version);
            break;
        }
        case 1: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 4);
            ParseRequest(fdp, BuildInitProducerIdRequest(fdp, version), version);
            break;
        }
        case 2:
            ParseRequest(fdp, BuildAddPartitionsToTxnRequest(fdp), 3);
            break;
        case 3:
            ParseRequest(fdp, BuildAddOffsetsToTxnRequest(fdp), 3);
            break;
        case 4:
            ParseRequest(fdp, BuildEndTxnRequest(fdp), 3);
            break;
        case 5: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 3);
            ParseRequest(fdp, BuildTxnOffsetCommitRequest(fdp, version), version);
            break;
        }
        case 6: {
            const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 4);
            ParseRequest(fdp, BuildDescribeConfigsRequest(fdp, version), version);
            break;
        }
        case 7:
            ParseRequest(fdp, BuildAlterConfigsRequest(fdp), 2);
            break;
        case 8:
            ParseRequest(fdp, BuildCreatePartitionsRequest(fdp), 3);
            break;
    }

    return 0;
}
