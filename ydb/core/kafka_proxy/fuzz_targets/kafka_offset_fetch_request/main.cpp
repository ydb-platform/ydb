#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

void FillTopicPartitions(TFuzzedDataProvider& fdp, std::vector<TKafkaInt32>& partitions) {
    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        partitions.push_back(fdp.ConsumeIntegral<TKafkaInt32>());
    }
}

TOffsetFetchRequestData BuildOffsetFetchRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TOffsetFetchRequestData request;

    if (version <= 7) {
        request.GroupId = NFuzz::ConsumeString(fdp);

        for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
            TOffsetFetchRequestData::TOffsetFetchRequestTopic topic;
            topic.Name = NFuzz::ConsumeString(fdp);
            FillTopicPartitions(fdp, topic.PartitionIndexes);
            request.Topics.push_back(std::move(topic));
        }
    } else {
        for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
            TOffsetFetchRequestData::TOffsetFetchRequestGroup group;
            group.GroupId = NFuzz::ConsumeString(fdp);

            for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
                TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topic;
                topic.Name = NFuzz::ConsumeString(fdp);
                FillTopicPartitions(fdp, topic.PartitionIndexes);
                group.Topics.push_back(std::move(topic));
            }

            request.Groups.push_back(std::move(group));
        }
    }

    if (version >= 7) {
        request.RequireStable = fdp.ConsumeBool();
    }

    return request;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 8);

    NFuzz::ParseGeneratedRequest(fdp, BuildOffsetFetchRequest(fdp, version), version);
    return 0;
}
