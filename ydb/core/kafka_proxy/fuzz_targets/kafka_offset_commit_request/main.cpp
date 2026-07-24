#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

TOffsetCommitRequestData BuildOffsetCommitRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TOffsetCommitRequestData request;
    request.GroupId = NFuzz::ConsumeString(fdp);

    if (version >= 1) {
        request.GenerationId = fdp.ConsumeIntegral<TKafkaInt32>();
        request.MemberId = NFuzz::ConsumeString(fdp);
    }
    if (version >= 7) {
        request.GroupInstanceId = NFuzz::ConsumeOptionalString(fdp);
    }
    if (version >= 2 && version <= 4) {
        request.RetentionTimeMs = fdp.ConsumeIntegral<TKafkaInt64>();
    }

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TOffsetCommitRequestData::TOffsetCommitRequestTopic topic;
        topic.Name = NFuzz::ConsumeString(fdp);

        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition partition;
            partition.PartitionIndex = fdp.ConsumeIntegral<TKafkaInt32>();
            partition.CommittedOffset = fdp.ConsumeIntegral<TKafkaInt64>();
            if (version >= 6) {
                partition.CommittedLeaderEpoch = fdp.ConsumeIntegral<TKafkaInt32>();
            }
            if (version == 1) {
                partition.CommitTimestamp = fdp.ConsumeIntegral<TKafkaInt64>();
            }
            partition.CommittedMetadata = NFuzz::ConsumeOptionalString(fdp);
            topic.Partitions.push_back(std::move(partition));
        }

        request.Topics.push_back(std::move(topic));
    }

    return request;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 8);

    NFuzz::ParseGeneratedRequest(fdp, BuildOffsetCommitRequest(fdp, version), version);
    return 0;
}
