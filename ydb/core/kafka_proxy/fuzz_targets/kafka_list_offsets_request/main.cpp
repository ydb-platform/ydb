#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

TListOffsetsRequestData BuildListOffsetsRequest(TFuzzedDataProvider& fdp, TKafkaVersion version) {
    TListOffsetsRequestData request;
    request.ReplicaId = fdp.ConsumeIntegral<TKafkaInt32>();
    if (version >= 2) {
        request.IsolationLevel = fdp.ConsumeIntegral<TKafkaInt8>();
    }

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TListOffsetsRequestData::TListOffsetsTopic topic;
        topic.Name = NFuzz::ConsumeString(fdp);

        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition partition;
            partition.PartitionIndex = fdp.ConsumeIntegral<TKafkaInt32>();
            if (version >= 4) {
                partition.CurrentLeaderEpoch = fdp.ConsumeIntegral<TKafkaInt32>();
            }
            partition.Timestamp = fdp.ConsumeIntegral<TKafkaInt64>();
            if (version == 0) {
                partition.MaxNumOffsets = NFuzz::ConsumeInRange<TKafkaInt32>(fdp, 1, 16);
            }
            topic.Partitions.push_back(std::move(partition));
        }

        request.Topics.push_back(std::move(topic));
    }

    return request;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 7);

    NFuzz::ParseGeneratedRequest(fdp, BuildListOffsetsRequest(fdp, version), version);
    return 0;
}
