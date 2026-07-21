#include <ydb/core/kafka_proxy/fuzz_targets/kafka_generated_requests_common.h>

namespace {

using namespace NKafka;
using NKafka::NFuzz::TFuzzedDataProvider;

TConsumerProtocolAssignment BuildAssignment(TFuzzedDataProvider& fdp) {
    TConsumerProtocolAssignment assignment;

    for (size_t i = 0; i < NFuzz::ConsumeCount(fdp); ++i) {
        TConsumerProtocolAssignment::TopicPartition topicPartition;
        topicPartition.Topic = NFuzz::ConsumeString(fdp);
        for (size_t j = 0; j < NFuzz::ConsumeCount(fdp); ++j) {
            topicPartition.Partitions.push_back(fdp.ConsumeIntegral<TKafkaInt32>());
        }
        assignment.AssignedPartitions.push_back(std::move(topicPartition));
    }

    return assignment;
}

void ParseVersionedAssignmentEnvelope(const TString& bytes) {
    NFuzz::ParseVersionedEnvelope<TConsumerProtocolAssignment>(bytes, 0, 3);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TFuzzedDataProvider fdp(data, size);
    const TKafkaVersion version = fdp.ConsumeIntegralInRange<TKafkaVersion>(0, 3);
    const TConsumerProtocolAssignment assignment = BuildAssignment(fdp);

    const TString serialized = NFuzz::SerializeMessage(assignment, version);
    const TString truncated = NFuzz::MaybeTruncate(fdp, serialized);

    try {
        NFuzz::ParseMessage<TConsumerProtocolAssignment>(truncated, version);
    } catch (...) {
    }

    const TString versioned = NFuzz::SerializeVersionedMessage(assignment, version);
    const TString truncatedVersioned = NFuzz::MaybeTruncate(fdp, versioned);

    try {
        ParseVersionedAssignmentEnvelope(truncatedVersioned);
    } catch (...) {
    }

    const TString rawInput(reinterpret_cast<const char*>(data), size);
    try {
        ParseVersionedAssignmentEnvelope(rawInput);
    } catch (...) {
    }

    return 0;
}
