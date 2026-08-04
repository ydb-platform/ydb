#include <ydb/core/kafka_proxy/actors/kafka_api_versions_actor.h>

#include <ydb/core/kafka_proxy/fuzz_targets/kafka_fuzz_common.h>

namespace {

struct TSupportedApi {
    i16 ApiKey;
    NKafka::TKafkaVersion MinVersion;
    NKafka::TKafkaVersion MaxVersion;
};

constexpr TSupportedApi SupportedApis[] = {
    {NKafka::PRODUCE, 3, 9},
    {NKafka::FETCH, 0, 4},
    {NKafka::LIST_OFFSETS, 0, 7},
    {NKafka::METADATA, 0, 9},
    {NKafka::OFFSET_COMMIT, 0, 8},
    {NKafka::OFFSET_FETCH, 0, 8},
    {NKafka::FIND_COORDINATOR, 0, 4},
    {NKafka::JOIN_GROUP, 0, 9},
    {NKafka::HEARTBEAT, 0, 4},
    {NKafka::LEAVE_GROUP, 0, 2},
    {NKafka::SYNC_GROUP, 0, 3},
    {NKafka::DESCRIBE_GROUPS, 0, 5},
    {NKafka::LIST_GROUPS, 0, 4},
    {NKafka::SASL_HANDSHAKE, 0, 1},
    {NKafka::API_VERSIONS, 0, 2},
    {NKafka::CREATE_TOPICS, 0, 7},
    {NKafka::INIT_PRODUCER_ID, 0, 4},
    {NKafka::ADD_PARTITIONS_TO_TXN, 0, 3},
    {NKafka::ADD_OFFSETS_TO_TXN, 0, 3},
    {NKafka::END_TXN, 0, 3},
    {NKafka::TXN_OFFSET_COMMIT, 0, 3},
    {NKafka::DESCRIBE_CONFIGS, 0, 4},
    {NKafka::ALTER_CONFIGS, 0, 2},
    {NKafka::SASL_AUTHENTICATE, 0, 2},
    {NKafka::CREATE_PARTITIONS, 0, 3},
};

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    NKafka::NFuzz::TFuzzedDataProvider fdp(data, size);
    const TSupportedApi& api = SupportedApis[fdp.ConsumeIntegralInRange<size_t>(0, std::size(SupportedApis) - 1)];

    NKafka::TRequestHeaderData header;
    header.RequestApiKey = api.ApiKey;
    header.RequestApiVersion = fdp.ConsumeIntegralInRange<NKafka::TKafkaVersion>(api.MinVersion, api.MaxVersion);
    header.CorrelationId = fdp.ConsumeIntegral<NKafka::TKafkaInt32>();
    header.ClientId = NKafka::NFuzz::ConsumeOptionalString(fdp);

    const TString serialized = NKafka::NFuzz::SerializeMessage(
        header,
        NKafka::RequestHeaderVersion(header.RequestApiKey, header.RequestApiVersion));
    const TString truncated = NKafka::NFuzz::MaybeTruncate(fdp, serialized);

    try {
        NKafka::NFuzz::ParseMessage<NKafka::TRequestHeaderData>(
            truncated,
            NKafka::RequestHeaderVersion(header.RequestApiKey, header.RequestApiVersion));
    } catch (...) {
    }

    return 0;
}
