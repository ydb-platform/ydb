// Fuzzer for NKafka::TProduceRequestData binary wire parsing.
// The Kafka PRODUCE request is the primary data ingestion path and arrives
// over a raw TCP socket as a length-prefixed binary frame. Any client
// (legitimate or attacker) can send arbitrary bytes on the Kafka port.
// Version byte lets the fuzzer explore all protocol generations.
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <util/generic/buffer.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 2) return 0;
    // First byte selects API version (PRODUCE supports v0..v9)
    NKafka::TKafkaVersion version = data[0] % 10;
    TBuffer buf(reinterpret_cast<const char*>(data + 1), size - 1);
    NKafka::TKafkaReadable readable(buf);
    try {
        NKafka::TProduceRequestData msg;
        msg.Read(readable, version);
        (void)msg.ApiKey();
    } catch (...) {}
    return 0;
}
