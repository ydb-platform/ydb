// Fuzzer for NKafka::TFetchRequestData binary wire parsing.
// The Kafka FETCH request is sent by consumers to pull messages from brokers.
// It carries topic/partition/offset information in a complex nested binary
// format (varies by protocol version). Attacker-controlled clients can send
// arbitrary bytes on the Kafka port.
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <util/generic/buffer.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 2) return 0;
    // FETCH supports versions 0..15
    NKafka::TKafkaVersion version = data[0] % 16;
    TBuffer buf(reinterpret_cast<const char*>(data + 1), size - 1);
    NKafka::TKafkaReadable readable(buf);
    try {
        NKafka::TFetchRequestData msg;
        msg.Read(readable, version);
        (void)msg.ApiKey();
    } catch (...) {}
    return 0;
}
