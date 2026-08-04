// Fuzzer for NKafka::TMetadataRequestData binary wire parsing.
// METADATA requests ask the broker which partitions/leaders exist.
// Topic names and UUIDs are parsed as variable-length fields and the
// request exists in both legacy (v0–8) and flexible (v9+) forms with
// different compact/nullable encoding — rich ground for parser bugs.
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <util/generic/buffer.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 2) return 0;
    // METADATA supports versions 0..12
    NKafka::TKafkaVersion version = data[0] % 13;
    TBuffer buf(reinterpret_cast<const char*>(data + 1), size - 1);
    NKafka::TKafkaReadable readable(buf);
    try {
        NKafka::TMetadataRequestData msg;
        msg.Read(readable, version);
        (void)msg.ApiKey();
    } catch (...) {}
    return 0;
}
