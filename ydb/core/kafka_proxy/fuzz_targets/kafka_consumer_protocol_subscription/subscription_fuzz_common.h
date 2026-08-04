#pragma once

#include <algorithm>
#include <cstring>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <ydb/core/kafka_proxy/kafka_consumer_protocol.h>

#include <util/generic/buffer.h>

namespace NKafka::NFuzz {

using TFuzzedDataProvider = ::FuzzedDataProvider;

inline TString ConsumeString(TFuzzedDataProvider& fdp, size_t maxLen = 32) {
    return TString(fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen)));
}

inline std::optional<TString> ConsumeOptionalString(TFuzzedDataProvider& fdp, size_t maxLen = 32) {
    if (!fdp.ConsumeBool()) {
        return std::nullopt;
    }
    return ConsumeString(fdp, maxLen);
}

inline size_t ConsumeCount(TFuzzedDataProvider& fdp, size_t maxCount = 3) {
    return fdp.ConsumeIntegralInRange<size_t>(0, maxCount);
}

template <class T>
TString SerializeMessage(const T& message, TKafkaVersion version) {
    TKafkaWriteBuffer buf(std::max<size_t>(1, static_cast<size_t>(message.Size(version)) + 16));
    TKafkaWritable writable(buf);
    message.Write(writable, version);
    return buf.AsString();
}

template <class T>
TString SerializeVersionedMessage(const T& message, TKafkaVersion version) {
    TKafkaWriteBuffer buf(sizeof(version) + std::max<size_t>(1, static_cast<size_t>(message.Size(version)) + 16));
    TKafkaWritable writable(buf);
    writable << version;
    message.Write(writable, version);
    return buf.AsString();
}

inline TString MaybeTruncate(TFuzzedDataProvider& fdp, const TString& bytes) {
    return TString(bytes.data(), fdp.ConsumeIntegralInRange<size_t>(0, bytes.size()));
}

template <class T>
void ParseMessage(const TString& bytes, TKafkaVersion version) {
    TBuffer buffer(bytes.data(), bytes.size());
    TKafkaReadable readable(buffer);
    T parsed;
    parsed.Read(readable, version);
}

template <class T>
void ParseVersionedEnvelope(const TString& bytes, TKafkaVersion minVersion, TKafkaVersion maxVersion) {
    if (bytes.size() < sizeof(TKafkaVersion)) {
        return;
    }

    TKafkaVersion version = 0;
    std::memcpy(&version, bytes.data(), sizeof(version));
    if (version < minVersion || version > maxVersion) {
        return;
    }

    TBuffer buffer(bytes.data() + sizeof(TKafkaVersion), bytes.size() - sizeof(TKafkaVersion));
    TKafkaReadable readable(buffer);
    T parsed;
    parsed.Read(readable, version);
}

inline TKafkaBytes BytesView(const TString& bytes) {
    return TKafkaRawBytes(bytes.data(), bytes.size());
}

} // namespace NKafka::NFuzz
