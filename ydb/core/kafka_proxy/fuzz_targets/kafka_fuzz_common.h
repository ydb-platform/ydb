#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <ydb/core/kafka_proxy/kafka_messages.h>

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
T ConsumeInRange(TFuzzedDataProvider& fdp, T minValue, T maxValue) {
    return fdp.ConsumeIntegralInRange<T>(minValue, maxValue);
}

class TBytesStorage {
public:
    TKafkaBytes Hold(TString value) {
        Owned_.push_back(std::make_unique<TString>(std::move(value)));
        TString& stored = *Owned_.back();
        return TKafkaRawBytes(stored.data(), stored.size());
    }

private:
    std::vector<std::unique_ptr<TString>> Owned_;
};

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

inline TString SerializeRequestFrame(const TRequestHeaderData& header, const TApiMessage& request, TKafkaVersion requestVersion) {
    const TKafkaVersion headerVersion = RequestHeaderVersion(request.ApiKey(), requestVersion);
    TKafkaWriteBuffer buf(std::max<size_t>(
        1,
        static_cast<size_t>(header.Size(headerVersion)) + static_cast<size_t>(request.Size(requestVersion)) + 16));
    TKafkaWritable writable(buf);
    header.Write(writable, headerVersion);
    request.Write(writable, requestVersion);
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

inline void ParseRequestFrame(const TString& bytes, i16 apiKey, TKafkaVersion requestVersion) {
    TBuffer buffer(bytes.data(), bytes.size());
    TKafkaReadable readable(buffer);
    TRequestHeaderData header;
    header.Read(readable, RequestHeaderVersion(apiKey, requestVersion));
    auto request = CreateRequest(apiKey);
    request->Read(readable, requestVersion);
}

inline TKafkaBytes BytesView(const TString& bytes) {
    return TKafkaRawBytes(bytes.data(), bytes.size());
}

} // namespace NKafka::NFuzz
