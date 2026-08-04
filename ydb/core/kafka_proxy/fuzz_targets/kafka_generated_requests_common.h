#pragma once

#include <cstring>

#include <ydb/core/kafka_proxy/fuzz_targets/kafka_fuzz_common.h>

namespace NKafka::NFuzz {

template <class T>
void ParseGeneratedRequest(TFuzzedDataProvider& fdp, const T& request, TKafkaVersion version) {
    const TString serialized = SerializeMessage(request, version);
    const TString truncatedMessage = MaybeTruncate(fdp, serialized);

    try {
        ParseMessage<T>(truncatedMessage, version);
    } catch (...) {
    }

    TRequestHeaderData header;
    header.RequestApiKey = request.ApiKey();
    header.RequestApiVersion = version;
    header.CorrelationId = fdp.ConsumeIntegral<TKafkaInt32>();
    header.ClientId = ConsumeOptionalString(fdp);

    const TString frame = SerializeRequestFrame(header, request, version);
    const TString truncatedFrame = MaybeTruncate(fdp, frame);

    try {
        ParseRequestFrame(truncatedFrame, request.ApiKey(), version);
    } catch (...) {
    }
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

template <class T>
void ParseVersionedEnvelope(TKafkaBytes bytes, TKafkaVersion minVersion, TKafkaVersion maxVersion) {
    if (!bytes) {
        return;
    }

    const auto view = bytes.value();
    if (view.size() < sizeof(TKafkaVersion)) {
        return;
    }

    TKafkaVersion version = 0;
    std::memcpy(&version, view.data(), sizeof(version));
    if (version < minVersion || version > maxVersion) {
        return;
    }

    TBuffer buffer(view.data() + sizeof(TKafkaVersion), view.size() - sizeof(TKafkaVersion));
    TKafkaReadable readable(buffer);
    T parsed;
    parsed.Read(readable, version);
}

inline TString ConsumeBytes(TFuzzedDataProvider& fdp, size_t maxLen = 64) {
    return TString(fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen)));
}

} // namespace NKafka::NFuzz
