#include "cdc_payload_compression.h"

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

#include <algorithm>
#include <limits>

namespace NKikimr::NDataShard {

ECdcPayloadCompression SelectCdcPayloadCompression(
        const NKikimrConfig::TDataShardConfig& config,
        TStringBuf replicationAttributes,
        const NKikimrPQ::TPQTabletConfig& topicConfig)
{
    if (config.GetCdcPayloadCompression() != NKikimrConfig::TDataShardConfig::CDC_PAYLOAD_COMPRESSION_GZIP) {
        return ECdcPayloadCompression::Raw;
    }

    NJson::TJsonValue attributes;
    if (!NJson::ReadJsonTree(replicationAttributes, &attributes) || !attributes.IsMap()
        || !attributes.Has("supported_payload_codecs") || !attributes["supported_payload_codecs"].IsArray())
    {
        return ECdcPayloadCompression::Raw;
    }

    bool supportsGzip = false;
    for (const auto& codec : attributes["supported_payload_codecs"].GetArraySafe()) {
        // Validate the entire declaration, including entries after gzip.
        if (!codec.IsString()) {
            return ECdcPayloadCompression::Raw;
        }
        supportsGzip |= codec.GetString() == "gzip";
    }
    if (!supportsGzip) {
        return ECdcPayloadCompression::Raw;
    }

    // Stored topic codecs are public Topic codec IDs minus one. As in the
    // public writer, an absent/empty topic allowlist is unrestricted.
    constexpr i32 storedGzip = Ydb::Topic::CODEC_GZIP - 1;
    bool topicPermitsGzip = topicConfig.GetCodecs().IdsSize() == 0;
    for (const auto id : topicConfig.GetCodecs().GetIds()) {
        topicPermitsGzip |= id == storedGzip;
    }
    return topicPermitsGzip ? ECdcPayloadCompression::Gzip : ECdcPayloadCompression::Raw;
}

TMaybe<TString> CompressCdcPayload(TStringBuf payload, ECdcPayloadCompression compression) {
    if (compression != ECdcPayloadCompression::Gzip || payload.size() < 1024
        || payload.size() > static_cast<size_t>(std::numeric_limits<i32>::max()))
    {
        return Nothing();
    }

    TStringStream output;
    try {
        TZLibCompress compressor(&output, ZLib::GZip, 1);
        compressor.Write(payload.data(), payload.size());
        compressor.Finish();
    } catch (const TZLibCompressorError&) {
        return Nothing();
    }

    const auto& compressed = output.Str();
    if (compressed.size() >= payload.size()
        || payload.size() - compressed.size() < std::max<size_t>(64, payload.size() / 8))
    {
        return Nothing();
    }
    return compressed;
}

} // namespace NKikimr::NDataShard
