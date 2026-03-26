#include "parsing.h"
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {

std::string CompressionToString(const arrow20::Compression::type compression) {
    return arrow20::util::Codec::GetCodecAsString(compression);
}

std::string CompressionToString(const NKikimrSchemeOp::EColumnCodec compression) {
    switch (compression) {
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
            return "off";
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD:
            return "zstd";
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
            return "lz4";
    }
    return "";
}

std::optional<arrow20::Compression::type> CompressionFromString(const std::string& compressionStr) {
    auto result = arrow20::util::Codec::GetCompressionType(compressionStr);
    if (!result.ok()) {
        return {};
    }
    return *result;
}

NKikimrSchemeOp::EColumnCodec CompressionToProto(const arrow20::Compression::type compression) {
    switch (compression) {
        case arrow20::Compression::UNCOMPRESSED:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain;
        case arrow20::Compression::LZ4_FRAME:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
        case arrow20::Compression::ZSTD:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD;
        default:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
    }
}

std::optional<arrow20::Compression::type> CompressionFromProto(const NKikimrSchemeOp::EColumnCodec compression) {
    switch (compression) {
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
            return arrow20::Compression::UNCOMPRESSED;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD:
            return arrow20::Compression::ZSTD;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
            return arrow20::Compression::LZ4_FRAME;
    }
}

}
