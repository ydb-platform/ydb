#include "parsing.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {

std::string CompressionToString(const arrow::Compression::type compression) {
    return arrow::util::Codec::GetCodecAsString(compression);
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

std::optional<arrow::Compression::type> CompressionFromString(const std::string& compressionStr) {
    auto result = arrow::util::Codec::GetCompressionType(compressionStr);
    if (!result.ok()) {
        return {};
    }
    return *result;
}

NKikimrSchemeOp::EColumnCodec CompressionToProto(const arrow::Compression::type compression) {
    switch (compression) {
        case arrow::Compression::UNCOMPRESSED:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain;
        case arrow::Compression::LZ4_FRAME:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
        case arrow::Compression::ZSTD:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD;
        default:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
    }
}

std::optional<arrow::Compression::type> CompressionFromProto(const NKikimrSchemeOp::EColumnCodec compression) {
    switch (compression) {
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
            return arrow::Compression::UNCOMPRESSED;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD:
            return arrow::Compression::ZSTD;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
            return arrow::Compression::LZ4_FRAME;
    }
}

}
