#include "parsing.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {

std::string CompressionToString(const arrow::Compression::type compression) {
    return arrow::util::Codec::GetCodecAsString(compression);
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
        case arrow::Compression::GZIP:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecGZIP;
        case arrow::Compression::SNAPPY:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecSNAPPY;
        case arrow::Compression::LZO:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZO;
        case arrow::Compression::BROTLI:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecBROTLI;
        case arrow::Compression::LZ4_FRAME:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4RAW;
        case arrow::Compression::LZ4:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
        case arrow::Compression::LZ4_HADOOP:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4HADOOP;
        case arrow::Compression::ZSTD:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD;
        case arrow::Compression::BZ2:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecBZ2;
        default:
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
    }
}

std::optional<arrow::Compression::type> CompressionFromProto(const NKikimrSchemeOp::EColumnCodec compression) {
    switch (compression) {
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
            return arrow::Compression::UNCOMPRESSED;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecGZIP:
            return arrow::Compression::GZIP;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecSNAPPY:
            return arrow::Compression::SNAPPY;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZO:
            return arrow::Compression::LZO;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecBROTLI:
            return arrow::Compression::BROTLI;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4RAW:
            return arrow::Compression::LZ4;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
            return arrow::Compression::LZ4_FRAME;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4HADOOP:
            return arrow::Compression::LZ4_HADOOP;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD:
            return arrow::Compression::ZSTD;
        case NKikimrSchemeOp::EColumnCodec::ColumnCodecBZ2:
            return arrow::Compression::BZ2;
    }
}

}
