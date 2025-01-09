#include "parsing.h"
#include "utils.h"

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {
bool SupportsCompressionLevel(const arrow::Compression::type compression) {
    return arrow::util::Codec::SupportsCompressionLevel(compression);
}

bool SupportsCompressionLevel(const arrow::Compression::type compression, const i32 compressionLevel) {
    if (!SupportsCompressionLevel(compression)) {
        return false;
    }
    int minLevel = MinimumCompressionLevel(compression).value();
    int maxLevel = MaximumCompressionLevel(compression).value();
    if (compressionLevel < minLevel || compressionLevel > maxLevel) {
        return false;
    }
    return true;
}

bool SupportsCompressionLevel(const NKikimrSchemeOp::EColumnCodec compression) {
    return SupportsCompressionLevel(CompressionFromProto(compression).value());
}

bool SupportsCompressionLevel(const NKikimrSchemeOp::EColumnCodec compression, const i32 compressionLevel) {
    return SupportsCompressionLevel(CompressionFromProto(compression).value(), compressionLevel);
}

std::optional<int> MinimumCompressionLevel(const arrow::Compression::type compression) {
    if (!SupportsCompressionLevel(compression)) {
        return {};
    }
    return NArrow::TStatusValidator::GetValid(arrow::util::Codec::MinimumCompressionLevel(compression));
}
std::optional<int> MaximumCompressionLevel(const arrow::Compression::type compression) {
    if (!SupportsCompressionLevel(compression)) {
        return {};
    }
    return NArrow::TStatusValidator::GetValid(arrow::util::Codec::MaximumCompressionLevel(compression));
}
}
