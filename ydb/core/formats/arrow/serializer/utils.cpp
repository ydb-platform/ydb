#include "parsing.h"
#include "utils.h"

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {
bool SupportsCompressionLevel(const arrow::Compression::type compression) {
    return arrow::util::Codec::SupportsCompressionLevel(compression);
}

bool SupportsCompressionLevel(const NKikimrSchemeOp::EColumnCodec compression) {
    return SupportsCompressionLevel(CompressionFromProto(compression).value());
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
