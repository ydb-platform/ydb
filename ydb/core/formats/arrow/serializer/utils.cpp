#include "parsing.h"
#include "utils.h"

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {
bool SupportsCompressionLevel(const arrow::Compression::type compression, const std::optional<i32>& compressionLevel) {
    if (!arrow::util::Codec::SupportsCompressionLevel(compression)) {
        return false;
    }
    if (compressionLevel.has_value()) {
        int minLevel = MinimumCompressionLevel(compression).value();
        int maxLevel = MaximumCompressionLevel(compression).value();
        if (compressionLevel < minLevel || compressionLevel > maxLevel) {
            return false;
        }
    }
    return true;
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
}  // namespace NKikimr::NArrow
