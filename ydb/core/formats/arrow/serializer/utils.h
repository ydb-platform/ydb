#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/type_fwd.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NArrow {
bool SupportsCompressionLevel(const arrow::Compression::type compression, const std::optional<i32>& compressionLevel = {});

std::optional<int> MinimumCompressionLevel(const arrow::Compression::type compression);
std::optional<int> MaximumCompressionLevel(const arrow::Compression::type compression);
}
