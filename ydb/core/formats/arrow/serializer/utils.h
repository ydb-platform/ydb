#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/type_fwd.h>
#include <util/system/yassert.h>

#include <optional>

namespace NKikimr::NArrow {
bool SupportsCompressionLevel(const arrow::Compression::type compression);
bool SupportsCompressionLevel(const NKikimrSchemeOp::EColumnCodec compression);

std::optional<int> MinimumCompressionLevel(const arrow::Compression::type compression);
std::optional<int> MaximumCompressionLevel(const arrow::Compression::type compression);
}
