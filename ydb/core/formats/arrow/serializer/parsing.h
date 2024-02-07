#pragma once

#include <util/system/yassert.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/type_fwd.h>
#include <optional>
#include <string>

namespace NKikimr::NArrow {

std::string CompressionToString(const arrow::Compression::type compression);
std::optional<arrow::Compression::type> CompressionFromString(const std::string& compressionStr);

NKikimrSchemeOp::EColumnCodec CompressionToProto(const arrow::Compression::type compression);
std::optional<arrow::Compression::type> CompressionFromProto(const NKikimrSchemeOp::EColumnCodec compression);

}
