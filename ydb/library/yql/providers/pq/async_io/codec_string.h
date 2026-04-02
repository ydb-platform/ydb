#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <util/generic/strbuf.h>

#include <optional>
#include <utility>

namespace NYql::NDq {

// Parses a codec string of the form "<name>[_<level>]" into (ECodec, optional level).
// The codec name is matched case-insensitively.  The optional suffix "_N" (N > 0) is
// the compression level.  Throws yexception on an unknown codec name.
std::pair<NYdb::NTopic::ECodec, std::optional<int>> ParseCodecString(TStringBuf str);

} // namespace NYql::NDq
