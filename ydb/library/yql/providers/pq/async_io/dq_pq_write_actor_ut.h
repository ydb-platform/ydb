#pragma once

// This header exposes internal helpers for unit testing only.
// Do NOT include in production code.

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <optional>
#include <utility>

#include <util/generic/strbuf.h>

namespace NYql::NDq::NPrivate {

std::pair<NYdb::NTopic::ECodec, std::optional<int>> ParseCodecString(TStringBuf str);

} // namespace NYql::NDq::NPrivate
