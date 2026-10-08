#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

// The local Kikimr alias and the legacy scan provider already use "ydb".
inline constexpr TStringBuf YdbQueryProviderName = "ydb_query";

} // namespace NYql
