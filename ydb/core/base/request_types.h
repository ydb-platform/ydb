#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NRequestTypes {

inline constexpr TStringBuf Analyze = "_analyze_request";
inline constexpr TStringBuf ChecksumTable = "_checksum_table_request";

} // namespace NKikimr::NRequestTypes
