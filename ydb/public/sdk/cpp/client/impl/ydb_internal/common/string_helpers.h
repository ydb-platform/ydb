#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>


namespace NYdb::inline V2 {

// C++17 support for external users
bool StringStartsWith(const TStringType& line, const TStringType& pattern);
TStringType ToStringType(const std::string& str);

} // namespace NYdb

