#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>


namespace NYdb {

// C++17 support for external users
bool StringStartsWith(const TStringType& line, const TStringType& pattern);
TStringType ToStringType(const std::string& str);

} // namespace NYdb

