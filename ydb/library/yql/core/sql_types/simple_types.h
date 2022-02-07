#pragma once

#include <optional>
#include <string_view>

namespace NYql {

// simple type is a type which is not parameterized by other types (or parameters)
// Void, Unit, Generic, EmptyList, EmptyDict and all Data types (except for Decimal) are simple types
std::optional<std::string_view> LookupSimpleTypeBySqlAlias(const std::string_view& alias, bool flexibleTypesEnabled);

}
