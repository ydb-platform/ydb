#pragma once

#include <optional>
#include <string_view>
#include <functional>

namespace NYql {

// simple type is a type which is not parameterized by other types (or parameters)
// Void, Unit, Generic, EmptyList, EmptyDict and all Data types (except for Decimal) are simple types
std::optional<std::string_view> LookupSimpleTypeBySqlAlias(const std::string_view& alias, bool flexibleTypesEnabled);

void EnumerateSimpleTypes(const std::function<void(std::string_view name, std::string_view kind)>& callback, bool flexibleTypesEnabled);

} // namespace NYql
