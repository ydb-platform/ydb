#pragma once

#include <string_view>

namespace NYql::NTimeOrderRecover {

inline constexpr std::string_view OUT_OF_ORDER_MARKER = "_yql_OutOfOrder";

} // namespace NYql::NTimeOrderRecover
