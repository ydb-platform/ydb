#pragma once
#include <string_view>

namespace NYql::NTimeOrderRecover {

using namespace std::string_view_literals;
constexpr auto OUT_OF_ORDER_MARKER = "_yql_OutOfOrder"sv;

}//namespace NYql::NMatchRecognize
