#pragma once
#include <util/generic/array_ref.h>
#include <string_view>

namespace NYql {
namespace NUdf {

TArrayRef<const std::string_view> GetTimezones();

}
}
