#pragma once

#include <base/types.h>
#include <string_view>

namespace DB_CHDB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexp(std::string_view pattern);

}
