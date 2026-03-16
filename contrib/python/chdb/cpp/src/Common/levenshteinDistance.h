#pragma once

#include <base/types.h>

namespace DB_CHDB
{

/// How many steps if we want to change lhs to rhs.
/// Details in https://en.wikipedia.org/wiki/Levenshtein_distance
size_t levenshteinDistance(const String & lhs, const String & rhs);

}
