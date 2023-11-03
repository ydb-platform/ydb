#include "string_helpers.h"

#include <algorithm>


namespace NYdb {

bool StringStartsWith(const TStringType& line, const TStringType& pattern) {
    return std::equal(pattern.begin(), pattern.end(), line.begin());
}

TStringType ToStringType(const std::string& str) {
    return TStringType(str.data(), str.size());
}

} // namespace NYdb
