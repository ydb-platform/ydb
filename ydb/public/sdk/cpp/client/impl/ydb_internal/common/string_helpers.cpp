#include "string_helpers.h" 
 
#include <algorithm> 
 
 
namespace NYdb { 
 
bool StringStartsWith(const TStringType& line, const TStringType& pattern) { 
    return std::equal(pattern.begin(), pattern.end(), line.begin()); 
} 
 
} // namespace NYdb 