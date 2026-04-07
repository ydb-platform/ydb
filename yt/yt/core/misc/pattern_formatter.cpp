#include "pattern_formatter.h"

#include <yt/yt/core/misc/error.h>

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr char Dollar = '$';
constexpr char LeftParen = '(';
constexpr char RightParen = ')';

////////////////////////////////////////////////////////////////////////////////

TPatternFormatter& TPatternFormatter::SetProperty(std::string name, std::string value)
{
    PropertyMap_[std::move(name)] = std::move(value);
    return *this;
}

std::string TPatternFormatter::Format(TStringBuf pattern)
{
    std::string result;

    for (size_t pos = 0; pos < pattern.size(); ++pos) {
        if (pattern[pos] == Dollar && (pos + 1 < pattern.size() && pattern[pos + 1] == LeftParen)) {
            auto left = pos + 2;
            auto right = left;
            while (right < pattern.size() && pattern[right] != RightParen) {
                right += 1;
            }

            if (right < pattern.size()) {
                auto propertyName = std::string(pattern.substr(left, right - left));
                auto it = PropertyMap_.find(propertyName);
                if (it != PropertyMap_.end()) {
                    result.append(it->second);
                    pos = right;
                    continue;
                }
            }
        }

        result.push_back(pattern[pos]);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
